> 可移植诊断副本；源码链接改为仓库相对路径并标注原审计行号，内容结论未改。本文件与[原字节 gzip](../evidence/parallel-progress/wireguard-stopold-recovery-review.md.gz)不同；原文 raw/gzip SHA 见证据清单。本文仅只读方案，不代表已实现或通过运行验证。

# WireGuard StopOld 后同状态恢复：只读审计

2026-10-20；读取 `/workspace/core` HEAD `0ee7c39d971b6656c837c68222cf2213ac210a50`。与 `/root/stream_io` 已直接协调：其 no-standby listener WIP 先完成全部 affected StopOld barrier，之后才 first candidate bind；失败恢复取真实 last-good runtime config。本文不改 root/worker Rust、原断言、协议分支、超时或生产启用；不运行 Cargo/产品/诊断负载。

## 结论

**协议实现允许保留同一个 actual WG crypto/history 对象，关闭旧 FD 后用新 FD重新驱动；当前 listener/recovery 接线尚不保证做到。** `wireguard::runtime::InboundDevice / PeerTunnel / gotatun::Tunn` 本身不持 UDP socket 或 Tokio task，因而无需复制密钥、序列化 session 或构造新的 Tunn 来换 listener FD。

最小安全方案是：在 actual旧 operation仍存活时，向同 host affected-set 持久登记一个 **opaque recovery capsule**，强持原 `Arc<LiveInboundDevice>` 及必要的实际 endpoint 关联；保留仅内存的同一份安全历史，不保备用socket/活跃旧listener。先停原I/O执行、取实际TaskExit并显式关闭原FD，确认后才能给候选/恢复实例新grant；candidate失败还要先完成其所有真实nativecleanup，再用capsule接回旧已提交身份。standalone不能只指望现有Weak cache；linked不能只保keys而换掉 actual stack cells 或错误使用candidate pending map。

**这不是现已通过的资格。** 原WG UDP目前裸Arc native I/O未被真实listener close controller覆盖；只做通用StopOld顺序或等待旧Scope结束，不能据此宣布原FD关闭。

## 1. 原测试到底要求什么

- [linked测试入口](../../../../crates/proxy/tests/wireguard_inbound.rs#L41)（审计时第 41 行）：`linked_wireguard_endpoints_share_socket_and_peer_state_in_both_directions`。
- [不同UDP端口+后续TCP端口冲突](../../../../crates/proxy/tests/wireguard_inbound.rs#L185)（审计时第 185 行）：candidate的WG新UDP端口可用，另一TCP listener被真实occupied socket占用；apply须Err，candidate UDP端口在ACK前可用作外部行为检查，再由仍持原WG会话的对端发 `after-failed-bind` 并得到真实echo。**测试没有证明port可重绑就是nativecloseACK**。
- 该case先把原inbound/outbound keepalive改成2并成功apply；[实际last-good赋值](../../../../crates/proxy/tests/wireguard_inbound.rs#L159)（审计时第 159 行） 是 changed_endpoint。随后failed_bind却从初始a_config clone。恢复必须用真实last-good（keepalive2），不能仅从candidate/部分active map猜旧配置。
- [standalone server入口](../../../../crates/proxy/tests/wireguard_inbound.rs#L298)（审计时第 298 行）：server只有WG inbound，无linked outbound，因此其device最容易在旧operation退出后失去强Arc。
- [same-port原TCP continuity](../../../../crates/proxy/tests/wireguard_inbound.rs#L405)（审计时第 405 行） 是hot update语义；该路径应仍复用旧I/O与原运行状态。不能为了新StopOld方案把原hot update也变成重启。
- [不同UDP端口真实占用失败](../../../../crates/proxy/tests/wireguard_inbound.rs#L434)（审计时第 434 行）：apply须Err，旧WG配置的真实UDP echo在15秒原deadline内恢复。对端保持其已建立的WG安全会话；fresh responder Tunn未必能解当前transport data，不能等待更长重握手来掩盖。

最新规则明确不允许新端口预持备用listener：[lifecycle.md:61](../../../../design/v0.0.3/lifecycle.md#L61)（审计时第 61 行）。[StopOld/BuildNew/Compensate](../../../../design/v0.0.3/lifecycle.md#L125)（审计时第 125 行） 要先确认旧task/I/O释放，失败先清候选再恢复。旧测试的外部恢复结果仍可保留，但不应再靠“旧FD从未停止”满足；其中已断的业务连接不承诺可复活，不能将这点扩成允许丢弃WG replay/history。

## 2. actual security state 与持有点

| 对象 | 真实内容 / 当前强持有者 | StopOld风险与可复用边界 |
| --- | --- | --- |
| `InboundDevice` | [protocol runtime](../../../../protocols/wireguard/src/runtime/inbound.rs#L77)（审计时第 77 行）：PreparedInbound、Vec<PeerTunnel>、peer_sources、static key objects、RateLimiter、local_indices/index_order、unknown-source handshake limiter。 | 与socket/task分离。**同一对象**在内存保留即可保全当前索引、cookie/限流、安全历史和learned authenticated source；不应按相同key重构来假装相同state。 |
| `PeerTunnel` | [fields](../../../../protocols/wireguard/src/runtime.rs#L108)（审计时第 108 行）：actual `Tunn`、其RateLimiter Arc、MTU、emitted_data_messages、unknown-source limiter。 | `emitted_data_messages`的拒绝上限也是历史，不只AEAD counter。不得clone两份并行驱动；从单个owner移交同对象。 |
| `Tunn` / Session | [gotatun Tunn](../../../../vendor/gotatun/src/noise/mod.rs#L69)（审计时第 69 行）保存handshake、最近sessions、current/session_counter、blocked packet_queue、timers、tx/rx counters、RNG；[Session](../../../../vendor/gotatun/src/noise/session.rs#L32)（审计时第 32 行）保存key/receiver index、sending_key_counter、receiving replay validator。 | 新FD不是新cryptoepoch。不能copy Tunn造成同key双counter，也不能reset接收bitmap/nonce counters/重放时间戳。 |
| replay / handshake | [8192bit receive window](../../../../vendor/gotatun/src/noise/session.rs#L51)（审计时第 51 行）；[handshake last_timestamp](../../../../vendor/gotatun/src/noise/handshake.rs#L343)（审计时第 343 行），[reject旧timestamp](../../../../vendor/gotatun/src/noise/handshake.rs#L613)（审计时第 613 行）。 | 保原actualpeer而非仅config keys。timestamp/replay reject必须跨停止/恢复继续生效。 |
| `LiveInboundDevice` | [adapter holder](../../../../crates/proxy/src/adapters/wireguard/inbound/device.rs#L7)（审计时第 7 行）：Mutex<InboundDevice>、generation、incarnation、identity。`WireguardInboundDevice(Arc<LiveInboundDevice>)`包入actual raw-IP operation。 | 它是最小same-crypto capsule的实际对象。同protocol identity的port-only恢复无需调用prepare_device或replace，只迁移这一个Arc的执行所有权。 |
| standalone cache | [WireguardAdapter.inbound_devices](../../../../crates/proxy/src/adapters/wireguard.rs#L39)（审计时第 39 行）仅Weak；[prepare路径](../../../../crates/proxy/src/adapters/wireguard.rs#L102)（审计时第 102 行）尝试upgrade/matches_identity，否则fresh `inbound::prepare`。 | oldoperation通常是standalone唯一强Arc；StopOld退出后Weak可能已空，rollback又构造fresh InboundDevice。必须在停前持强capsule，不能用strong_count当运行/关闭证明，也不宜将Weak简单换成永久不清理的global strong map。 |
| linked endpoint | [LinkedEndpoint](../../../../crates/proxy/src/adapters/wireguard/inbound.rs#L30)（审计时第 30 行）强持live、state.devices、sender watch、peers watch、Option packet receiver和active lease。adapter的[linked_endpoints map](../../../../crates/proxy/src/adapters/wireguard.rs#L40)（审计时第 40 行）强持link。 | 已published link会在oldoperation退出后留live；但不是每个different-listen candidate都用原link。按tag+listen筛选，必须用**published旧link**恢复，不能拿candidate pending map作lastgood。 |
| linked client stacks | [LinkedEndpointState.devices](../../../../crates/proxy/src/adapters/wireguard/inbound.rs#L42)（审计时第 42 行）实际Arc<SharedRawIpDevice>；[pool cache](../../../../crates/proxy/src/runtime/raw_ip/pool.rs#L29)（审计时第 29 行）是Arc<OnceCell<Arc<...>>>；业务流/packet return也持引用。 | 这些是内层TCP/UDP栈、packet returns和实际endpoint driver，不是WG crypto本身。未变的linked outbound stack应保原cell/Arc，不能用freshstack冒原continuity。若依赖确实被StopOld关闭则已断flow不能假恢复，需记录影响。 |

### replace_preserving 并不等于完整same-state

[InboundDevice::replace_preserving_peers](../../../../protocols/wireguard/src/runtime/inbound.rs#L136)（审计时第 136 行）只在相同private key、原peer位置且public key/PSK/keepalive/reserved一致时，把**原PeerTunnel和peer_source move/swap**入next，并搬receiverindices；允许更新MTU/routes。peer重排/identity变化不能重用错误索引。它不是克隆sessions。

但是next是通过[prepare_device](../../../../crates/proxy/src/adapters/wireguard/inbound.rs#L188)（审计时第 188 行） → [into_device](../../../../protocols/wireguard/src/runtime/inbound.rs#L48)（审计时第 48 行）新造，device层RateLimiter、cookie相关state及unknown-source limiter会是新对象；完全相同配置换FD不应无谓走这条replacement路径。sameidentity port-only更小且更严格的方案是直接保留原Live/InboundDevice，不重构再swap。

[standalone update_inbound_listener](../../../../crates/proxy/src/adapters/wireguard.rs#L220)（审计时第 220 行）目前会立即prepare_device并 `live.replace`；linked则返回true并让[LinkedEndpointUpdate::publish](../../../../crates/proxy/src/adapters/wireguard/inbound.rs#L57)（审计时第 57 行）在commit替换。若通用cold-plan阶段仍调用有副作用的standalone update，后面别的candidate失败可能已改变旧crypto。这是独立的staged-update硬依赖：不能仅强持Arc就声称保住了尚未被candidate改写的历史。cold阶段应只决定reuse/hot/rebuild；真正修改必须保对应commit/compensate义务，不能在runtime加WGmatch来绕。

## 3. linked与standalone在停后实际丢什么

### linked

- [prepare_linked_endpoint](../../../../crates/proxy/src/adapters/wireguard/lifecycle/linked.rs#L55)（审计时第 55 行）查existing时要求listen完全相等。differentport导致existing=None，即使同tag、samecrypto、oldlink尚在publishedmap。
- existing+deviceidentity/generation匹配时，还核[pool cell actualArc ptr](../../../../crates/proxy/src/adapters/wireguard/lifecycle/linked.rs#L72)（审计时第 72 行），能重用原stackcells；否则[直接 start_on_endpoint_with_options](../../../../crates/proxy/src/adapters/wireguard/lifecycle/linked.rs#L141)（审计时第 141 行）启动新clientstack任务，并放staged.new_devices。这是现有设备prepare会启动任务的事实，不能因为listener禁standby就声称整个WG资源图无新旧实例重叠。
- newlink路径也会尝试[inbound weakcache upgrade](../../../../crates/proxy/src/adapters/wireguard/lifecycle/linked.rs#L168)（审计时第 168 行）保旧live，只有旧Arc仍在且identity匹配才成功。候选pipeline时序改变之后不能依赖偶然upgrade成功。
- [LinkedEndpoint::take_operation](../../../../crates/proxy/src/adapters/wireguard/inbound.rs#L118)（审计时第 118 行）的active swap只防两个operation并行领取；old RawIpInboundEndpoint Drop把lease设false，**不是TaskExit/FDcloseACK**。receiver已消费后，下一次创建queue256并sender.send_replace；原stack的watchreceiver可接新sender。
- [send_endpoint_packet](../../../../crates/proxy/src/runtime/raw_ip/device/endpoint.rs#L219)（审计时第 219 行）在旧sender关闭后保原packet并等待watch.changed再重发。此机制可帮助短暂underlay重建，但须证明只投递一次、旧return_channel/closed fence仍有效；它不证明原native已关。

### standalone及raw-IP loop局部state

- [raw_ip/run](../../../../crates/proxy/src/runtime/inbound_operation/raw_ip/run.rs#L12)（审计时第 12 行）每次创建UserNetworkStack、TCP accept/UDP tasks、response queue256、PacketSocketSender、endpoints Vec、fragment reassemblers、PeerTimers、packet pins、ingressid。loop退出后这些局部状态会销毁；sameLive保crypto不等于自动保原内层flow。
- `endpoints`初值只取[linked.initial_endpoints](../../../../crates/proxy/src/runtime/inbound_operation/raw_ip/run.rs#L38)（审计时第 38 行），standalone为空；实际authenticated source会在recv时更新looplocal endpoint。保留的protocol [peer_sources](../../../../protocols/wireguard/src/runtime/inbound.rs#L182)（审计时第 182 行）已有真实authenticated_endpoint，恢复时可以由中立RawIpInboundDevice窄方法投影，不需要runtime识别WG或从未经认证的新来源猜端点。opaque carrier的source_unknown必须仍是None。
- linked [refresh_endpoint_peers](../../../../crates/proxy/src/runtime/inbound_operation/raw_ip/outer.rs#L70)（审计时第 70 行）只在同一个存活loop中对比旧initial并保learnedendpoint；不能恢复已被Drop的旧looplocal Vec。generation/revision改变还会重置fragments，须明确不能宣称保所有assembly/oldbusiness状态。
- 必须区别原same-port hot-update TCPcontinuity与different-port StopOld补偿。前者不应触发此重建；后者按最新lifecycle允许受影响已断连接不能复活，但新业务必须可用原保留WG session继续通信，不能把freshhandshake当完全等价。

## 4. native控制与真实join证据现况

| 边界 | 现代码实际做了什么 | 尚无的证明 |
| --- | --- | --- |
| 原WG UDP listener | [bind_inbound](../../../../crates/proxy/src/adapters/wireguard.rs#L258)（审计时第 258 行）裸Tokio bind→Arc→PacketSocket。loop内PacketSocketSender还clone同Arc。 | 没有在任何bind之前登记的ControlledDatagramOwner/Control；Drop/原端口可再bind均不是显式close receipt。 |
| raw listener子任务 | [run末](../../../../crates/proxy/src/runtime/inbound_operation/raw_ip/run.rs#L303)（审计时第 303 行） `tasks.shutdown().await; proxied_tasks.shutdown().await`。 | JoinSet确实abort并等其成员销毁，但没有把所有原Err/class/id收进host TaskScope。它不包含native显式close，也不能当另一个原driver的TaskExit。 |
| linked raw-IP client stack | [start_on_endpoint](../../../../crates/proxy/src/runtime/raw_ip/device/endpoint.rs#L104)（审计时第 104 行）tokio::spawn后只存AbortHandle；[Completion Drop watch](../../../../crates/proxy/src/runtime/raw_ip/device/completion.rs#L3)（审计时第 3 行）标true，wait_stopped等该flag。 | completion说明future/guard已销毁，包括abort-beforepoll，**不是实际JoinHandle await/TaskScope TaskExit、不是nativeFD ACK**，原panic/source没有等价保存。 |
| standalone outbound raw device / retired pool | [device start](../../../../crates/proxy/src/runtime/raw_ip/device.rs#L145)（审计时第 145 行）同样只存AbortHandle；[retire_after](../../../../crates/proxy/src/runtime/raw_ip/device.rs#L190)（审计时第 190 行）另spawn grace timer，close_now abort；[pool.publish](../../../../crates/proxy/src/runtime/raw_ip/pool.rs#L187)（审计时第 187 行）允许30秒retiregrace。 | 不符合把“新旧设备不并行、旧资源先释放”一概宣称完成；该pool不是本次listener顺序patch已迁移的canonicalgraph。 |
| InboundOwner | [confirm_physical_close](../../../../crates/proxy/src/runtime/inbound_owners/entry/terminal.rs#L140)（审计时第 140 行）核真实scope_closed/resource.stopped等。 | 若WG operation没有登记native resource，空resource集合不能凭空证明其裸FD关闭。不能将Scope结束当socket实际ACK。failed/pending裸UDP也[明确return false](../../../../crates/proxy/src/runtime/inbound_owners/entry/pending_bound/tree.rs#L144)（审计时第 144 行）。 |

### 正常 Legacy WG 的实际误确认路径（不是现有 Unknown gate）

这项必须和failed/pre-execution pending-bound分开：

1. [spawn正常路径](../../../../crates/proxy/src/runtime/listeners/inbound/spawn.rs#L108)（审计时第 108 行）把原Bound直接move给operation.execute；`retain_unexecuted_bound`只在此前register/install错误分支调用。正常WG **pending_native_bound=None**。
2. `RawIpInboundListenerOperation`没有override install_resources，继承[默认空注册Ok](../../../../crates/proxy/src/runtime/inbound_operation/contract.rs#L14)（审计时第 14 行），故原裸nativeUDP没有Resource owner/Control，peers也不是Combined PeerGroup资源。
3. 原Scope实际退出后，只要installation_complete/cancel/scope_closed满足，[terminal确认](../../../../crates/proxy/src/runtime/inbound_owners/entry/terminal.rs#L161)（审计时第 161 行）对空resources/peers的any检查均为false，走到187行把 **physically_closed置true**。
4. [terminal生成](../../../../crates/proxy/src/runtime/inbound_owners/entry/terminal.rs#L190)（审计时第 190 行）可因此产生 `closed_io_receipts=0` 的terminal；[shutdown返回](../../../../crates/proxy/src/runtime/inbound_owners/entry/shutdown.rs#L104)（审计时第 104 行）优先self.result，若无taskfault可返回Ok。实际只是旧loop/future销毁、裸socket随Drop释放，没有原FD显式close调用结果。

因此，**现代码不能保证正常WG StopOld会以Unknown阻止firstcandidatebind**；它可能把logical legacy task completion误标成physicalcomplete。上表pending_bound::Kind::Raw返回false只覆盖另一个前执行失败持有路径，不能拿来为正常路径背书。本报告步骤里的“Unknown必须挡新bind”是待实施要求，不是当前行为。

最小纠正不能全局硬写“closed_io_receipts必须>0”（确有无native的合法operation）：应在cold actualbound recipe/decl或进入accepted执行前，把本实例**实际expected native leaf obligations**登记到persistent owner；每个actualUDP/TCP/QUICleaf必须对应真实Control/owner/physicalconfirmation。只有leaf义务全部确认才置physicalclosed。未迁移的已知nativeleaf不能靠空resource迭代推导成功；否则通用no-standby顺序虽正确，仍在错误的barrier上放行。最终方案仍须把WGactual裸Arc consumer/ancillary接成真实controlledgrant，不能只新增一个永远不可能确认的bool占位后宣称迁移完成。

因此现在只能说明“理论上可保同协议state跨FD”，不能授权WG生产StopOld/restore为完整nativeowner完成。此前controlledUDP library已有真实sameFD close测试，不代表这里的bareArc consumer已经接入；还需PacketSocket ancillary同原FD发送（WG保IP_PKTINFO/IPV6_PKTINFO/Linux SO_MARK与Darwintemporaryscope），不能退化成plain send_to。

## 5. 最小可实施的generic接口切口（未编码）

1. **StopOld前捕获原state**：actual旧listener operation/owner提供默认可空的中立opaque `ListenerRecoveryState` capability；WG具体实现捕获原Live/原published link关联，Runtime只存trait object，不做WireGuard enum/tag match。capsule加入actual事务affected-set/owner的已有有限map，绑定原owner identity+旧配置摘要+lastgoodgeneration；不是新global缓存或无限history。
2. **capsule只保被动协议state**：不保旧nativeSocket/Control、旧acceptor运行权限或完整Engine/Config闭包；FDControl仍唯一归原host nativeowner。samecrypto只持原Arc不Clone Tunn/复制session。oldold state只能一个executor active，跨取消/错误capsule仍由持久host保留到commit或确认compensation。
3. **真实停止原I/O**：关闭受影响admission；原实际driver退出、所有实际子taskjoin/错误source归owner；同IOgate撤销所有旧view，并取原FD显式closeACK。Unknown保source/permit，**禁止firstcandidatebind**。与内层sessionstate分开证明：被动安全历史的留存不是仍运行旧device/备用listener。
4. **新构建仍需完整candidate cleanup**：新port bind失败是BuildNew失败，不提前reserve FD。已创建的其它candidate必须先join+nativeclose；之后才能用lastgood recipe恢复。不能用candidatependingmap、最初a_config或部分active map替真正previouslastgood。
5. **恢复原state**：恢复接口接**已确认shutdown的原owner票据 + 新nativegrant + 同capsule**。sameoldprotocol identity时直接构造原WireguardInboundDevice(Arc)；linked恢复指向原publishedlink，原unchangedstackcells与watch sender关系保持。需要新packetreceiver时只做一次checked领取/切换，不允许两个activelease。newFD/newI/Ogeneration由新hostowner分配，旧IOguard不得因保旧crypto而复活权限。
6. **恢复learnedsource与timer**：以actual保留PeerSource投影authenticated endpoint；opaque无源保持None，timer基于原Tunn clock/剩余期限重建poll计划，不resetkeyage。重放、receiverindices、sendingcounter、cookie/unknownsource limiter完全留在原InboundDevice，不通过freshprepare/replace再伪称同state。
7. **cold与hot副作用边界**：standalone immediate `live.replace` 应改为actualprepared staged update，commit时才执行；失败候选不能先改旧Device。linked prepared device/stack也需cold recipe与start拆开，避免替listener修顺序却预启动第二组actualdevice。这个动作不属于stream_io只改genericlisteners就能声称完成的范围。
8. **完成/故障收尾**：nativeknownclosed可归还对应permit，但stickyTaskFailure独立交付；cleanup Err不能提前跳掉其它nativeowner。capsule的释放不是FDACK；可在所有commit/rollback义务结束后释放不再使用的旧内存state，失败重试保同actualsource/history。回收不依赖strong_count/Drop。

如果先做最小standalone原断言修复，也必须包含步骤1/3/4/5及nativeconsumer实际接线；仅把Weak改强或在rollback重新prepare_device可能使一次echo偶然成功，但不证明安全历史/关闭顺序正确。若暂未接controlledUDP ancillary，不应借此开启production，应该把该路径列未迁移硬依赖。

## 6. 最少真实回归（保留原断言/原deadline）

- 原linked和standalone两项differentport失败测试完整保留，不提前放开端口/不延长5秒apply与15/20秒echo期限；same-portcontinuity原样保留。
- 新独立case先真实wire建立session，保存实际已接收transport datagram；在**原recv实际Pending点**触发StopOld，证明真实原TaskExit+原FDclose先于新bind。失败候选清完后恢复原旧端口，原remote不强制重新handshake，下一合法data counter继续成功；重放已接收旧datagram仍拒，响应sendingcounter不回退。不打印key/plainsecret。
- standalone停后把所有旧operation引用释放，证明capsule是实际强holder且rollback接回同实际Live/crypto；既不靠Weak偶然upgrade也不靠未停止旧task。
- linked证明actualoldLink/live/stackcell按identity复用，原watch队列切换一次；pendingpacket不复制/双投递，closedreturnchannel仍丢弃。热更新新身份与协议合法rekey不可错误保旧peer。
- 真实close Unknown、taskpanic、取消重入：无新bind；capsule/source/permit仍存，重试不生成freshpeer绕过失败，candidate已取得部分FD也继续cleanup。单端口可重绑仅作外部行为回归，不替原closeACK。
- 可复用[protocol replay tests](../../../../protocols/wireguard/tests/runtime.rs#L185)（审计时第 185 行）与[authenticated roaming replay test](../../../../protocols/wireguard/tests/runtime.rs#L206)（审计时第 206 行）做窄协议断言，但协议单元通过不能替wholelistener native/task归属。

## 本轮交付与未完成项

本轮仅此分析与消息协作，未修改任何原断言、计时、WG/runtime业务代码或启用项。stream_io已告知其patch不触WG、不同port路径仍legacy；本报告识别的opaque recovery/nativegrant/actualtaskjoin/ancillary/stageddevice图接线需后续明确实施与root统一验证。原smaps诊断继续不运行，root37仍在Cargo/真实gRPC挂起调查中。

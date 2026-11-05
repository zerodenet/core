# 第二轮执行与候选验收记录

这是实际执行索引，不是 Z003 或 G0–G10 完成证书。整轮仍有明确的未完成实现；
保留原任务卡和验收标准，不以单条组合或检查通过代替整域完成。
所有实现 PR base 是 `codex/v003-cloud-r2-integration`，仅 M 普通合入该分支。
没有合入 develop/main，没有发布、部署或修改消费者及用户系统网络。

## 固定起点与调度

- remote fetch/push 均核对为 `https://github.com/zerodenet/core.git`。
- 基线包 head：`c940b727193f69752c66a9f49a7d64693ad74c1c`，包含
  checkpoint `e4c3bc0e20628ce4e50bde4d829b80fba17266c5`；未改写基线。
- 同一云端环境三个独立实现 worktree：A DNS、B outbound、C device。
  首波后复用 A 的独立 control 分支与 B 的独立 budgets 分支，未增加第四名实现 agent。
- Rust `1.99.0 (b940084d7 2026-09-28)`，匹配 rustfmt/Clippy，protoc `35.0`。
  同一共享 target，由 M 串行发放构建/测试槽；没有同时运行两个 Cargo。
  构建使用 `CARGO_BUILD_JOBS=4`、dev/test debug=0、incremental=0；这是云端验证配置，
  不代表 1 CPU/1 GiB 压力资格。
- 仓库忽略 Cargo.lock。原环境锁与包 head 不匹配，首次 locked fetch 失败；
  M 对当前 manifests 离线 materialize 并下载依赖，再向 worktree 分发相同锁。
  后续 grpc 和 raw-ip-runtime 的 zero-tun 可选依赖变化同样只由 M 离线重建锁；
  包版本与 checksum 未升级，没有升级既定协议上游。
  组合锁 SHA-256：`05843fd0d9b831fd66bd3c69e67dd257cbe27429607040d0b1f2f858d2ecf599`。
- 固定参考工具：Xray 26.3.27 / d2758a0、Shadowsocks-rust 1.21.2 / a03006a、
  Mieru v3.33.0 / 48ddb69d5d343d76c9004c5054ee36609579ba13、
  Hysteria2 v2.12.2 / 619a6f856b69fb7ee6a7a379e810e68b84004605、sing-box 1.13.14。
  Mieru 准备初次失败因为环境的 `/usr/bin/go` 不是 Go 编译器；使用校验后的官方
  Go 1.25.0 成功准备。不把工具准备成功记为互通通过。

## PR、依赖及文件责任

| 域 | 分支 / PR | 实际交付与依赖 |
| --- | --- | --- |
| A DNS | `codex/v003-cloud-r2-dns` / [#130](https://github.com/zerodenet/core/pull/130) | 消费 B 的实际 StreamLoan / MUX DatagramLoan、M 的原件转交与 current-plan 校验；最后提交依赖 B observed pending 路径 |
| B outbound | `codex/v003-cloud-r2-outbound` / [#129](https://github.com/zerodenet/core/pull/129) | HY2 原 logical stream、Mieru pool 与 shared carriers 前置先合入，后续预算分支继续交 pending 原件与真实调用者 fixture |
| C device | `codex/v003-cloud-r2-device` / [#128](https://github.com/zerodenet/core/pull/128) | Direct 原件、TUN completion/Windows SDK、linked cold graph；M 统一接实际 host transaction 和 pending publication |
| control | `codex/v003-cloud-r2-control` / [#131](https://github.com/zerodenet/core/pull/131) | 复用 A；依赖 M Unix/accepted 原件、ControlBudget、同 Source quota-storage、Engine shutdown |
| budgets | `codex/v003-cloud-r2-budgets` / [#132](https://github.com/zerodenet/core/pull/132) | 复用 B；唯一 config 写入者，A 消费 DNS/control，C 消费 Direct，M 接公共 FIFO / 预算 / ordinary projection |

实现分支精确头（共同集成后才赋予组合资格）：

| 分支 | 完整 SHA |
| --- | --- |
| DNS | `37cb4a8514568f4f0d6908792f1b205248a46cd2` |
| outbound | `40661615f06b4c6734d9abfe568cbb9da4dc2ebc` |
| device | `3c0a9ffc1435409291342ce107d1f59ae223b895` |
| control | `5dd94af9ea9805f4416e97b97ab8fb3db4593b0e` |
| budgets | `51ad5723bb5c782c24152af2822b0bb62b3ef8aa` |

预算 `51ad5723` 的 Node N → B Node Stream/Service `daa1b889` / original cancellation
`d8d3f724` / actual executor admission `168507d3` → A Node purpose 校验 `81c652ba`；
这些都消费 M 的同 slot custody、同 cold union 与 actual Node acquisition 共享前置。
B 的共享原件先合入、A 再消费；C 独立原件先行，尚未实现的 Packet borrowing 不冒列已满足依赖。

M 独占公共 platform/transport/runtime、register、inventory/service 与协议总投影；
A/B/C 对共享文件给仓库外建议补丁，M 审阅提交。额外明确转交：A 控制服务装配、
HTTP/IPC/gRPC、Engine quota；B config 非 manifest；C TUN inbound 生命周期与
vendor/wintun 原 Adapter/Session 关闭接口。没有升级 vendored Wintun 上游或改许可证。

关键共享前置：`5be118e2` 标准 connect_carrier_stream；`8f5aa3b3` Unix listener 原件；
`187c6efa` ControlBudget；`1a67f501` 原物理 MUX FIFO；`623ef1a0` 实际 LoanOwner 强留证明；
`08813513` accept 前预算；`8d067c15` 弱实际 caller cleanup observer；
`d9440372` exact rejected device cleanup；`d326e321` 原设备 guard 跨 Ready ACK 到实际 commit；
`d2c9240e` 同 host/pool Scope 实际 driver custody；`a1d16329` 明确 observer opt-in。
这些提交只提供相应真实职责，不单独赋予完整协议或整域资格。

## 实际生产调用者与关闭/删除边界

| 实际调用者 | 当前原 owner / 新接线 | 清理与替换门槛 | 状态及旧链 |
| --- | --- | --- | --- |
| Node / Default DNS UDP、TCP、TC 回退、DoT、DoQ、System | 各 purpose 实际 Source/Query Scope；同一 typed 四额度预算 | 原 task joins、logical return、matching native receipt；同真实 Node/Default budget | 已有 native 路径增量；组合资格按下表，不重复计基线成果 |
| DNS 的合格 taskless / Mieru / MUX / HY2 Stream detour | 实际 selected role 返回 StreamLoan；DNS child 保存真实 LoanOwner | 首 await 前同 caller 有界原件账本；sealed receiver topology 与 host driver custody 分别验证；原 pool 仍负责自身 native / Scope | 已接新实际消费；其他未迁组合不能据此删整条兼容链 |
| Default 借 domain pool 的 Node bootstrap | 实际 Node Query Scope、真实 Default 祖先 witness、原 Node resolver | 共享预算及父/子 task ledger；保留真实依赖环校验 | 删除错误的“必须同一个 Scope”假设，没有跳过 graph cycle |
| RFC6761 localhost | 同现有本地解析实现，无 OS/upstream/Fake-IP/native task | 校验原 local lifecycle、config generation、Node purpose 和族策略，停后拒绝 | System/None 与 configured Node 均恢复纯本地能力；非 localhost 仍走原 native 准入 |
| 持久 DoH | 原 client cache / H2 driver / connect lock | 单 query RST 与 pool 原 driver/native 退出分开；网络换代确认原件 | 没有第二 HTTP client/driver 池 |
| DNS WG UDP exchange 和其他未迁 detour | 仍通过既有 RawIpDevicePool / compatibility operations | 缺标准生产 Packet/Datagram role/provider | **未完成实现**；ProxyDnsOutboundConnector、Session::new(0)、prepare_datagram_exchange 仍有真实调用者 |
| 普通 Mieru 已合格配置 | 同 adapter 实际 pool、真实 grants、actual managed records、原 Scope | 总 physical cap 包括 pending / failed records；换代先实际 close，profile 与协议身份隔离 | 全 pool mixed gate 保留；不创建 native/legacy 双池。未迁 mixed/device/nondefault、UDP-underlay 缺 DatagramLoan donor、末叶 UDP caller 未迁时整个同池保 legacy；这些仍是实现缺口 |
| VMess gRPC 合格 Node Stream/Service/Datagram | 同一 Managed/record/Scope、Node DNS、真实 H2 driver 与 original grant cancellation；所有用途共享 N | 原记录、用户和 driver/native 分别收尾；原 adapter registry 原子互斥 legacy/native，新候选修改前校验真实债务 | 普通 Proxy Node TCP/Service 已接；Relay 等仍有旧执行器，跨方式迁移未完成。实际旧 debt 未证实关闭时提前拒绝候选，不能记作迁移完成 |
| taskless VLESS / VMess / Trojan / SS carrier | 同 role、TLS/framing/replay state及 same-slot CarrierOwner | observed caller 首 await 前实际 retain；声明 observer 后失败不降级 | 无实际 observer 的旧 service/business/library caller 保原 pool 债务，pre-delivery 即时取消仍未迁完 |
| VLESS / VMess / Trojan native MUX | 同协议 physical pool/driver；TCP/Datagram 使用同有界 FIFO | accepted data 与 END 同 FIFO，logical controller 归还、原 driver join、native close 分开 | gRPC/H2/QUIC/其他既有 transport 与完整 mixed prefix 仍未迁；旧上传兼容执行链仍活跃 |
| HY2 root pool | 同 authentication / QUIC / metadata owner；返回 actual StreamLoan | entry / UDP registration / payload byte 预算，失败组同计额 | 新 budgets 与 loan 已实现；不扩展既有 nonempty-prefix 能力 |
| SIP003 outbound / SS replay capacity changes | 既有真实 Child/adapter security history | 缺完整普通 process provider 和 canonical history 容量切换 | **未完成实现**；不能丢有效 replay、猜容量默认或另建安全历史池 |
| Direct 非入站 Packet | acquire 前安装原 AdoptedTunOwner / OwnedWintunOwner；同 HostPacketDevice driver；中立 ControlledPacketDevice provider/FencedPacket 已交付 | 同 Scope 真实 driver witness 在 acquire 前登记；actual join 和唯一 view 归还后，matching 原 fd / Session / Adapter close | provider/fence 是基础接线；普通 Direct Packet factory/source/routing/return/MTU/计量和完整设备 TaskRegistrar 仍未完成，生产 gate 未开启 |
| plain linked WG stacks | 同冷 PendingEntries / 原 LinkedStackFactory / EndpointStackOwner | 同冷 union，实际 NodeDNS dependency generation；StopOld→resolve→deferred actual Scope start→install→同步 commit；失败丢弃不 revoke borrowed current TCP | startup/reload/network/recovery 已由 M 接线；standalone WG 原生 UDP provider、普通 Packet 消费仍未迁完 |
| system TUN Linux/macOS/Windows | 同 retained TunCompletion / owned channels，Windows 同 SDK 原 owner 与实际 std reader | main、route、read/write joins 与 native receipt 独立；取消保留原 handles | host TaskRegistrar、适用 helper process / descendant / transfer acknowledgement 仍缺，不能写成纯环境问题 |
| Unix IPC / HTTP / SSE / gRPC | 原 controlled listener、accepted graph、TaskScope、原订阅 blocking JoinHandle | accept 前全局 charge；真实 join + matching native close 后归还；预算满拒新、不驱逐活连接 | Linux 控制 owner/预算接线已实现；Windows NamedPipeServer 原 handle provider 仍未迁 |
| runtime 统计 sampler | 既有 orchestration inline drive，同一个上限 1 的 blocking TaskScope | 首 await 前保留实际 TaskJoinWitness；stop 唤醒 interval、不再登记；shutdown 等同一个真实 join 后才关 Engine | 已移除 detached 外层/Drop abort；保留原一秒采样、十 tick endpoint 分页与原错误，不以 body 完成冒充 join |
| 初始 TLS / quota / standalone inspect | 同 ConfigurationSource、固定 QuotaStorage、原 lock 与 std persistence worker | active leases 归还→原 worker join→matching lock close；失败保实际 Source IO | 删除直接文件/锁/后台丢失原件分支；不让 Engine-only / FFI 启动 Proxy |

Driver custody 凭证只说明原 host 强留同 slot/controller 与同 Pool Scope 实际 driver；
不说明 pool cache 已发布、native 已关闭或 budget 已返还。未接管的 wire pending 仍由
actual caller 清理。Observer hint 也不授权 I/O，不可当成清理凭证或吞掉实际 retain 错误。

设备 publication 只保留一个实际候选 guard，没有第二任务、池或历史配置注册表。
endpoint Ready 只暂存候选，等待持久化后由同一个外层 commit 最终提交 DNS token；
设备 publication 完成后刷新同一个实际 incarnation 的 endpoint facts，再释放串行 apply。
实际 2 MiB worker 栈溢出通过装箱大型恢复状态修复；
完整特性图随后暴露原 1 MiB 主栈的 linked resolve 嵌套，按实际 retained invocation/resolve future 装箱缩小，原限制不变；root 编译器布局递归通过私有
非泛型 BoxFuture ABI 消除，没有提高生产栈、root recursion_limit 或既有 timeout。
失败恢复先让真实 last-good listener 消费同一 fresh pending，再确认 catalog/DNS/device publication；
不允许过早清图后从历史 capsule 复活退休 linked 原件。
同 config 可以有后续网络代际：清槽必须匹配原 Publication Arc；迟到 finish 不清新 guard。
冷准备要求同一实际 slot/Scope 与依赖代次才能复用已消费 Options；重新准备的原件
显式请求同 canonical ID 新 generation，依然先由原事务确认停旧，不能靠相同 spec 替回旧 slot。
同步 publish 与 catalog commit 之间没有 await；未确认 guard Drop 仅发 stop，不能作关闭证据。
故障期间仍保留 typed worker/native 源；部分物理完成但错误未交真实 supervisor 的恢复路径
仍可能保持 fail-closed，不能把此实施缺口记为“已成功恢复”。

## 生产预算口径

- DNS originals / queries / blocking jobs / H2 drivers 各默认 16，Node 与 Default 使用同一
  actual budget；旧省略字段不增加导出字段，改变额度进入原资源身份及替换事务。
- 合格 Mieru Managed 的所有 profile/underlay 共享总 carrier cap 默认 4，范围 1..256；pending/failed/retiring records 同占额，非每个 profile 各借一个上限。
  whole-legacy 未消费该 runtime 额度，仍每 identity 默认缓存 4、单 dialing、最多 512 keys；
  缓存淘汰/未缓存退休不构成原件关闭账本，不能据此推算实际 FD 硬上限，属于未完成预算实现。
- HY2 entries 默认 256，范围 1..256；每 authentication entry UDP registration 默认 4096，
  payload queued/fragment bytes 默认 4 MiB，上限 64 MiB。总体口径分别为 E×L、E×B，
  不是全进程只占一份。拒新增，不驱逐已有逻辑会话或已接受数据。
- taskless native stream original cap 默认 4，范围 1..4096；pending 与 delivered 同计额。
- native MUX physical originals 默认 4，范围 1..4096，同原池 TCP/Datagram/未完成原件同计额。
- 原生 MUX FIFO：8 data frames × 最大 66559 bytes，加按真实 logical concurrency 保留的
  END 位置（每项最多 1024 bytes），另计 physical driver 已出队的一帧；不是 RSS 总预算。
- VMess gRPC Node originals 默认 4096，范围 1..4096；Stream/Service/UDP、pending、
  failed、retiring 同占一个 N，两个 provider 与 TaskScope 使用同一 N，不是 UDP logical 总上限。
- WG Source owned send 在既有队列外只保留一个实际 pending IpPacket；原 buffer 与
  endpoint/generation 元数据一起跨 Pending，实际 Source join 后才能清槽。
- Direct Packet write_queue 默认 128，范围 1..4096，实际同一 mpsc FIFO 消费；配置变化先关原设备。
- control defaults connections/subscriptions 各 256、serialized queued event bytes 1 MiB。
  IPC/SSE 额度持到实际 write/flush，gRPC 持到 Tonic handoff；其 32 KiB/stream H2 send buffer
  与 replay storage 独立，handoff 不代表已写到物理连接。
- WG/TUN worker、Packet return、其他 transport/process 预算及 SS replay canonical capacity
  仍有未完成生产投影。1 CPU/1 GiB 压力结果没有以这些配置或定向数量替代。

## G0–G10 对账

| gate | 本轮状态 |
| --- | --- |
| G0 | 包 head、checkpoint、remote、同环境 worktree、角色及工具冻结；消费者样本真实资格仍按 G9 |
| G1 | 最小中立原件/loan/custody/accept/预算契约已加；不宣称未迁 Packet/process 标准 executor 已完成 |
| G2 | 同 host 冷 union、exact candidate 与实际 dependency generations 增量；剩余兼容投影仍存在 |
| G3 | 部分出站/shared carrier/队列已迁；完整 transport、mixed prefix、SIP003 和旧链删除未完成 |
| G4 | 继承基线入站能力；控制与独立资源增量不等于所有 source 平台资格 |
| G5 | 设备冷准备/丢弃/恢复与 publication、控制原 owner、Source quota 增量；仍有 fault handoff/未迁 owner 缺口 |
| G6 | DNS 实际 provider/borrowed caller、bootstrap purpose、预算与 cycle 增量；WG/其他 legacy DNS 未归零 |
| G7 | 原件/native receipts、linked Scope 与 Direct/TUN custody 增量；标准 Packet/provider/完整设备 Scope 未完成 |
| G8 | Linux 控制/订阅/TLS/quota/sampler owner与预算增量；Windows pipe 仍未迁，完整 V1 投影回归按实际结果 |
| G9 | 未执行新内核与冻结 GUI/面板真实组合；没有修改消费者换取通过 |
| G10 | 旧链未归零、互通/平台/性能和最终统一检查按实际下表；不关闭整轮 |

## 候选、实现头及验证索引

最终推送候选以 [verification.md](verification.md) 的完整 SHA 和精确源码输入为准。
最终文档提交与被测试的实现提交分别记录；文档提交不借用不同代码的资格。
完整 workspace、严格 Clippy、独立 feature/no_std/DAG 和固定参考互通的真实命令、结束码、
通过/失败/忽略/未执行见同一索引。原始日志、输入 manifest、锁和产物摘要保留在仓库外，
不是新 checkout 的构建依赖；没有上传测试二进制或临时快照到 Git。

所有 Cargo 来自 Rust 1.99.0；测试线程栈 16 MiB，显式生产 worker 2 MiB 回归保持原栈。
历史失败保留，包括锁/Go 准备、Mieru/DNS/gRPC fixture、真正 2 MiB SIGABRT、root 编译布局
递归、控制面 endpoint DNS token/facts 以及后续测试借用错误；修复后的资格按精确输入另列。
曾尝试的非 Direct non-default dial 组合被既有配置校验拒绝后已普通 revert；没有为测试
开放此前不合法组合，也不把这个历史失败归为既有产品能力回退。

C 的未完成 Direct driver 原型保存在独立 device-stage 分支，不在候选祖先中；其准确头、
未资格原因及外部保存索引见验证记录。未应用 Packet activation，不将原型测试或接口基础
当成普通生产调用者迁移。真正 System TUN source/动态 owned-create、helper 后代、
Windows pipe、全 transport/process 和 replay capacity 仍是实现缺口。

## 本地资格与唯一 TEMP

特权 TUN、系统 route/DNS 恢复、真实 Packet/Flow/源地址/MTU/回程、Windows/macOS 原件、
冻结 GUI/面板同新内核产物、完整适用官方互通及 1 CPU/1 GiB RSS/FD/任务/吞吐/延迟/恢复退出
按对应环境验收。实现尚缺的 provider/Scope/process/标准 caller 不属于这些环境资格。

`TEMP-WG-OUTER-UDP-01` 单独保留未关闭：仍仅既有 outer_udp_proxy、ProxiedRawIpWireCarrier、
legacy_outer 与重连历史。不扩大到其他路径，不从通用 helper 或测试通过推导其关闭。
FinalMask 并发 UDP 返回异常及早期 cold-spawn 夹具超时保留历史未归因状态，没有新证据不称修复。

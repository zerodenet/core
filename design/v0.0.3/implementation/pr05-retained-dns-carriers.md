# PR05：retained DNS carrier 的实施边界与下一切口

本文是实施记录，不改写原规格或 provenance。代码检查点为 `a48bf333` 与 `57d791ac`；本文撰写时子任务只执行 rustfmt / diff 检查，编译、真实测试与整体资格由根任务在冻结源码统一记录。以下“新增测试”表示源码中的回归，不能据此宣称已运行通过。

## 已交代码的精确边界

- `1ef140e2`：Proxy 实际持有 `Arc<CurrentServiceSource>`，DNS callback 只保 Weak。一次调用升级后短借当前 Engine / snapshot / inventory；没有新增永久 keeper、历史快照表或独立 owner。
- `a3535457`：EnginePlan 为每个实际 concrete outbound 保存当前 tiny 静态 token。相同 tag、完整相关 OutboundOptions（含 dial / UDP）和 source_dir 才复用；已提交的选项 ABA、删除后重加不复用。它不是资源许可、任务 join 或 I/O 关闭证明。
- `966c5e6b`：VLESS `PoolAccess` 检查实际 runtime pool owner 和实际 retire epoch；retire 更换 Arc 身份，访问者只保 Weak，取消 wrapping 计数的 ABA。旧 one-shot finishing flow 的既有 private-pool 行为保留。其他 PreparedOperation 的 retained admission 默认明确 Unsupported。
- `6f3ae743`：实际 Proxy 与 CurrentServiceSource 两个 host builder 都把同一实际 Weak source 交给短期 TcpExecutionServices；没有为请求创建临时 source owner。
- `a48bf333`：具有已实现 owner 检查的完整已选前缀可保存 selected operations、按顺序的静态 bindings、真实网络代际及原 request facts；重连临时获取当前服务，检查当前 EndpointAdmission、每个实际 owner 与上述 bindings。它不重新解析组、选择 fallback 或推进 load-balance。TCP factory 及其关联 UDP factory 不再捕获 TcpExecutionServices / 历史 snapshot；UDP factory 的 NodeHostResolver 同样改为 weak recipe。
- `57d791ac`：实际 Native 首跳还要求已有 Invocation、实际 slot 的 ServiceAdmission、当前具体 tag 对应 slot 的 Arc 身份一致。没有用整个 ComponentPlan 或 config Arc 作为门禁。新增 native SOCKS 回归是明确无 Datagram grant 的 Stream-only 前缀，不代表生产混合链路已迁移。

未知 owner、未知 UDP codec 或缺失授权仍保留明确的 legacy compatibility 分支，不能将其 Unsupported 当成成功许可。生产 legacy SOCKS associated UDP 仍属于这个范围。VLESS 当前 `on_config_reloaded` 无差别 retire 其 runtime pools；普通规则更新的**静态依赖相等**不能覆盖实际 owner 已退休的事实，也不能把仅 Engine publication 的复用测试描述成完整 reload 事务资格。

## 实际调用与保活链

| 路径 | 当前边界 |
| --- | --- |
| `proxy/runtime/dns_outbound.rs` → `dns_outbound/service.rs` → `tcp_dispatch/service.rs` | 整个 native ServicePlan 在 I/O 前准备；成功产生实际 chosen-prefix receipt；native 失败不重新提交 legacy 请求。 |
| legacy DNS fallback → `inventory/tcp/outbound.rs` → `inventory/tcp/relay.rs` | 含未迁移 XHTTP 等 hop 时使用 legacy prepared chain；其 first 通常仍是 Connect operation，不自动成为 Native first。 |
| `tcp_dispatch/relay.rs::lazy_relay_carrier` → `relay_connector::RelayStreamConnector` | `a48` 的已支持子集使用 weak recipe；未知子集仍保留原强 Execution compatibility capture。 |
| `vless/transport/outbound/plan/xhttp.rs::open_relay_connector` → `open_xhttp` → `transport/split_http/xmux/group.rs::Group.factory` | Group 真正保留 carrier factory，能够在原请求以后再建立物理连接；不能把它当 one-shot future。 |
| `tcp_dispatch/relay/datagram.rs::factory` → `PreparedDatagramRelayCarrier::build` → `adapters/vless/packet_path.rs` → `udp_flow/packet_path/tuple_flow.rs` | factory 层可弱化；实际已打开 legacy PacketPathCarrier 仍保存 UdpNetworkServices（含 Engine / upstream），这一层尚未迁移。 |

原强捕获可构成 `DnsSystem → DoH driver → XhttpStream → Group.factory → Execution.upstream.resolver → DnsSystem` 的保活环；取消、显式 retire / shutdown 可以打断它，不能据此断言每个实例都是不可释放的永久泄漏。cold Weak-drop 回归仅证明 retained factory 不再维持 Engine / config / DNS 的强引用，不证明实际 opened UDP carrier 或每个跨组件原始 socket 已物理闭合。

## 为什么不能直接替换 mixed DNS 的 first hop

`RelayStreamConnector::connect(server, port)` 不接收本次 caller 的 CarrierContext。XHTTP 的 Group.factory 也保留无本次 demand 参数的闭包。将首个 DNS query 的 cancel / 绝对 deadline 存进去，会使健康池在该 query 结束或旧 deadline 到期后无法重连；改成无限预算、重新开始原有时限或另造后台 owner 都会改变授权。

仅给 legacy SOCKS Connect operation 借旁边 Native slot 的 ServiceAdmission 也不成立：该 operation 仍执行旧协议/网络入口，并没有使用所声称的 actual Native role。`57d` 因此只检查确实为 Native 的首跳，且不把旧 associated UDP operation 一并授权。旧 Session compatibility 外壳仍是已记录缺口，不在这里制造业务 SessionId 或假 business facts。

## 下一最小生产切口

1. 先让一个真实 XHTTP pool 资源通过已有组件资源生命周期取得实际 owner、受限任务额度、retire token 和清理入口。Cold prepare 不启动驱动；关闭停止新 demand，取消并 join 旧 users / tasks，确认真实底层资源释放后才允许替代实例启动。静态 token / source 存活不替代这些许可。
2. retained connector 只保 complete selected strictly-shorter prefix 的不可变操作、Weak host source、实际角色 / pool admission 与网络身份。执行期间临时取得当前能力；相关资源退出后返回原 Revoked / StalePlan，不能重新组选或改成直连。
3. 把**每次实际 connect demand** 的现有 neutral `CarrierContext { session: None, deadline, cancel }` 沿 XHTTP pool open → Group.factory → RelayStreamConnector 透传。Service 事实来自当前 DNS demand，不能从 SessionId(0) 转换。具体签名应在实际 pool owner 接入时冻结，当前不增加一个默认无限预算的新 API。
4. 后台维护若会另开 carrier，只能使用已经 start 的实际池 owner 所授权的 control demand：来源为实际 Component / RoleInstance，purpose 为实际 BackgroundControl，预算来自已声明资源和真实维护操作。它不能复用首 query token，也不能因 Weak source 能升级就获得后台权限。
5. 先迁移 Stream-only `Native SOCKS → VLESS XHTTP`，保持原完整 destination、dial、selected prefix 和请求计数；associated UDP 在取得自身真实 owner / generation / Control 之前保持明确未迁移。保留原 TCP / UDP 共享连接数回归，不以重开第二套池满足测试。
6. 真实聚合回归须覆盖当前 query 取消不误杀其他 users、最后 user 退出与 owner retire、旧驱动实际 join 后才建立新 socket、相关 owner recovery ABA、不相关规则更新复用，以及原 source 链。分别 trait fixture 成功、远端 EOF、任务 body Ready、Drop 或 abort 都不是这些证据。

## Native Service 前缀的生命周期审查

`tcp_dispatch/service/root.rs::RootOpener` 检查精确 endpoint、空 root prefix、完整 binding 与 Service 身份，真实网络开口继续执行硬 dial / DNS role。`stream_bridge/relay/prefix.rs::PrefixOpener` 检查完整 expected binding、selected next endpoint、session identity 和真实 network generation；合并 parent / caller deadline 取最小值，同时观察两侧取消。当前一次性请求没有发现绕过严格前缀或重复 load-balance 选择的开口。

这些检查与完整长期 owner 仍不同：

- RootOpener 现在保存 UpstreamConnectServices（含强 DNS handle），PrefixOpener 保存原 OpenContext（含其 parent upstream、原 query deadline / cancel）。当前生产普通 Stream 角色不把它们保存到长期共享池；未来 resourceful XHTTP role 若直接保存它们，会重新引入 DNS 保活环和旧 query 生命周期，必须换为上述弱 bound recipe + 每次 demand。
- ServiceBinding / NativeDnsBinding 保存实际角色 admission、network watch 与 chosen selection signal，不保存 Engine / 整 config；它们提供撤权信号，没有提供底层物理关闭 receipt。DoH consumer 的 lease / driver join 也不能泛称跨 SOCKS、MUX、TLS 和原始 socket 的全部物理闭合。
- Stream 的 `Connected.io` / BoxStream 传递 I/O，目前没有 Datagram 那样独立 Control / wait_closed 的跨前缀确认通道。prefix 建立后发生 generation 变化、或最终 receipt 构造失败时，现有错误路径会 drop returned Stream；迁移有共享后台资源的角色时，不能把该 drop 当旧资源 join / physical-close ACK。
- 现有 Service start 的五秒协调上限只用于角色启动，不是新增池维护默认预算。长期后台工作必须由真实资源 owner 声明和关闭；native admission / static dependencies 本身不提供 TaskScope 或物理资源额度。

# 控制面兼容性与破坏性变更


第二轮控制预算续作保持V1控制信封、事件、命令超时、ACK/revision和CLI/FFI；新增可选
control消息字段省略不导出。IPC/HTTP/SSE/gRPC消费者与原Connection、Scope和序列化输出
原件接线仍在开发；新字段和局部测试不表示职责域关闭。详情见
[控制原请求预算续作](../design/v0.0.3/compatibility.md#控制原请求与序列化输出预算续作)。

Packet route 控制查询改为同原表冻结编码，V1形状/分页/错误不变；新增可选
`runtime.packet_route_max_routes`（默认4096，省略不导出），低于未归还原件用量时
拒绝配置替换，不驱逐活跃业务。Rust 完整 RuntimeOptionsConfig 字面量需增加 Option
字段，Default 构造不变。见[原表与额度续作](../design/v0.0.3/compatibility.md#packet-route-原表查询与生产登记额度续作)。
当前仍为开发中实现，尚未取得最终候选资格。

本文记录会影响 GUI、SDK、面板、事件 Sink 或进程内 Rust 集成的控制面语义变化。公开接口与事件说明由 [ZeroDeNet 文档仓库](https://github.com/zerodenet/docs)维护；本文只维护 Core 的版本边界和迁移要求。

第二轮续作的内部 Rust 错误转发须保留 `ChannelError` 可选的结构化 cleanup receipt；
同 payload 的显式交接不替代 native/view/Scope/用户关闭。实际 Registrar 的 Scope 退休
观察和同 ConfigurationSource 的 quota 检查入口见
[续作兼容说明](../design/v0.0.3/compatibility.md#同原件故障交接与控制-scope-续作)。
Application 状态采样改由原 TaskScope 监督并等待真实 join，正常配置替换继续监督下一
实例。保留 V1 配置/API/events、CLI/FFI、ACK/revision 与计量语义，不需要消费者改动。
当前仍为开发中续作，历史局部限制/资格不代表当前完整迁移或最终候选已完成。

第二轮完整调用者续作新增两个可选的 Engine 统计字节预算：
`runtime.event_log_stats_max_bytes`（默认 8 MiB）及
`runtime.event_subscriber_stats_max_bytes`（每订阅默认 1 MiB），范围均为 1..=256 MiB，
省略不导出。按原实际账本检查缩额，低于 held 用量保留旧配置/revision/事件并返回 Busy；
控制 ACK、V1 信封和流量结算保持。TUN 初启/换代/恢复的内部 Rust 调用者接续同一
Catalog 停旧、额度安装及真实网络代次后的 cold refresh；取消不丢原件责任。
独立 inspect 与 quota 最终 worker 继续归原 ConfigurationSource。具体 Rust 接线见
[当前调用者边界](../design/v0.0.3/compatibility.md#第二轮完整调用者续作的当前-rust-边界)。
当前 Direct Packet/TUN 已有普通装配接线，完整 TX、其余调用者/旧链删除与全部预算仍在
实现；下文先前“gate 未开启”等阶段性说明不作为当前状态。未取得统一最终候选资格，
不要求客户端/面板修改，不改变 TEMP 的原有范围。

进程内 Rust 的 Packet 缓冲现保留 `PacketBuffer` 原件，`PacketRequest` 需显式提供
实际返回 codec 布局，工作区声明增加原控制缓冲。新增 Source/Metadata/QUIC runtime
预算保留省略导出和协议原默认；V1 消费者无需修改。具体 Rust 接线与开发中资格边界见
[同原件 Packet 内存与 Source 生产预算](../design/v0.0.3/compatibility.md#同原件-packet-内存与-source-生产预算续作)。

Windows Pipe 原件工作区新增可选生产总字节额度
`runtime.control_pipe_max_workspace_bytes`（默认2MiB，省略不导出），由既有
ControlBudget按实际provider预付并保留到原件/任务/Host ACK确认。
进程内Rust `ControlBudgetLimits/Usage` 新增 `pipe_workspace_bytes` 字段和对应charge；
V1/API/events、CLI/FFI与消费者无需改动。具体计额及尚待资格边界见
[控制原件工作区](../design/v0.0.3/compatibility.md#同一控制原件的-pipe-工作区额度续作)。

## 消费者如何判断兼容性

外部消费者连接内核后应依次检查：

1. `health.engine_build_id`，确定实际运行的内核版本；
2. `capabilities.api_id` 和 `capabilities.schema_id`，确定请求与事件信封版本；
3. `capabilities.features`、`build_features` 和协议矩阵，确定当前构建实际启用的能力；
4. 本文对应版本的语义变更，再决定是否启用兼容分支。

兼容性标识的含义：

| 标识 | 当前值 | 何时必须变化 |
|------|--------|--------------|
| `api_id` | `zero.api.v1` | 请求、响应信封或既有字段出现不兼容 wire 变化 |
| `schema_id` | `zero.event.v1` | 事件信封或既有事件字段出现不兼容 wire 变化 |
| `engine_build_id` | Cargo 包版本 | wire 兼容但行为、时序或恢复语义发生变化 |

新增可选字段、未知事件类型和新增 capability 通常保持向前兼容；消费者必须忽略不认识的可选字段和事件。改变 ACK 时序、快照含义、增量合并规则、重放范围或既有字段含义，即使 JSON 形状不变，也必须在本文登记。

SOCKS5/mixed 的标准接入迁移保持既有 TCP/UDP 线格式、认证、逐包路由和计量，未增加客户端或面板工作流。`runtime.udp_direct_max_sockets` 是可选的每标准 UDP 关联 Direct 原生 socket 上限（默认 64，范围 1..=4096，按需创建）；省略时配置导出不增加字段，API/schema 标识不变。超过上限会拒绝新原生 socket，既有复用继续；需更多独立策略 socket 的部署可显式调整。内部 Rust 接入者需迁往现有 ComponentFactory/InboundSource/Datagram 角色，旧 SOCKS5/mixed listener、association handler/dispatcher 和无所有权 UDP setup 接口已删除。

Direct TCP-only 与既有同端口 TCP/UDP 入站整体迁入标准 ComponentFactory/InboundSource/Stream/Datagram，删除 Direct 旧 listener 准备/绑定和 tcp_and_peer_datagram 执行链。保留固定目标/默认端口、目标缺省静默关闭、Flow 身份/计量、空 UDP 包、客户端隔离与 TCP 半关闭。UDP 绑定使用 TCP 实际选择的本地端点；原始 UDP owner、peer 真实 Scope、Direct egress 和逻辑归还全部确认后才能替换。公开配置/API/events/FFI 不变，客户端和面板无需修改；标准 source 执行器随内核内置，原 `standard-stream-inbound-runtime` feature 名继续接受。内部 Rust 的 `ResourceDeclaration::DatagramListener` 字面量须明确 `accepted`：普通 UDP listener 为 `None`，标准 native peer source 声明独立的有限 peer/队列预算。没有扩大协议组合、native 登记或 WireGuard 临时例外。

Mieru TCP/原生 UDP underlay 入站均使用协议自有标准 ComponentFactory/InboundSource 与 Multiplex/Stream/Datagram，删除 Mieru 的 InboundListenerCapability 和最后一条 adapter 旧 listener。内部 accepted 任务/资源类型统一为 AcceptedCaller/AcceptedTasks/AcceptedResource，保留真实 TCP 或 Datagram 来源；原生 UDP admission 用同一宿主 Scope 的真实 TaskId/witness 领取唯一 peer 作用域，有限任务/逻辑资源预算按需使用。物理可靠传输 driver、并发逻辑握手、TCP carrier 和 UDP 原始 provider 均纳入现有任务/资源账本；peer 返回与原生 listener 关闭凭据分开确认，不能用 Drop/取消或端口可用代替。UDP 域名监听保留受宿主监督并 join 的真实解析再绑定，候选地址仅在原始失败已确认安全关闭后按有限额度回退；字面地址单次启动额度保持。peer 执行仅保存弱 Registrar，不自持任务宿主。保持既有认证、可靠传输、协议字节计量、逐会话策略及原协议关闭回复；旧任务、用户和原始 I/O 清理确认后才能启动替代资源。公开配置/API/events/FFI 不变，客户端与面板无需调整；没有开启普通 Mieru 出站登记、扩展协议组合或扩大 WireGuard 临时例外。

Shadowsocks 无 SIP003 插件的原生 TCP/UDP 入站接入协议自有标准 ComponentFactory/InboundSource；该范围不再调用旧 listener capability。冷准备只校验并保存完整凭据，真实宿主确认停旧后 Build 才发布到原 profile 的用户权威。保留密码套件、SS2022/SIP023、认证策略、计量及 SS2022 地址迁移后跨目标复用原关联 socket。标准 Datagram 增加逐包认证事实和来自实际 flow 的协议关联返回值，现有 UDP source 以已认证关联分组，协议拥有编解码和最新认证返回地址，宿主持有原始 listener/peer/egress 与 Scope。SIP003/SIP003U 仍单列兼容所有权，Shadowsocks 的 legacy capability 尚未整体删除；不能据此宣称剩余 adapter 或重构已完成。公开配置、API、events、FFI 与客户端面板工作流不变，未扩大协议组合、native 出站登记或 WireGuard 临时例外。

WireGuard 最后旧 listener 迁入标准 ComponentFactory/InboundSource/Packet，进程内私有 InboundListenerCapability、PreparedInboundListenerOperation、InboundDefinition、BoundInbound 和旧 listener prepare/bind/execute 已删除。标准 Packet 补齐可选的原始 UDP 来源、link MTU、接收代际/本地地址事实与宿主边界观察；默认 PacketReply/AcceptedPacket 方法仍可由现有实现沿用。真实 wire task、路由和子任务归同一实际宿主，原始 provider 与 joins 确认后才能重绑。共享 endpoint 双向复用、同 crypto 状态恢复、热更新/依赖重启、IP 分片和计量保持；TEMP-WG-OUTER-UDP-01 的既有 outer_udp_proxy/reconnect 临时边界单独保留，没有因接口删除而解决或扩大。公开配置/API/events/FFI 与客户端面板对接不变，仍为 v0.0.3 / develop。

Shadowsocks SIP003/SIP003U 的三种既有 TCP/UDP 模式整体使用标准 Inbound 工厂和真实进程/I/O 宿主，删除旧 adapter listener、InboundCarrierPlan/InboundCarrier、ShadowsocksInboundPluginPlan、TcpAndDatagramInboundListenerOperation 与 InboundConnectionContext 执行链。进程内 Rust 使用者改用现有标准工厂 Options::with_plugin；内部资源契约补齐有限 ListenerHandoff、实际端点和原始 readiness probe，TCP2022 维护任务进入实际 Scope。公网原件确认关闭后才能将端口交给 Child，直接 Child reaping、宿主 I/O 原始凭据和任务 join 全部确认后才替换；没有以此声明进程后代容器或开放普通 native Process catalog。配置 JSON、API/events/FFI、SIP003 环境/参数、SS wire 和客户端面板对接不变。WireGuard 临时边界及普通 Mieru/native MUX 出站准入不变。

VMess 入站 TCP/UDP/MUX 迁往协议自有标准 source，删除旧 adapter listener、InboundListenerCapability 和 `VmessInboundAcceptedRoute` / `accept_standard_route` 中转。TCP cipher、TLS/WS/gRPC、AuthID replay、UDP packet/raw 检测、公开 `vmess` 协议名、原计量及热凭据更新保持；共享真实宿主 Scope 和原始资源关闭凭据。独立 VMess/Trojan feature 补齐现有 Datagram 运行时依赖，不再借默认 feature 集合完成所有权装配。公开配置/API/events/FFI 不变，客户端和面板无需修改；不扩大 native 出站登记或 WireGuard 临时例外。

Trojan 入站 TCP、UDP 与 MUX 迁往协议自有标准 source，删除旧 InboundListenerCapability 和 listener 执行桥；保留 TLS/WS/gRPC、认证、协议字节计量及既有线格式。逻辑 UDP 使用同一宿主 Scope 和可复用的有限原始资源槽，实际 driver join 与原始 provider 清理完成前不能复用。内部 Rust RequestMultiplexer 增加有默认实现的认证/关闭/取消访问；公开配置、API、events、FFI 及客户端面板对接不变。此项不代表其余六个 adapter 已迁移，也不扩大 native 出站登记或 WireGuard 临时例外。

Hysteria2 既有 QUIC TCP/UDP 与可选 HTTP/HTTPS 伪装站点整体迁往协议自有标准 ComponentFactory/InboundSource 和现有 Stream/Datagram/Multiplex 契约。删除其旧 listener capability、独占 authenticated-QUIC/group/multiplex 执行器及无调用者的旧 proxy QUIC 接收链；进程内 Rust 装配应消费标准工厂，不能再调用这些私有运行时接口。QUIC、额外 TCP、文件和 TCP/Unix origin 共享真实宿主与任务监督，原始 close receipt、转交用户归还和 Scope join 全部确认后才能替换。公开配置、API、events、FFI、认证/UDP 线格式和客户端面板对接不变；保留既有主 QUIC 字面地址限制与站点域名支持，不扩展协议组合、普通 Mieru/native MUX 出站登记或 WireGuard 临时例外。

VLESS 入站仅登记实际标准 ComponentFactory/InboundSource，删除 adapter 的 InboundListenerCapability、listener.rs 和独占的旧 peer/recorded 执行链。进程内 Rust 接入者不再使用 `VlessInboundListenerRequest::accept_mkcp_peer` / `accept_recorded_mkcp_route`，应消费协议工厂的标准请求；没有新增替代 SPI。mKCP、可选 TLS 与 REALITY 各自保留原有能力，mKCP+REALITY 和无 QUIC 的 Hysteria 仍按原配置校验拒绝，不开放组合。公开配置/API/events/FFI 不变，客户端和面板无需调整；原始资源与 Scope 的关闭确认仍先于替代启动，WireGuard 临时例外和未资格原生资源不因接口删除而关闭。

已迁标准接入的受控 business TCP carrier 现在随实际 accepted 资源退休，旧连接的原始 socket 不再仅等待下次拨号回收。apply 对受影响路径继续等待真实用户/任务与原始关闭凭据；这修正内部资源时序，不改变配置、线格式或 API/schema 标识，也不授权客户端/面板改动。

共享 carrier 继续保留真实 caller 的原始失败诊断；Auxiliary 业务任务因断开/撤销结束不再被提升为共享 native 故障，Critical worker 与真实 native 失败仍按原始来源报告并保留失败边界。

第二轮云端候选增加按实际 owner 消费的可选 DNS/Mieru/HY2/native-stream/VMess-gRPC/control 预算及 Direct Packet `write_queue`，旧省略字段导出形状保持。容量不足拒绝新增或对已有订阅背压，不驱逐活跃业务或有效 replay。内部 Rust 的原件观察、device cold/recovery/publication、同源 quota/TLS 和有限 FIFO 接口变化见 [兼容记录](../design/v0.0.3/compatibility.md#第二轮云端-rust-接口与可选预算迁移)。这不是完整架构、全部出站/Packet/process、跨平台或 GUI/面板资格通过的声明；未完成实现和 TEMP 债务仍单列。统计 sampler 已改用实际 blocking Scope join；endpoint 最终提交同时收尾 DNS token 并观察实际发布设备。对尚无原件关闭证明的 VMess gRPC/Mieru 跨执行方式切换，候选在源修改/停旧前拒绝并保留当前 revision 与旧 TCP；这是未完成迁移的明确限制。ControlledPacketDevice/owned PacketTx 是基础接口，普通 Direct Packet/TUN 生产 gate 未开启。

Linux 进程原件续作增加内核 Rust 的 cold owned-process grant，保留实际 actor Scope 与原生进程树/描述符关闭分证据；公开 wire 与消费者无需改变。此时 SIP 生产消费、总体预算与其他平台仍有实现，接口出现和底层通过不代表完整迁移。详见上述兼容记录。

后续续作增加 Windows Job 原件、同 job 的真实端点预留/消费授权与可选 process job 预算，
以及 Packet 的实际 MTU、预付读取与原存储派生契约。H2 客户端原入口改为同调用者推进
原连接。省略配置字段及 V1/API/events/CLI/FFI 不变；内核 Rust 构造者和自定义 storage
的适配要求见兼容记录。真实 Windows/macOS、SIP、WG/TUN、剩余出站与全域预算仍逐项
收尾，不能据此视作完整候选、客户端同产物或跨平台资格。

原 Packet Source 的异步获取、唯一读任务 checkpoint 和按需同 Slot 进程初始化要求
内核 Rust 构造者使用实际 Scope/native binding，并给出原协议启动期限。新增 SS UDP
响应票据的每 Flow/总体可选字段省略不导出，普通生产消费尚在整轮组合。具体 API 和预算
口径见上述兼容记录；本次没有改变 V1 wire、ACK/revision、事件、CLI 或 FFI，未取得
完整候选或客户端同产物资格。

## 版本矩阵

| 版本 | 影响面 | 迁移结论 |
|------|--------|----------|
| `Unreleased` | 共享资源替换、VMess cipher 配置、Raw-IP Direct 资源关闭 | 已迁移的严格路径确认停旧后才启动替代；WireGuard outer_udp_proxy 尚有临时兼容债务；旧私有 `zero` 配置需迁移为 `zero-plus`；Raw-IP 已覆盖路径使用真实任务与原生关闭凭据 <!-- version-contract:unreleased-row --> |
| `0.0.1` | 首个统一正式版、TUN 路由恢复 | 发布编号重置；配置与控制面仍为 V1，旧版本需手动安装 |
| `0.0.16-rc.202609070904` | Direct 入站、监听热更新、构建能力发现 | Direct 默认绑定 TCP 与 UDP；仅需 TCP 时使用现有 `udp.enabled: false`，面板按能力事实判断支持 |
| `0.0.16-rc.202609060636` | CLI 配置预检查、内核安装器 | `zero validate` 不再启动运行时或访问 Fake-IP / 配额持久化状态；可与运行中的内核并行校验 |
| `0.0.16-rc.202609051609` | - | No pending compatibility changes |
| `0.0.16-rc.202609050728` | GUI 首次启动、进程内 Proxy 集成 | 空入站且无 TUN 配置进入管理待命；控制面可应用第一个监听，移除最后监听后继续待命 |
| `0.0.16-rc.202609041712` | - | No pending compatibility changes |
| `0.0.16-rc.202609041703` | - | No pending compatibility changes |
| `0.0.16-rc.202609041004` | - | No pending compatibility changes |
| `0.0.16-rc.3` | - | No pending compatibility changes |
| `0.0.16-rc.2` | - | No pending compatibility changes |
| `0.0.16-rc.1` | - | No pending compatibility changes |
| `0.0.15` | - | No pending compatibility changes |
| `0.0.15-rc.4` | - | No pending compatibility changes |
| `0.0.15-rc.3` | Diagnostics API 消费者、Connector 运维与事件 Sink | `diagnostics.trace_route` 改为执行真实会话路由追踪；Connector 对事实事件施加有界内存与可选 outbox 背压，采样事件改为仅 best-effort 且不持久化；默认重试次数由 3 调整为 10 |
| `0.0.15-rc.2` | 构建脚本、事件消费者、Webhook 接收端 | 公开 Cargo feature 改用 kebab-case；引擎生成的 `event_id` 增加每次启动唯一的随机 epoch；开发期固定中心 API 被撤销，Connector 收缩为通用 Webhook 事件投递；认证项速率改为 Zero 主体策略聚合 |
| `0.0.15-rc.1` | 进程内 Rust `EventSource`、事件 Sink | Rust 实现者必须迁移到实时 `EventStream`；IPC/HTTP/gRPC GUI wire 无变化 |
| `0.0.15-rc` | GUI flow 生命周期 | 订阅 ACK 后以 `flow.snapshot` 建立活动连接基线，再合并 flow 增量 |

## Unreleased

### 控制 Health 与 Capabilities 的原件投影

HTTP、IPC 和 gRPC 从同一 Proxy/Engine/设备/协议登记读取 Health 与 Capabilities，
先由原响应账预付必要标量、descriptor 和排序行存储，再编码原 V1 形状。
build-feature、配置标签及协议静态能力事实保持借用；不另建查询执行器或资源池。
已有 owned QueryService/TrafficStatisticsCapability API 继续承担独立 Rust 调用者职责；
新增借用投影及其纯事实构造不改变配置/API/events、CLI、FFI 或面板消费者。
对应回归位于 Proxy control_query/tests 和 gRPC messages/proxy_query/tests.rs。

### 同一原件清理故障与实际退休

内部 Rust `CloseReport` 增加 `resource_failures`；完整 struct literal 需提供该字段或使用
`..Default::default()`。实际组件的协议控制器、声明资源控制器故障以真实 instance 和
controller phase 进入既有 Plan 归档，不再伪装为任务失败。TaskId 和 TaskFailure 语义保持。
故障接收与原件、逻辑用户、Scope join/Host ACK 是独立事实；缺少匹配关闭凭据继续阻止
替换。同一原件晚清理完成后可以推进退休，原始故障仍交给原 supervisor，不因恢复删除。
对应回归位于 `crates/runtime/src/component/tests/replacement/resource_fault.rs`。
V1 配置/API/events、CLI、FFI 及面板 wire 形状保持，无需消费者迁移。

### 第二轮内部 Datagram 原件转交续作

进程内 Rust `ResourceDeclaration` 新增借入专用 `ControlledServiceDatagram`；穷举实现需
处理新变体。`LoanDatagramIo::association()/wire_association()` 区分实际逻辑调用者与
原 Tx/Rx 绑定，`into_session` 的真实 codec 须使用原 wire ID；SS 借入 consumer 已配对。
`BorrowedPacketSegment` 使用同 Factory Scope 的真实 driver 和既有队列，关闭须分别
确认原件、join 和 Host ACK。现 QUIC 完整生产 consumer 仍在实现，尚非最终资格。
具体调用者与 Rust 适配见[同原 Datagram 转交](../design/v0.0.3/compatibility.md#同一-datagramloan-的实际-wire-与逻辑-association-续作)。
V1/API/events、CLI/FFI 和客户端面板保持兼容，无需消费者迁移。


### v0.0.3 云端配置资源与 Hysteria2 出站

Config 的运行资源编译迁入 `zero-runtime::configuration`。Rust 嵌入者应消费准备值；Engine 保留领域快照/策略/revision 与原有公开入口，资源持久化和恢复委托同一个 ConfigurationSource。Proxy 的跨组件提交仍使用唯一 RuntimeHost 的 ApplyOwner，已受理操作由宿主持有到最终 ACK；应用 reconciler 必须确认失败候选真实退休后才恢复。Engine-only/FFI 不扩大为网络宿主。

Rust `RuntimeConfig::load_from_path` 的文档 I/O 入口删除，嵌入调用方改用 `ConfigurationSource::load_document`；需要继续准备或持久化时，持同一 `Arc<ConfigurationSource>` 调用 `load_initial_configuration` 并传入 `PreparedConfiguration::prepare_startup`。已有 `validate_config` 保留；已准备输入可用 `validate_prepared_configuration`，避免重复打开规则资源。CLI validate、connector-state 与 reload 的文档读取也使用真实源原件，V1 校验和控制消息形状不变。

初始 URL 规则预取复用既有应用 TCP 原件和 TaskScope 实现，下载为有限队列的流式 HTTP/HTTPS，保留相对路径、验证 TLS、GET 重定向、URL Basic 认证与新鲜/陈旧缓存语义。网络转交用户、任务 join 与原始 native 控制器实际关闭后才重定向或使用缓存；临时文件同步及原件 close/matches 后才安装不可变缓存，取消不丢原件或凭 Drop 放行 Engine 构建。

Root Hysteria2 出站统一到协议拥有的同一真实认证池和标准角色；删除私有 TCP/UDP 桥及最后无调用者的 SessionTcpHandshake/SessionTcpConnectOperation。保留协议库公开 standalone convenience 范围，不作为 root 回退。CA 材料在同一启动准备中读取，旧配方恢复使用已准备的原 crypto；原始用户归还、TaskScope join 和 native close 独立确认。

没有改变 V1 配置/API/events、CLI、FFI ABI 或协议基线，也没有增加协议组合。GUI/面板、特权 TUN、其他平台及 1 CPU/1 GiB 仍待最终候选真实验收；TEMP-WG-OUTER-UDP-01 保留未关闭。

运行配置增加可选的 `runtime.event_network_max_tasks`、`event_network_max_originals`、
`event_network_max_lookups`，默认分别为 1024、256、256，范围 1..4096；
省略时导出仍省略。实际应用网络 Epoch 使用这些限制，并与同一个 RuntimeHost 的
Carrier 总原件额度共同约束；不是第二个连接池。初始规则下载持有的
`ProxyPreparation` 在原网络、缓存原件及 Scope 清理确认后移交 `Proxy::from_preparation`，
继续使用同一个 Host。Rust 嵌入者使用应用网络构造器时须提供该 Host 真实签发的
component identity 与实际 root；不能自行拼装 ID 或建立替代 Host。

进程查找不再暴露无 owner 的执行入口；实际 lookup 在原 caller Scope 内登记阻塞
worker，持有平台原始 File/DIR/HANDLE 和同 Host 原件额度，原生关闭、worker join 与
Host ACK 分别确认。Logical Packet 注册只观察真实软件 driver 和同 Scope 的计额责任，
不会签发物理设备或 outer carrier 的 native-close 凭据；唯一 TEMP 债务不因此关闭。

### HTTP 标准监听与原始连接所有权

HTTP 字面量及域名监听使用协议的 `ComponentFactory`/`InboundSource` 和真实宿主所有者，HTTP 的旧 listener capability/operation 准备已删除。接受条目、任务登记与关闭接收方来自同一个原始监听；热更新继续先确认旧资源关闭，再装配替代，候选失败后使用实际旧配置恢复。域名沿用系统解析，在原始 Scope 内监督实际 lookup worker 并等待其真实 join，冻结系统顺序的多个地址，失败原始句柄明确关闭后才允许尝试下一地址；所有尝试共用一份有限 native 创建预算。HTTP CONNECT、Forward、逐消息路由与计量、keep-alive、Upgrade 保留既有线格式和 V1 控制面形状，客户端与面板沿用既有接入方式。

该路径按需记录最多 4096 个在用、退役或 Unknown 原始连接，每连接一个宿主主任务；达到上限暂停 accept，不预建任务/连接。当前额度由内核装配提供，统一的用户配置入口尚未完成。剩余 10 个 adapter 的旧 listener 链仍未整体迁移，本项不是接入职责域或整体重构完成的证明。

### WireGuard outer_udp_proxy 临时生命周期例外

2026-10-31 用户确认：既有 `outer_udp_proxy` 及重连历史的兼容生命周期只作为临时方案保留，未来必须纠正。当前换代仍有 30 秒退役宽限，可能短暂并存新旧资源；原始载体的 native 关闭凭据尚未补齐。这条路径不能宣称满足严格的确认停旧后启新，也不能计入完整 native 所有权或整体资源生命周期已完成。

本例外不扩大到其他资源，不开放相应普通 native 登记，不新增协议组合，不要求客户端或面板改动。后续由内核补齐实际原始所有者、真实任务退出、转交资源归还与原始关闭凭据，移除兼容换代重叠，并按 [TEMP-WG-OUTER-UDP-01 的验收条件](../design/v0.0.3/lifecycle.md#temp-wg-outer-udp-01)关闭债务。旧接口删除或其他路径测试通过均不构成该项完成证据。

### VLESS and Trojan root MUX prefixes for VMess gRPC UDP

Ordinary root-prefix admission extends to qualified literal/default-dial VLESS with explicitly enabled, equal XUDP/MUX concurrency and to Trojan MUX. The protocol factories expose Stream-only roles; compatible UDP uses the same actual shared pool, native owner and supervised physical driver. VLESS preserves its XUDP global ID and packet compatibility entry; ordinary non-XUDP UDP is not silently converted. Trojan retains its fixed-target/raw-packet constraints. Non-root/nested and other unqualified compositions remain on their existing ownership boundary.

The stable node ownership fence and the complete actual carrier description are separate: changing a WS profile changes the native recipe and requires confirmed old-original cleanup. Existing `runtime.native_mux_max_carriers` applies with its unchanged default and optional serialization. No additional public configuration fields, V1 API/event shapes, FFI or client/panel project changes are introduced.

### Ordinary VMess MUX root prefixes for VMess gRPC UDP

The ordinary develop/v0.0.3 catalog admits the qualified literal TCP/TLS/WS VMess MUX root-prefix subset used by non-MUX VMess gRPC UDP relays. It advertises the actual Stream role and original shared-pool ownership; ordinary TCP roles and compatible UDP calls delegate to the same supervised physical pool while retaining existing protocol framing and accounting. Non-root/nested or unqualified combinations, ordinary VLESS/Trojan native MUX and Mieru remain on their existing compatibility boundary. Previously exposed legacy resources cannot be adopted after cache eviction or transport changes of the same node.

Optional `runtime.native_mux_max_carriers` bounds on-demand physical carriers and their tasks per admitted pool (1 through 4096, default 4). Omission retains the exported configuration shape. This intentionally replaces unbounded native-pool growth with a finite resource ceiling; exhausted live/retiring capacity fails explicitly without spawning another pool. Existing V1 APIs/events, FFI and protocol configuration fields are unchanged; client and panel projects are not modified. Rust callers constructing `RuntimeOptionsConfig` with exhaustive struct literals must initialize the new optional field or use `..Default::default()`.

### VMess gRPC UDP with shared-state and hosted pooled prefixes

On develop/v0.0.3, ordinary standard Stream prefixes may retain non-I/O shared dependencies such as the actual Shadowsocks TCP2022 replay owner instead of reverting the whole gRPC UDP relay to legacy preparation. Their wire handshakes, dependency/task ownership and Datagram Flow/Outer accounting are preserved.

The explicitly hosted VLESS/VMess/Trojan MUX roles additionally return real logical Stream loans with their original physical-pool lineage. The final gRPC owner installs the actual loan before later handshakes and retains its original H2 task owner. Middle pools retain upstream loans; host generation edges order consumer and donor retirement before replacement. Returning a logical stream does not certify donor native closure. The ordinary native MUX/Mieru registration gates remain closed; these hosted combinations do not qualify every pooled or mixed production path.

No public configuration, V1 API/event shape, FFI or protocol framing changes are introduced. Client and panel projects are not modified. Legacy interfaces remain for unqualified existing callers; migrated standard paths use the actual executable roles. In-process Rust implementations overriding the internal `UpstreamOpener::prepare_controlled_stream_async` migrate to `acquire_controlled_stream`; the redundant public async prepared surface is removed. Synchronous native preparation remains for owners that must persist the actual controller before awaiting I/O. Errors preserve their business identity, and DNS preparation failures cannot enter TCP address retry.

### VMess gRPC UDP over standard Stream relay prefixes

On develop/v0.0.3, ordinary non-MUX VMess gRPC UDP relays with default dial policy and task/pool/dependency-free standard Stream prefixes dispatch directly through the actual Datagram role and existing logical UDP session/accounting executor. The host owns the one physical root TCP carrier, executes each protocol's Stream handshake inline, and then uses the original TLS/gRPC implementation and scoped H2 driver. The original business grant is bound to the selected final endpoint and full actual prefix generations; selected role failures cannot reopen via the legacy UDP executor or direct dialing.

The existing host generation graph propagates prefix changes to the actual consumer. Replacement waits for transferred users, original drivers, prefix leases and original native close receipts before starting successors. A domain root uses the same configured native Node DNS provider; the final hostname is delegated through the prefix and retains its TLS/CA identity and logged upstream. Only physical TCP failures before prefix handshakes may try another DNS address, after confirmed original close. Mixed, pooled, non-default-dial and otherwise unqualified prefixes retain their existing compatibility callers; native MUX/Mieru registration remains closed.

Public configuration, protocol frames, Flow/Outer accounting, V1 APIs/events and FFI remain unchanged in this increment. Client and panel projects need no adaptation and are not modified. This is a qualified relay migration, not completion of all legacy relay interfaces or the whole architecture plan.

### VMess gRPC UDP with component-owned Node DNS

On develop/v0.0.3, non-MUX VMess gRPC UDP with domain node endpoints may use the standard Datagram production role when the configured DNS servers support the actual native query provider (literal/bootstrap peers, no System/detour backend, required encrypted-DNS features compiled). Cold selection checks the candidate DNS configuration before migration. DNS failures after selection remain failures; they never select the same node's legacy UDP executor. Other configurations retain their existing compatibility path.

The same RuntimeHost owns the shared DNS provider and its transitive gRPC consumers. Queries use the configured Node policy, real query TaskScope and original socket receipts. Native and legacy cache entries are isolated within the same configured total LRU/TTL budget. The configured hostname remains the TLS/SNI identity and logged upstream; the selected IP remains the actual connection endpoint. Address candidates are attempted serially, with the failed original confirmed closed before another socket is acquired. Network changes fence pending answers and new gRPC acquisition until old users, drivers and original controls have settled; changing DNS configuration applies confirmed StopOld-before-BuildNew to the dependency and its consumers.

Public configuration, protocol framing, Flow accounting, V1 APIs/events and FFI are unchanged in this increment; clients and panels require no adaptation. TCP, relay, external ECH, System/detour DNS and native MUX/Mieru registration remain outside this increment. This records the qualified path, not completion of the whole architecture migration.

### VMess gRPC UDP with component-owned CA files

On develop/v0.0.3, non-MUX VMess gRPC UDP with literal node endpoints and supported prepared TLS now uses the standard Datagram production role with external CA files. Relative CA paths resolve beside the configuration. The actual host reads one bounded CA snapshot during component start; new flows use that snapshot. Replacing the configured CA path or file budget uses confirmed StopOld-before-BuildNew, including transferred users, the original H2 driver and native carrier receipts.

The optional `runtime.file_read_max_bytes` configures the per-file snapshot budget (default 8 MiB). It must be positive and leave room for the overflow probe. Configurations using larger CA bundles must set a larger budget. Omission preserves the previous exported runtime JSON shape. The existing `tls.ca_cert_path`, V1 API/events, control transports and FFI remain supported; clients and panels need no changes for existing configurations. File-read, PEM or trust failures remain errors rather than silently selecting the legacy UDP executor. In-process Rust integrations constructing `RuntimeOptionsConfig` exhaustively must add `file_read_max_bytes: None` or use `..Default::default()`. Hostname/ECH/relay and MUX admission remain separately scoped.


### Protocol-owned TCP preparation and MUX owner isolation

VLESS/VMess/Trojan compatibility TCP preparation now materializes its protocol leaf from the actual configuration source, without retaining an adapter-runtime factory closure. TCP errors remain deferred until TCP selection. The same real protocol MUX map distinguishes outbound owners and their complete static transport configuration: an existing carrier cannot bypass changed TLS/WS authentication or attribute another outbound's I/O to the original owner. Same-owner TCP/UDP reuse remains supported. Public config, API, events, FFI and protocol framing are unchanged; clients/panels need no adaptation. Cache isolation/eviction is not native close confirmation; existing confirmed replacement and production admission gates remain in effect.

### Rejection cleanup and protocol startup supervision

Configuration rejection confirms and reports the failed candidate's original bind-owner cleanup before rebuilding last-good component, TUN or DNS resources. Incomplete cleanup continues to block acquisition, and published closed faults cannot mask another pending original. WireGuard recovery retains its original crypto state. SIP003 startup observes actual child exit while its TCP readiness probe is pending and retains the same failure/child owner without respawning. Source-relative HY2 TCP preparation removes another claim wrapper while retaining the same protocol-owned pool and CA path semantics. Public configuration, control API and protocol framing are unchanged; native production gates remain in effect.

### TCP pooled compatibility execution convergence

Mieru pooled TCP relays now use the common socket executor and the existing protocol-owned lazy pool lookup. Socket and transport TCP connect/relay contracts share one owned operation per kind; TCP no longer adds the forwarding-only prepared transport wrapper. This removes adapter-specific relay preparation/execution shells while preserving the actual pool, prefix identity, generation and single-use carrier behavior. Pool-open errors retain their original typed source. Public configuration, API and wire framing are unchanged; native production admission and confirmed old-resource cleanup remain required.

### Mixed TCP relays select standard Stream suffixes

Mixed TCP relay chains select the ordinary Stream suffix after the last unmigrated node directly from the actual catalog, without requiring standard nodes to provide legacy TCP claimers. Unmigrated pools retain their original shorter prefixes until actual carrier borrowing and closure qualify. Standard hops retain actual generation references and check host admission before opening the selected prefix. A failed component catalog for an explicit request returns its original error instead of silently choosing legacy TCP. The compatibility prefix grants no new native ownership or direct fallback; possibly submitted failures cannot replay. Ordinary socket and Direct preparation remove redundant claim wrappers. Public configuration, API schemas and protocol framing are unchanged. UDP/device prefix callers and unqualified resource owners retain their existing explicit migration gates; no additional production factory is enabled.

### Standard outbound Datagram association execution

SOCKS5 UDP association dispatch now consumes the compiled standard Datagram role through a protocol-neutral runtime owner. The upstream handler-provider registry and opaque resume probing are removed. Endpoint references use actual component generations without retaining credentials or previous configurations. Reopening after replacement, idle expiry or send failure waits for the retained original session controller's complete close receipt; cancellation and incomplete cleanup retain that owner and block acquisition. Stale plan errors keep their structured channel cause, and local cleanup errors retain the original source without penalizing an unattempted upstream. The internal `upstream-association-runtime` feature now declares its required UDP response support, without relying on SOCKS5 to enable it. Existing public protocol feature sets, configuration, control-plane schemas, protocol framing and wire-byte accounting remain unchanged. This does not qualify missing native dual-original providers or enable additional production factories.

### Background carrier borrowing tracks actual cross-owner generations

The develop/v0.0.3 internal qualification assembly records actual background borrowing dependencies before I/O. A changed or removed donor propagates retirement through its consumers; confirmed original closure, task joins and transferred control/user settlement still precede candidate start. Taskless native stream/datagram custody retains its actual original controller and explicitly has no driver. In-process users of `zero_platform_tokio::controlled_carrier::CarrierOrigin::driver` must handle `Option<&TaskJoinWitness>`; `None` cannot substitute for native close evidence.

Public configuration, V1 API/events, IPC/HTTP/gRPC and FFI ABI are unchanged. Ordinary Mieru background production registration remains disabled pending the complete provider and qualification gates; this internal Rust contract does not advertise additional public protocol support. Local evidence and remaining gates are recorded in `design/v0.0.3/implementation/cross-owner.md`.

The internal `ResourceDeclaration::ControlledMetadataDatagram` and `ResourceAccess::register_metadata_pool` support on-demand original ancillary sockets for the same host Scope. Rejected unused-view claims remain with their original owner; successful native retirement requires the actual original user/driver graph and main/probe close receipts. Startup-only `MetadataDatagram` semantics and public configuration/control schemas are unchanged. Actual Proxy DoQ qualification is recorded separately in `design/v0.0.3/implementation/native-doq.md`; it does not enable ordinary Mieru production registration.

### RuntimeHost shared-resource replacement confirms StopOld before BuildNew

The develop/v0.0.3 host now owns the component catalog and the existing single accepted configuration task. Protocol lookup no longer owns the lifecycle catalog. Rebuilding a shared/native owner revokes affected admission and waits for transferred users, actual component tasks and original provider close evidence before building its replacement. Timeout, a dropped waiter or a protocol-only report cannot grant a second instance. A failed candidate must close before last-good reconstruction, which creates a new generation rather than reopening the stopped handle.

Affected connections may disconnect when their actual shared owner is rebuilt. Ordinary rule changes and unchanged resource owners retain their connections. Historical role users remain charged to the actual shared generation after the old role is released; cached fenced handles cannot pin retired private roles. Native Shadowsocks TCP without plugins uses the owning adapter's existing salt history and one canonical replay-maintenance owner. Plugins and other unconverted paths retain their compatibility implementations; unsupported native provider/role combinations are still rejected.

Changing a native Direct outbound's dial policy now closes that owner's existing TCP relays before the apply ACK, including explicit source-bound and address-family-only policies. Other unchanged outbound owners remain usable. Native SOCKS5 handshake writes explicitly flush their supplied carrier, matching the existing platform socket behavior, so a buffered Shadowsocks intermediate hop can complete authentication and CONNECT without changing wire frames.

V1 configuration, API/event envelopes and the Engine-only FFI ABI are unchanged; no GUI/panel schema migration is required. Cleanup failure can reject an apply while the last committed revision remains current and affected resources are faulted. A lost response is resolved by querying core_instance_id/config_revision rather than starting a second kernel. This is an internal host implementation, not completion of independent zero-runtime extraction, all protocol roles or device qualification. Validation is recorded separately in design/v0.0.3/implementation/README.md.

### Raw-IP Direct caller closure uses original native receipts

Standalone TCP and DNS service execution now drive their owning component supervision while connecting. WireGuard key rotation selects a new live crypto owner even on the same listen address. Retired packet-to-flow and owned QUIC streams return terminal `BrokenPipe` instead of retryable `Interrupted`, preventing read helpers from spinning after cancellation; actual caller join and transferred-view release remain separate closure requirements. Probe diagnostics classify the original typed establishment error while retaining its outbound/stage metadata. Public configuration and wire identifiers are unchanged.

WireGuard/raw-IP TCP Direct sessions now settle the same controlled carrier registrations after the actual caller Scope join; raw-IP UDP associations use the existing bound Direct provider and retain its matching native/driver/caller completion. Completed covered records can return their quota. Uncovered gateways, DNS work without a controlled root, conflicting owners and incomplete native closes remain Unknown and charged. This does not qualify the remaining resource migrations or non-Direct native roles.

Production Direct decoding now installs the existing controlled stream owner with 4096 native carrier slots, 256 refresh task slots and a five-second retirement budget. An opener with the original tracked Business caller selects it before acquisition; an explicitly selected controlled operation never falls back on failure. Service and untracked calls keep their separate legacy operation and do not establish whole-generation native coverage. Other resourceful cold gates remain closed.

Each raw-IP UDP association has explicit ceilings of 256 logical sessions, 256 native sockets, one pending native open, 16 MiB pending payloads, 1024 candidates per job and 64 policy/network bindings. Native bind fallback has two pre-I/O family attempts and owner start has a five-second deadline. Exhaustion fails through the existing error path; it never switches to an untracked socket. These are finite safety ceilings, not throughput or production sizing claims. Existing bound-provider budget, source-revocation and original-close regressions cover the provider; raw TCP refusal/success and WireGuard inbound interop cover runtime integration.

Same-address listener candidates use replacement preparation; only explicit rollback selects restoration, because linked outbound dependencies can change without changing inbound fields. Whole-batch compensation now carries that explicit restoration mode through listener preparation after failed candidate reservations are physically retired. A controlled bound UDP listener may use kernel routing when neither TUN nor a forced interface is active, without acquiring a hidden route-probe socket. Active TUN and forced-egress checks remain enforced. Regressions cover WireGuard unlink/relink, physical-egress recovery and the original closed-owner transition.

V1 config/API/events and FFI ABI are unchanged. GUI/SDK consumers need no wire migration; embedding applications must await shutdown and retain unconfirmed generations rather than infer closure from cancellation or Drop.

### Cancellation callback panics retain a usable original tree

The workspace preserves tokio-util 0.7.18 with a local cancellation-tree patch. It publishes the actual cancelled nodes and releases ancestor locks before notifying waiters; a panicking waker cannot poison the parent or prevent sibling notifications, and the original first panic resumes after delivery. Task/native closure still requires the original join/provider receipts. Provenance and the exact local change are recorded in `vendor/tokio-util/UPSTREAM.md`; regressions use actual registrar cancellation and the proxy monitor's pending I/O/real task-exit path. No protocol package baseline, V1 wire format or FFI ABI changes.

### Component task observation stays active during start and shutdown

Hosted component task exits now use the original bounded Scope registration and completed queue throughout Proxy startup, reconciliation, Service diagnostics and shutdown. One inline catalog driver consumes actual native joins while lifecycle start or cleanup is suspended. A Critical failure retains the same TaskId, class and original error Arc before revoking that generation and rejecting pending start; cancellation cannot erase that source. Auxiliary failures keep the owning protocol's local semantics: its bounded receiver must retain the actual joined error before the host acknowledges the exit. Successful deliveries do not form a lifetime TaskId history. The ordinary public TaskScope behavior is unchanged; HostedScope adds only a bounded snapshot of native joins still outstanding.

V1 config/API/events and FFI ABI are unchanged; no GUI/SDK migration is needed. Existing zero-resource cold role handles remain valid handles with invocation admission still fenced. Actual task/native starts require the installed host generation, and asynchronous commit paths scan through the same driver. Scope joins and declared lifecycle completion do not prove accepted-user/native graph closure; eligible shared-resource activation additionally requires the RuntimeHost transaction described above. Regressions use the actual HostedScope consumer with pending stream reads, native JoinError identity, Auxiliary receiver acknowledgement, nested leases, cancellation takeover and repeated finite-capacity cycles in `component/tests/lifecycle_scope.rs` and `lifecycle_scope/hosted.rs`; build/runtime qualification is recorded separately.

### Listener replacement uses the single-instance StopOld boundary

Changed listeners now prepare cold operation values, stop every affected old listener and confirm its actual retirement before binding any candidate, including candidates on a different port. Unchanged actual listener owners and existing protocol credential hot-update paths continue to be reused. This removes standby port reservation to match `design/v0.0.3/lifecycle.md`; a port conflict can therefore appear during the switch after old connections have stopped. Candidate cleanup and recovery use the actual last-good snapshot, including tags already removed from the active map by a cancelled attempt. A recovered listener is a new generation, not the original FD or a restoration of disconnected sessions. Stateful protocol security history must not be silently replaced; an unconfirmed or unsupported recovery remains an explicit failure. Pending native bind owners, their original failures and Unknown outcomes remain retained across cancellation and block further acquisition until resolved.

V1 configuration/API/events and FFI ABI are unchanged; no caller schema migration is required. A controlled listener close receipt does not establish closure of every accepted child socket or completion of all M4 resources. Validation for this change is recorded separately.

### WireGuard 同实际安全状态的 listener 恢复

WireGuard listener 使用原 canonical UDP native owner 与独立 NativeIcmp 声明；资源、原任务及转交用户仍须由实际宿主图确认关闭。冷准备不再取得 linked receiver 或改变运行设备。最后成功 listener 的 opaque 行为持有同一 LiveInboundDevice；宿主确认确切旧实例 terminal 后，恢复与同私钥端口迁移继续使用该真实 crypto owner，保留原重放、握手、计数、cookie/limiter 与认证源状态。候选依赖必须来自真实 BuildNew，未就绪、Unknown 或已退休 linked 资源不能因 Arc 仍在而复活。不同私钥候选使用独立被动状态，弱缓存只随实际 current/pending/recovery owner 存活并清理 dead entries。

配置、wire、API/events 与 FFI ABI 不变，无 GUI/SDK 迁移；恢复不是原 FD 或已断开会话的复活。本条记录 listener/capsule 接线，完整 linked outbound 冷启动与真实图验证独立交付，不能据此宣称整个 M4 完成。回归见 `adapters/wireguard/tests/inbound_state.rs` 与 `adapters/wireguard/inbound/recovery/tests.rs`；保留原 `wireguard_inbound.rs` 的失败候选后状态/业务断言。

Raw-IP 子 native 义务另在原 gateway 前持久登记，并计入真实 TCP4096、UDP association1024、DNS256 名额。未进入 gateway 的 reservation 可确认无 native attempt；进入未受控 gateway 后，future 返回、取消、socket Drop 和真实任务 join 均不能释放其槽位或签发 native Closed，原始失败与未确认记录继续阻止宿主 terminal/恢复。该门禁明确暴露尚待受控 carrier/UDP/DNS 接线的路径，不是兼容性完成或永久 Unsupported 方案；普通 TUN 的既有路径不变。

Raw-IP UDP association 原 future 现在只登记一次到同一实际 listener Scope 的独立 1024 childgroup；父 Scope 声明为该组上限与原 driver 的 checked sum，不借用旧 Direct peer 的 4096 或 DNS256 额度。完成消息只用于旧 association/backoff 元数据，真实 TaskExit 与同原 native owner 收尾仍独立；未受控 gateway 的义务门禁继续保留。普通 TUN 保留原 JoinSet 路径，内部 raw-ip-runtime 显式依赖既有 managed-datagram-runtime，以取得真实 HostPeerBinding/PeerResourceContext，不新增配置字段或默认 grant。

Raw-IP TCP 原连接 future 同样只登记一次到该 listener Scope 的独立 4096 childgroup，父额度按 TCP4096 + UDP1024 + 原 driver1 的 checked sum（5121）声明。原 Session 占位值仍经既有 Engine.prepare_session 获得真实业务身份及完整认证/策略处理后才进入所选 leaf；真实 caller 的 opaque 同槽观察随原 admitted Invocation 传递，停止或实际 joined caller 撤销同请求取消令牌。观察本身不授予 native open，不替换 selected-outbound/current 检查，也不能因任务结束清除 Unaccounted：最终仍需 C provider 的同原 grant/adoption/native Closed 与实际 caller join。

BoundUDP 的同一持久 peer provider 新增 opaque 完成观察：同一个实际 Scope 消费原 peer TaskExit、协议 mapping 清理与原 native owner 显式关闭全部确认后才发布。完成凭据匹配私有 provider origin，不能由相同数值 ID、任务结束、view Drop 或已关闭部分 FD 代造；等待取消与未知结果继续保留原 owner/错误。本观察不授新 open，不清除尚未完整接入的 raw-IP Unaccounted，也不提供推断的生产容量默认值。

显式调用方 BoundDirectBudget 现在可令原 UdpDispatch 的 Direct 首包、后续包、接收与 retire 共用同一实际 PeerResourceContext 下的 BoundUDP provider；原 selected Direct 操作提取同一 current host/source，协议不能自行选择 policy 或替换 generation。原 raw-IP association 在同一实际任务内驱动原 Direct driver，并保留 consumer 与 driver 同时失败的原 typed source。预算全部由调用方明确传入，不从外层 association1024、队列或测试数值造默认；缺省仍不启受控 grant。共享 resolver、其他 UDP 协议 gateway、原 auto bind-failure family fallback 资格和完整 caller-native coverage 仍须独立闭合，不能凭 Direct provider receipt 清除整个 association 的未知义务。

原 listener inline observer 现在并行推进真实 Scope exit 与已 joined child 的协议/native 清理，不等整个 listener 停止才归还已确切关闭的 child 容量。Pending 清理不会阻挡同一 observer 消费其他实际 TaskExit；取消观察保留原 owner 和 receipts，重入先处理已 joined 的原 child。首次实际清理错误在下一 await 前持久保存并撤销准入，Unknown 槽不回收；此处仍不把 TaskExit、body Ready 或空声明当 native Closed。

BoundUDP 新增独立 default-deny 的 typed acquisition attempt 能力；旧 opener 仅投影原错误，不据 diagnostic source 授权重试。原 UDP owner 的私有身份与不可改写 evidence 绑定真实 acquisition outcome、reservation 和原 SourceArc；相同数值 ID、其他 owner 或改写公开诊断字段不能取得 settled 证明。只有原 owner 已结算 KnownNoNative/明确 Closed、同一私有 binding issuer 核对确切 peer/generation，才提供 opaque pre-I/O receipt；Unknown 不提供。该凭据仍须与调用方有限 attempt/current/cancel 检查结合，不能重放已发送业务 payload。

### 候选入站的真实 Ready 确认

配置应用和启动的 listener 成功确认现在等待同一实际实例的 Ready receipt。协议资源 start 和 carrier activation 必须先完成，随后实际 accept/recv loop 首次 poll 到运行态才由宿主该调用点确认；TCP+UDP 和 listener group 必须确认所有实际分支，登记任务本身不是 Ready。就绪前错误、停止和取消不发布候选成功；取消等待保留实际 owner、候选信息与同一 receipt，旧实例的通知不能确认新实例。协议自有就绪预算保持原样，包括 SIP003 TCP 原十秒预算，不新增通用五秒 Ready 超时。

外部停止现在与原 startup/reconcile future 同时受 poll；停止只撤销该准备阶段记录的实际 pre-Ready 候选，并同步拒绝迟到安装，最后成功监听器不加入该候选集合。宿主继续驱动同一个借用事务完成真实清理与补偿，保留原 DNS candidate、任务/Child owner 和关闭收据；停止后最终发布与 ACK 再校验同一阶段，不能把取消等待或 future Drop 当关闭证明。跟踪元数据只弱引用本阶段实际实例，已完成物理关闭或 Ready 的项被回收，阶段结束不保留历史。回归新增真实 pending SIP003 的 startup/reload 停止与 reap，以及未受候选停止影响的真实 last-good TCP listener。

已经接受的配置事务在停止时仍可完成原补偿：只有私有 restore 操作及其同一实际 snapshot 的 ACK 使用独立恢复阶段，继续等待真实 DNS、组件和 listener Ready 后，再调用 application reconciler 恢复最后成功配置。普通候选和直接 Engine reload 不获得此豁免；全局停止和 Business admission 保持关闭。回归保留 `runtime/handle/configuration/tests/owned.rs` 原 application 回滚调用断言，并核对同一原 snapshot 与 Fake-IP 映射恢复。

过渡保守边界：listener owner 在对 Registry 可见之前先标记安装未封口；只有实际任务登记成功或原生预绑定对象已同步留入持久失败槽，才完成该安装阶段。准备阶段关闭后的迟到安装、已安装 owner 的资源准备失败不会再把原 `BoundInbound` 丢在返回栈上。冷态 TCP 叶节点现在把同一原监听器同步移入受控关闭状态，先持久安装唯一 control，随后才释放 accept view；group 逐叶保留实际关闭进度，等待取消和 deadline 不丢原对象、首个原生错误或已取得的关闭收据。真实空 TaskScope 完成仍记零 joined tasks；TCP 显式原生 close 与 Scope 完成都确认后才允许交付该实例的关闭证明。宿主正常阶段取消的原因单独保留，在真实关闭前仍返回同一 Unknown；关闭完整后它属于预期停止，不伪造 TaskFailure。真正准备、任务或原生错误仍持久交给监督；任意 Interrupted 或嵌套任务错误不能冒充宿主私有取消标记。

尚未迁移的 UDP/QUIC 叶节点仍由原槽持有且保持 Unknown，混合 group 的 TCP 关闭不代表这些独立资源已关闭。实际任务登记失败而 submitted future 已被消费的未封口路径仍不能确认关闭。owner 安装前的 adapter prepare、bind reservation 失败，以及普通活动 listener、SIP003 原生监听器交接、QUIC 隐藏 driver 的完整合同仍是独立缺口；后续必须通过同一宿主资源图的 bind lease 接入实际 control，删除原生未确认的永久持有分支，不能据此宣称 M4 完成。回归见 `runtime/inbound_owners/entry/pending_bound/tests.rs` 及 `tests/errors.rs`：真实 production spawn 迟到拒绝、原 TCP 端口关闭及复用、真实 Scope 等待取消、原 Arc source、混合 UDP 保留和监督交付重试。

单个 literal TCP listener 新增薄 `PreparedInboundBind` 桥：适配器投影同一 `ResourceDeclaration::TcpListener`，宿主沿用实际组件分配器和原任务预算，在第一次 listener socket create 前把 native owner 放进 Registry 持久 reservation；成功后仅转移唯一 accept view，promote 沿用同一 owner/ID/watch channel。原 startup 一次及原重试路径的 50 次/10ms 策略显式传给原生 owner，不新增默认额度；只有原生未创建或原 FD 明确关闭的错误可安全重试，Unknown 保留原 source 和占用。部分预绑定 map 丢失、prepare 失败或取消只请求停止，不移除控制权；恢复和全局停止必须先实际关闭并同步交付原 terminal，再允许新配置批次。reservation 只容纳一个实际配置的 tag 集合，配置身份为 Weak，无无限失败历史。全局停止在快照前线性化关闭 reserve/promote，重新打开要求 active/reserved 均已确认并移除。

此桥首切只覆盖声明的外层 TCP 原 FD，不等于所有 native/File grant：适配器既有证书、key/OCSP 文件读取、已接受载体/会话、共享池、Direct combined UDP、SS/SIP003 监听器交接及 QUIC driver 仍须各自迁移。未选择该能力的旧路径仍是明确旧边界；已选择后的 bind/prepare 错误不允许回退旧 binder。桥不新增独立 Manifest、静态资源 ID 或永久执行器，typed inbound factory 接管后删除。回归 `runtime/inbound_owners/bind/tests.rs` 与 `tests/retirement.rs` 覆盖原 FD、实际注册任务 I/O pending 的 join 取消、部分预留、原 source 交付重试及关闭门禁；不据此宣称整个 M4 完成。

wire、配置 schema 及调用方式不变，无 GUI/SDK 迁移步骤；原调用方可能更晚收到成功，实际初始化失败会进入原补偿路径。此确认描述监听循环已进入运行态，不代替已转交会话、原生 FD 或整个 M4 资源关闭证明。回归见 `runtime/inbound_owners/entry/readiness/tests.rs` 的真实 TCP/UDP pending、取消和原 TaskExit source，以及 `runtime/orchestration/tests/inbound_ready.rs` 的实际 SIP003 Child、超过五秒仍 pending 的初始化、真实 ACK 和明确 reap。

### 启动期间的实际配置代次一致性

宿主先订阅实际 reload 通知，再捕获一次 Engine snapshot；本次启动的 DNS candidate 准备/真实资源退休、组件 start/commit、listener 与服务均使用该同一 snapshot 的配置。启动 await 期间的后续公开 Engine reload 留待原 bridge 逐次 reconcile，不能把后来读取的配置提交到先前启动的组件。启动前已完成 Engine reload 也从捕获的实际配置建立 DNS 与组件。wire 与配置 schema 不变；公开 Engine reload / probe 调用方式不需要迁移。回归保留 `automatic_urltest_rebuilds_runtime_across_large_and_small_reloads`，并新增 `runtime/orchestration/tests/startup_snapshot.rs` 的真实 UDP DNS/HTTP 流量与同一 plan Arc 验证。

### HTTP forward 消息的宿主身份

HTTP forward 请求现在向宿主交付目的地与规范化 wire 元数据，不先制造协议 Session。原消息执行器为每条实际消息签发一个真实身份，继续使用既有准入、头/体改写、redirect、upgrade、取消与流量统计路径。旧 Rust `HttpForwardRequest` 访问器和 `into_parts` 保留在显式兼容包装中。

V1 配置/API/events、wire 响应及 FFI ABI 不变，无 GUI/SDK 迁移。回归见 `protocols/http/tests/inbound.rs` 及 `crates/proxy/tests/http/standard_inbound.rs`；实际验证结果独立记录。

### Shadowsocks 普通 TCP 标准请求移交

普通 SS TCP 入站在原协议解析、用户快照认证及同一 replay authority 检查完成后，把真实 Destination、完整 SessionAuth 和原加密流移交宿主标准 Stream 入口，由宿主唯一分配业务身份。plain/stream、旧 AEAD、SIP022 及 SIP023 EIH 共用此路径；原 salt、nonce、首包剩余明文、响应绑定、跨 acceptor clone 的 replay 保留与容量拒绝规则不变。协议不新增成功响应或提前 flush，SIP003 子进程 owner、UDP 及 listener 资源路径不变。

Rust 新增可选 `accept_stream_facts`；原返回 Session 的 `accept_stream`、公开 `ShadowsocksAccept` 字段及显式 response-salt 构造 API 保持兼容。V1 配置/API/event 与 FFI ABI 无变化，GUI/SDK 无迁移步骤。回归位于 `crates/proxy/tests/shadowsocks_inbound.rs` 和 `protocols/shadowsocks/tests/inbound/facts.rs`；本条记录实现，不代表完整 RuntimeHost 或原生资源关闭资格已通过。

### 冷态公开 outbound 诊断的 Service 生命周期

`ProxyHandle::new` / CLI / FFI 的独立 `diagnostics.probe_outbound` 继续支持尚未运行 listener 的宿主。调用入口显式把冷态探测接受到已有有限宿主任务及配置串行锁，安装并启动实际临时 component plan，仅授予 Service preparing 权限；探测完成后确认该 plan 的实际组件任务/声明资源关闭，全程不发布 Business。调用方取消不再遗弃已接受的组件维护，实际探测仍受原有超时预算限制。冷态维护串行，运行中探测继续使用原有并发上限与实际 runtime owner。

wire、结果码、策略/健康中立性与配置字段不变，无 GUI/SDK 迁移步骤。组件关闭不冒称转交给旧 Stream facade 的全部 native FD 已有关闭收据，该后续 I/O 迁移仍独立。回归保留 `crates/proxy/tests/probe_outbound_diagnostics.rs` 原公开行为断言，并补 `runtime/handle/command/diagnostics/maintenance/tests.rs` 的真实 HTTP pending、取消、Business 拒绝及实际组件关闭验证。


### 入站故障的物理关闭与恢复

入站故障不再把已完成的实际任务 join 和原生资源关闭永久视为未确认。宿主保留原实例的任务 ID、类别与原始错误，只有实际物理关闭证明完成、错误同步交付监督及已有 endpoint 健康记录后，才消费该实例收据并允许新一代替换；取消或交付失败保留原 owner 和同一收据供重试。候选替换遇到旧实例故障仍被拒绝，现有补偿路径从最后成功配置重新建立实例。未确认或 Unknown 的资源继续阻止替换。

此项影响配置更新、停止及内部 listener 监督，无配置字段或 API wire 标识变化，无 GUI/SDK 迁移步骤。普通 Direct listener 没有新增 endpoint 状态模型；候选 listener 的 Ready 确认由上述实际监听循环收据独立约束。回归位于 `runtime/inbound_owners/entry/tests/peer_resource/recovery.rs`（真实任务 panic、UDP close、取消、交付失败与重放）及 `runtime/orchestration/tests/inbound_recovery.rs`（真实 listener/UDP 流量与最后成功配置恢复）。

### Native Stream component integration

- HTTP and mixed CONNECT now pass protocol-owned destination facts into the host's standard accepted Stream handoff. The host mints one sealed business identity and retains the existing admission/routing/accounting/half-close pipeline. Protocol-owned replies preserve the established 200/403/502 wire and original carrier failure sources. Rust `http/runtime` gates that reply's Tokio dependencies; default parsing stays runtime independent and the legacy request API remains available. Forward HTTP messages retain the existing executor. V1 config/API/events and FFI ABI are unchanged; regressions live in `protocols/http/tests/component/inbound.rs` and `crates/proxy/tests/http/standard_inbound.rs`. This entry records implementation; validation results are recorded separately.

- Rust `InboundCarrierPlan` now supplies `owner() -> Box<dyn DynResourceLifecycle>` sharing the actual protocol state; the host must retain and start that owner before activation. Dropping activation/completion is no cleanup confirmation. SS inbound plugin activation installs its real Child before readiness can suspend; cancellation requests termination while the separate owner retains the Child for deadline-bound reaping. The report covers the direct child only. Native process grants, socket/task joins and host replacement require their actual host integration; V1 configuration/API and FFI ABI are unchanged.
- UDP peer 资源关闭（开发期宿主接线）：原有每 listener 4096 个 peer 名额现在覆盖准备、活动及退役阶段；首切每个真实 peer 最多登记一个冷态子资源，不增加任务或 socket 默认额度。宿主独立分配资源 ID 和 association ID，在资源 start 首次 await 前持久保存 owner。原 Scope 的真实 TaskExit 与原生资源明确关闭均确认后才回收名额；取消清理等待仍保留同 owner，Unknown/原始故障阻止新增。未支持的动态 grant/路由继续保留原明确边界，本条不宣称所有 UDP/RuntimeHost M4 已迁移。wire/config 无变化，GUI/SDK 无迁移；回归见 `runtime/inbound_owners/entry/tests/peer_resource.rs`。

- Native DNS detour connections now return the actual successful Service chain's carrier binding. It retains typed roles, endpoints, dial/network facts, observable host admission and only that branch's captured selector/URLTest state; it retains no Engine or configuration snapshot. Reuse and revocation checks are read-only and never advance load-balance scheduling. The existing pool driver must consume revocation and explicitly confirm its users/I/O before replacing the connection; the receipt itself does not close transferred I/O. Legacy paths, ambiguous aliases and graph expansions beyond the actual configured graph budget return no reuse binding. Additional resourceful component bootstrap and the full RuntimeHost migration remain pending.
- Direct inbound UDP peers now register in the listener's persistent actual TaskScope using the existing 4096-peer bound plus its outer driver. The host dispatches actual joined receipts individually, so cancellation of an observer or a primary driver panic retains pending peer I/O for explicit shutdown/retry. Successful peer exits reclaim their IDs after supervision; failed IDs/classes/original sources remain bounded and stop admission. This confirms registered task ownership and does not substitute for logical-session control or final datagram-controller I/O retirement.
- DNS reuse proof on configuration apply covers static native prefixes only. Any reachable selector, URLTest or load-balance group is conservatively affected without re-running its selection. Within one configuration, native DoH reuse additionally checks the actual successful carrier binding and observes its revocation; unknown legacy or ambiguous bindings are not reused. Configuration-apply owner comparison alone is not a carrier identity.
- Acknowledged configuration and endpoint transactions now confirm retirement of affected persistent DNS backends before publishing the exact prepared backend map. Reuse checks the complete selected native prefix's actual owners and order, including fallback candidates; unavailable legacy identities conservatively retire the backend. Interrupted/failed retirement retains the actual transport owners, and recovery prepares fresh backends for previously closed pools. An already accepted task retains compensation ownership during host stop while Business admission stays closed. Resourceful component bootstrap through a staged DNS map remains pending; resourceful production factories are still rejected. V1 configuration/API/events and FFI ABI are unchanged.
- Rust `ResourceDeclaration` adds `Process { program, max_starts }`; exhaustive matches must handle it. `ResourceAccess::spawn_process(ProcessRequest)` defaults to denial and returns a native Child synchronously. The actual component slot grants only declared executables and finite per-generation starts during lifecycle start; build, expired or revoked access cannot spawn. Arguments/environment remain protocol-owned and debug-redacted. Platform Tokio explicitly enables native process support. SS exposes a roleless PluginFactory using the original adapter plan, rejects legacy-child adoption and registers its actual monitor; explicit shutdown reaps the direct child. Production resourceful activation, descendant/I/O containment and RuntimeHost replacement remain pending. V1 configuration/API/events and FFI ABI are unchanged.
- Rust hosts constructing `BuildContext` must provide `dependencies: Arc<[ComponentInstanceId]>`, resolved from their validated component graph; use an empty slice for descriptors without dependencies. `ComponentInstanceId` identifies a component and resource generation without claiming a callable role. `BuildContext::instance/dependency` expose typed identities only. The erased build boundary checks declared dependency identities before factory construction; the actual catalog supplies their selected generations. Existing V1 configuration/API/events and FFI ABI remain unchanged; current-generation admission and confirmed resource replacement remain host responsibilities.
- Rust hosts can retain `SharedResourceIdentities` across cold passes within the same allocator and compiled assembly. Unchanged canonical resources keep their actual IDs; removed entries are omitted, so re-adding allocates a new ID. The index contains no previous recipes or factories and does not imply protocol cache or security-history reclamation. Actual component plans propagate dependency-generation changes transitively, increment each affected consumer once, and reuse unchanged owners. Resourceful production activation and confirmed host replacement remain pending.
- Rust registration can request canonical roleless resources through `ResourcePreparation`, using the actual host allocator and the owning factory's typed Options. Shared identities require equal complete descriptions within one assembly namespace; preparation errors invalidate the entire pass. Neutral `ComponentRecipe` uses the same role/identity validation and persistent lifecycle owner as protocol recipes, without invented capability descriptors. Existing production catalog activation and V1/FFI contracts are unchanged; generation propagation and host replacement transactions remain pending.
- Cold component plans reject unknown/duplicate dependency identities and cycles before building resources, then start in dependency order and invoke protocol cleanup in reverse order. These checks do not activate resourceful factories or replace host confirmation of affected sessions, shared I/O and recovery.
- Once a SIP003 outbound Child exists, failed or interrupted readiness is terminal for that cached plan. The Child is installed before readiness can suspend, and cancellation requests termination while retaining its handle. Rust integrations can obtain `ShadowsocksPluginResource` from the actual adapter state and call deadline-bound shutdown; only a reaped child produces `PluginTermination::Exited`. Timeout/interruption retains ownership for retry. Errors before Child creation still return their original cause. Resourceful production activation and RuntimeHost replacement transactions remain pending.
- Rust integrations can prepare a roleless Shadowsocks replay-maintenance component from the existing adapter-scoped salt pool. It declares one registered critical task and retains actual worker ownership across interrupted startup; a replacement cannot claim the pool until the old worker exits. Salt history, the 61-second security window and full-capacity rejection remain unchanged. Production resourceful-factory activation still awaits shared-resource host transactions.
- SIP003 TCP startup probes verify the actual local/peer endpoint pair and recheck child status before reporting Ready. A Linux socket can connect to itself without a listener; that connection no longer confirms plugin readiness. The original ten-second startup budget and SIP003u UDP readiness behavior remain unchanged, and startup failure still returns an error without direct fallback.
- Rust integrations can bind `OutboundHealthObservation` to a selected configuration generation without retaining its snapshot or Engine. Retired observations cannot change current health, while current failures preserve immediate URLTest selection and events. Compiled component factories retain the table assembled with their capability adapters so later protocol resource owners share one actual state.
- Prepared Shadowsocks TCP settings expose `establish_tcp_to` for destination facts without a business Session. The existing Session API and destination API use the same protocol framing, crypto and replay guards; production Shadowsocks shared-resource migration remains pending.
- Rust component factories now declare `ComponentSpec::task_limit: Option<NonZeroUsize>`; `None` grants no internal tasks. Interrupted start retains the installed owner, protocol cleanup and registered driver shutdown progress together, and interrupted task cleanup retains already collected exit IDs and original failures for retry. Direct/SOCKS5 still declare no internal tasks; this does not change public wire configuration or the FFI ABI.
- Ordinary Direct and SOCKS5 business streams now use cold-built component roles while retaining V1 configuration, API/event envelopes and existing FFI ABI. SOCKS5 retains protocol-owned credential validation and framing; existing rejection messages, stages and endpoints are covered by direct comparison tests. Internal DNS/UDP and mixed legacy relay migrations remain in progress.
- Rust runtime integrations must handle the explicit Service request identity and use `Some(existing_deadline)` for an existing absolute OpenContext/CarrierContext deadline. `None` retains native operation budgets; ordinary business connects retain existing DNS and per-candidate socket timers. Resource start/close/accept/handshake deadlines remain explicit.
- Native SOCKS5 fallback proceeds only when no CONNECT request has been submitted on an empty root prefix. Partial submission, an executed prefix or an unconfirmed request stops replay. Local cancellation, revocation and stale local plans remain neutral to remote health.
- Relay chains whose network nodes all expose native Stream roles now compose strictly shorter, endpoint-bound prefixes through those roles. They retain actual business/component identities, original named-node wire targets, per-hop Outer observation and existing relay Flow counters. Mixed chains retain their existing executor pending migration; revoked roles cannot fall back to a legacy claim.
- The Hysteria byte-stream carrier keeps at least two packets of congestion window at small bandwidth-delay products, matching Quinn's full-packet send reservation. QUIC close-only packets bypass the data congestion/pacing gate as required by RFC 9002 section 7.2, so queued application frames cannot strand CONNECTION_CLOSE. Existing authentication, negotiated rates, payload/FIN assertions and deadlines remain unchanged; both failure mechanisms have captured negative regressions.
- Browser Dialer retirement now reports `io::ErrorKind::ConnectionAborted` with the existing message. Permanent retirement must terminate byte I/O; returning `Interrupted` let retrying I/O helpers spin inside one poll. Existing lifecycle, reuse, peer-close and deadline checks remain.
- HTTP/1 XHTTP packet upload ownership retains existing request, byte and queue budgets and deadlines, and yields after bounded admission handoff so the consumer can progress before the next pipelined request. The receipt remains bounded ownership acknowledgment. Captured negative/positive regressions and the separate pinned reference-client byte-count qualification issue are recorded under design/v0.0.3/implementation.

### Host Wintun activation and XHTTP upload receipts

- Existing host-owned Wintun adapters validate their configured addresses and MTU before opening an exclusive packet session, then wait for Preferred address state on the activated link. A dormant Deprecated address is not treated as ready; duplicate, invalid and expired addresses remain errors. Host addresses, routes and MTU are never modified.
- XHTTP HTTP/1 packet uploads acknowledge completely received, validated and byte-budgeted ownership before waiting for ingress queue capacity. This allows a reused upload connection to deliver a missing sequence instead of waiting for a receipt that depends on that sequence. The admission task retains its request permit, payload reservation and deadline; receipt does not mean business delivery. Queue and listener budgets, duplicate rejection, cancellation and HTTP/2/3 admission backpressure remain enforced. No configuration or public API fields change.

### Shared raw-IP listener egress

- Native shared raw-IP listeners apply the current physical-egress selection at the datagram send boundary. Their outer packets no longer rely on being recaptured and routed through TUN as business flows. Explicit outer proxy carriers keep their existing transport ownership.
- Per-message interface hints preserve the listener's source port and receive binding. Route hints are bounded and expire after one second or immediately on egress generation changes; unavailable physical egress and failed interface hints do not fall back to an unbound send. Linux firewall marks accompany the individual datagram. No configuration or API fields change. On macOS, specifically addressed IPv4 listeners use a synchronous, checked temporary interface scope instead of IP_PKTINFO, preserving their configured receive address and restoring the previous interface scope before returning to the executor. Send or binding errors never trigger an unbound retry.

### DoH connection recovery after network changes

- DoH HTTP/2 pool identity now includes the existing physical-egress generation for native and detoured queries. Connections prepared before a network change cannot enter the current pool or send a new DNS request afterwards, even when the physical interface name remains unchanged.
- Failed or cancelled exchanges, including cancellation by the configured backend deadline, retire only their own connection. A delayed cancellation cannot evict a replacement connection. Healthy connection reuse, native platform binding, explicit detours, DNS dispatch, fallback selection and query deadlines retain their existing contracts; this does not introduce automatic query replay or new configuration fields.

### Connector reload recovery and sample delivery

- Tokio 最低版本声明为 1.53.3，Unix TUN 与路由监测使用对应的注册接口，保持描述符直到注销后再关闭；macOS 接收宿主描述符时继续保留其拥有关系。V1 配置、控制面及 TUN 路由语义不变。

- In-process dispatcher replacement retains an instance-qualified replay cursor and its bounded volatile delivery state. Missing retained history is reported through existing replay-gap status. This is not crash recovery without an outbox; receivers still deduplicate at-least-once delivery.
- Newly admitted `flow.updated`, `stats.sampled`, `stats.scopes_sampled`, `stats.host_interfaces_sampled`, and `endpoint.stats_sampled` are best-effort samples and do not enter the outbox. Samples already persisted by older versions retain their delivery/ACK obligation. Lifecycle facts, `stats.reset`, business usage and quota state are preserved.
- The source bridge stops receiving while preceding events cannot be admitted. `api.dispatcher.source_queue_max_bytes` defaults to 8 MiB of serialized event bytes, alongside the existing count limit (`replay_batch_size`). Reservations follow live events until sink admission. One existing oversized event may reserve the whole byte budget and enter alone; one event waiting for admission and a single replay/bootstrap event are additional bounds. This is not a heap/RSS ceiling, nor a byte budget for pending/in-flight delivery copies; those remain separately managed. Replay processes one owned event at a time. No existing durable delivery is deleted to enforce these limits.
- `api.dispatcher.max_in_memory_delivery_bytes` adds a 16 MiB default serialized-byte charge for the combined queued, retry/ACK-retry and in-flight delivery working set (valid 1..=256 MiB). The existing count limit remains independent. Durable entries beyond the budget stay indexed on disk. An older entry waiting for byte room pauses additional live working-set admission so continuous smaller events cannot starve it; volatile facts apply source backpressure. One oversized event is admitted only when the working set is empty. Lowering a reload budget retains inherited volatile facts and stops additional admission until they drain; it never acknowledges or deletes durable obligations.
- `sinks.delivery_buffer` optionally reports the shared dispatcher `byte_budget`, `reserved_bytes` and `over_budget` state, repeated per sink and not summable. Live charges measure event serialization; recovered charges conservatively include the journal frame. Shared fan-out obligations are charged separately. Carrier bodies, decoding scratch, source buffers, one prepared input, journal indexes and other runtime memory are separate: this is not an RSS limit. Oversized events and inherited state can exceed the configured charge. The application status projection retains its existing refresh interval.
- Connector workers and asynchronous sinks share immutable event ownership rather than cloning payloads for each handoff. A differing sink `source_id` still materializes a sink-specific event for compatibility; those copies are charged independently once queued. Only the current prepared input is cached during admission retries, avoiding repeated payload copies; reload invalidates that preparation for the new sink settings. Durable paging caches only the last candidate frame length (invalidated on compaction) to avoid rescanning a large record on every admission poll, checks lengths before decoding and does not retain whole encoded frames. Journal/webhook encoding pre-sizes scratch to avoid geometric Vec growth; JSONL delivery streams through a small I/O buffer instead of materializing a complete frame, and failed writes are not retried by buffer drop. JSON decoding and carrier encoding still require bounded-by-record scratch; this is not a zero-copy or arbitrary-size-event memory guarantee. Journal index metadata uses the disk-backed derived index described below.
- Outbox recovery, inspection, paging and compaction now use an exact disk-backed derived index instead of retaining two full Rust maps of pending identities. JSONL remains the only persistent source of delivery/ACK truth; its format and existing pending obligations are unchanged. Index tables contain identity/offset/count metadata only, not event payloads. SQLite is bundled only with the optional `event-dispatcher` feature; basic metering and querying remain independent of Connector. Each scratch connection targets a 256 KiB page cache with spill enabled and memory mapping disabled. Recovery/index construction commits at most 128 journal operations per batch; paging iterates the offset index without collecting all identities. Compaction temporarily uses two small page caches and streams frames into the replacement journal, preserving latest-offset order, all unconfirmed identities, payloads, attempts and ACKs.
- Derived indexes use private scratch files in the operating system temporary directory, including on Android where SQLite's anonymous temporary databases would otherwise be forced into memory. Deployments should place the OS temporary directory (for example via TMPDIR) on disk-backed storage when memory pressure matters; tmpfs charges remain system/cgroup memory. This trades RAM for disk I/O and temporary disk capacity proportional to pending metadata; it is not an RSS cap, a disk quota, or constant-cost recovery. Caches, the largest individual identity/frame scratch, configured-sink status projections and SQLite internals still consume memory; OS file cache is separate. Normal close removes scratch files; after abrupt termination leftover private scratch directories are disposable under the OS temporary-file cleanup policy, never a recovery source. Read-only preflight requires temporary storage but does not modify the inspected journal. Scratch allocation, full-disk or index-write failures are reported as index failures, never as journal corruption or successful delivery. The optional Rust Connector API adds `ConnectorError::OutboxIndex`; exhaustive matches must handle it, while control-plane envelopes/configuration stay unchanged. After a durable append whose index update fails, paging/status refuse stale offsets and the next persistence/ACK retry rebuilds the index from JSONL. No pending event is dropped to satisfy memory limits, and Engine cumulative meters, business usage and quotas are unaffected.
- Connector records are notification/delivery state, not authoritative cumulative traffic counters. Dropped/evicted samples do not decrement Engine counters, business usage or quotas. Consumers recover cumulative observations by queries, checking core instance and statistics epoch; replay gaps cannot restore per-flow detail absent from retained history. Cross-restart billing durability is not implied by in-memory statistics or a best-effort subscription.
- `sinks` query adds optional `source_buffer` admission gauges: channel capacity, byte budget, current reservations, high-water values, waiting event bytes, oversized admission count and pause state. These describe the shared dispatcher source, are repeated per sink and must not be summed. They exclude replay/handoff payloads and delivery copies, are read independently and reset with dispatcher replacement. `reserved_bytes` is budget charge, not RSS; oversized admission charges the whole budget. Missing fields mean unavailable on older cores or synchronous sinks.
- Single-file ordinary log rotation (`max_files=1`) now replaces the active file at its size boundary. Rotation errors propagate without resetting length accounting. One oversized write/event remains intact and rotates before the following record. Outbox retention is unchanged.
- Engine-generated opaque event IDs include an instance-local identity ordinal, preventing same-millisecond facts from colliding in durable deduplication. Identity allocation is separate from published sequence, so subscription snapshots and prepared completion facts create no replay gaps. Persisted IDs and caller-provided external IDs remain unchanged; consumers must treat IDs as opaque.

### Connector 持久化预检与配置热更新

- `zero connector state`、outbox 启动回放、dead-letter 和配额状态检查改为流式读取。预检只检查打开文件时的快照，不申请持久化写锁、不修复文件，也不改变运行中的内核；日志格式及 API/event V1 标识保持不变。
- outbox 在 PUT、ACK 和启动时检查历史压缩阈值，保留每个未确认事件的最新完整记录、payload 和重试状态。真正未投递的事件不会因压缩被删除；空间不足时保留原日志并延迟压缩。
- 配置热更新只重建实际变化的应用组件。仅修改代理设置时保留 dispatcher、状态监视器和未变化的 hook；需要重建 dispatcher 时取消已有 outbox 副本的在途请求，保留持久化事实供恢复，丢弃临时采样。没有 outbox 的生命周期事实仍须完成投递；接收端继续按既有事件身份幂等处理可能的重投。

### Direct dial 与 IPv6 监听（v0.0.3）

- `OutboundConfig.dial` 新增可选 `address_family`（`auto` / `only_ipv4` / `only_ipv6`）、`interface` 和 `source_ip`。省略与默认值保持旧行为；非默认策略首期仅用于 Direct TCP/UDP。接口与源地址必须存在且归属相符，启动/重载准备与实际拨号均校验；失败不能撤销约束重试。
- `direct_outbound_dial_policy_v1` 表示此能力。原始 PacketSink 与 ICMP 不支持非默认 dial；`auto` 的 TCP/UDP 转为受限 Flow。既有 TCP 保留旧策略，仅策略变更的 Direct UDP 失效重建，期间允许短暂丢包。
- 业务 DNS 的严格族覆盖仅作用于所选 Direct 解析，不改变全局 DNS 上游 socket、Node 或控制面默认值。System resolver 只保证候选过滤与实际拨号族。
- 监听 IP 使用结构化端点；合法裸/括号 IPv6 归一化。`::` 是否接受 IPv4 保留 OS 默认，不保证跨平台 dual-stack，也不保证双 wildcard 同端口共存。
- 进程内 Rust 集成：`OutboundConfig` 字面量添加 `dial: Default::default()`；`ResolvedLeafOutbound::Direct` 添加 `dial_policy` 与 `dial_generation`，只匹配 tag 的模式使用 `..`。wire API/event V1 标识不变。
- 内部边界遵守 [工程约束](../AGENTS.md)，对应测试契约随源码维护。

<!-- Record implemented but unsealed compatibility changes here. -->

- VMess `cipher: zero` 现采用 Xray 标准的 NONE（0x05）、无 ChunkStream body 语义。此前使用 Zero 私有 0x06 格式的节点必须将配置显式改为 `cipher: zero-plus`，并协调两端升级；不保留含糊别名。标准 `zero` 与私有 `zero-plus` 不可互换。

### VMess ordinary TCP host identity

VMess 普通 TCP 入站现在交付已认证的协议目的地与完整认证事实，由宿主签发一个真实业务身份并进入既有准入、路由、统计、限速和半关闭执行器。原 AuthID replay 状态、单次响应头和 AEAD/body codec 不变；UDP/Mux 与旧公开 Rust Session API 保留显式兼容路径。V1 配置、API、events 与 FFI ABI 不变，无 GUI/SDK 迁移；实际 TLS wire、全认证 sidecar、唯一身份、拒绝与 replay 回归位于 `crates/proxy/tests/vmess_inbound.rs`、`protocols/vmess/tests/auth/mod.rs`。此条记录实现，不代替冻结源码测试结果。

- Hosted task owners can defer Scope callbacks across an enclosing synchronous
  host lock with `HostedScope::defer_callbacks`. Actual witness notifications and
  native wake callbacks run after that lock is released; accepted exits retain
  their original capacity charge until deferred notifications finish. The
  catalog's inline driver now owns its external wake target, while retained
  monitors keep only weak relay metadata. These are task-delivery guarantees,
  not native I/O or transferred-user closure evidence.

- `TaskRegistrar::scope_identity` supplies opaque weak `TaskScopeIdentity`
  metadata for exact host/provider scope matching. Its `registration_open` check
  reports only original registration admission, without polling tasks, delivering
  exits, retaining a Scope or claiming resource/user closure.

- `ResourceAccess::register_controlled_carrier` is a default-deny Rust-only
  registration hook for the sole declared physical owner, actual declaration
  ordinal and host TaskRegistrar. HostResources requires the actual component
  generation and scope before supporting it. The exact owner/fence binding does
  not provide business admission or transferred-user completion; missing users
  retain an actual unfinished cleanup obligation. V1 config and FFI are unchanged.

- Internal replacement terminal publication runs outside its state lock. A
  receiver rejection before acceptance permits retry with the same terminal;
  callback panic retains an unknown publication outcome, its original owners
  and fault source, and blocks automatic redelivery. Physical declared closure
  and successful fault handoff remain distinct facts.

- TokioSocket can carry the same unique accepted TCP byte view while the
  listener entry retains the sole native control. Its Rust `into_inner` and
  `into_controlled` conversions, and `ControlledTcpStream::new`, are now fallible:
  a controlled accepted facade returns itself intact instead of extracting its
  original FD or creating another owner. Ordinary native callers explicitly
  unwrap the successful conversion. Metadata, effective addresses, peek and I/O
  observation preserve the same stream; no V1/FFI/default activation changes.

- An installed inbound driver now explicitly retires its same prebound native
  TCP/UDP listener after startup/work returns or panics. The original resource
  owner retains native receipts and cleanup faults before that task exits; the
  original Scope still must consume its real exit and child/user cleanup remains
  separate. Cancelling an orchestration observer requests stop without leaving
  the healthy native listener open indefinitely or claiming a shutdown ACK.

- `TaskRegistrar::downgrade` issues sealed `WeakTaskRegistrar` registration
  authority. It upgrades only during `spawn`/`spawn_tracked`, preserving the
  original scope's admission, allocator, budget and exit receipts, and rejects
  expired owners as closing. Retained task futures can hold this authority
  without retaining their own Scope; weak identity metadata cannot mint it.

- Hosted ancillary UDP acquisition now uses the canonical `MetadataDatagram`
  declaration ordinal, exact literal source, complete explicit metadata limits,
  finite bind-attempt ceiling and the installed instance's actual task-scope
  identity. The host installs the same main/probe native owner before creation
  and retains native controls when granting unique RX, bounded TX and unique
  observation authority. This is separate from plain UDP listener permission.
  Main/probe native-close evidence and original acquisition/close errors survive
  retry independently. Provider callbacks run outside HostState/cleanup locks;
  reentrant access to a busy original returns pending/busy rather than blocking.
  Canonical metadata coverage remains `Unaccounted` until the actual transferred
  users and QUIC driver graph are integrated. No default budget, production
  activation, outbound interface/dial-policy support, or full graph closure is
  implied by this host grant.

- Logical Datagram retirement keeps the original controller in its existing
  bounded entry independently of the borrowed close future. The existing
  waiter remains the only consumer; cancellation and protocol-future polling
  run outside owner/entry metadata locks. I/O receives an internal weak wake
  relay, while the actual drive future owns the caller wake target. Synchronous
  wake, last-Waker Drop and protocol callbacks therefore do not retain a caller
  task through the retirement owner or run under its parent locks. A cancelled
  wait restores the same future; a panicked poll propagates the original panic
  and keeps the controller/entry unconfirmed rather than repolling a poisoned
  protocol future or refunding capacity. Completed controllers are dropped
  outside metadata locks before their retained receipts and admission credit
  are released. This retirement change adds no background cleanup task and does
  not prove accepted-user closure. Listener observer retention is handled below.

### Interrupted listener Scope observation

- Orchestration borrows the registry's single original listener-observer JoinSet.
  Cancellation and cleanup timeout return that same set and its actual join
  handles to the registry instead of aborting the remaining Scope consumers.
  A concurrent orchestration lease is rejected. Recovery drains the interrupted
  generation's results and confirms its retained owners before preparing or
  binding new listeners; its errors never consume the new expected-exit count.
- The original production observer continues consuming its actual Scope after
  the primary driver exits. Critical first faults and unexpected successful
  driver exits can still notify the live supervisor through the same instance's
  bounded existing error disposition while remaining children are joining.
  This adds no task, Scope, task-ID registry, detached handle or cleanup actor.
- The persistent JoinSet stores internal weak-target Wakers. The actual borrowed
  join future owns the external Waker; retarget/cancellation and synchronous
  callbacks occur outside the registry parking lock and Tokio join polling.
- Actual all-task joins, native main/probe/listener closure and transferred-user
  closure remain distinct obligations. Parking or dropping an orchestration,
  an observer result, and Scope completion do not acknowledge native/user I/O
  closure or enable resourceful production factories. Proxy/registry destruction
  still drops its owned handles without manufacturing close confirmation.

### Weak original Scope collaboration

- `WeakTaskRegistrar::scope_identity()` copies only the original weak identity.
  `defer_callbacks()` returns the existing original relay's short parent guard
  after releasing its temporary Scope upgrade; an expired owner returns `None`.
  The guard retains no Scope, caller Waker, registration or I/O authority.
  Create it outside enclosing owner/resource locks and drop it after unlocking.
  Neither method changes admission, task budgets, receipt ACK or closure proof.

- Listener-observer parking additionally retains the current generation's
  `needs_retirement` bit independently of JoinSet length and error consumption.
  Startup marks it before preparation; actual registry install/reservation marks
  it before publishing an owner or native bind capability. Empty Scope/observer
  completion and forwarding a fault cannot clear it. Recovery clears the bit
  only after original handle drain and successful actual owner cleanup/source
  publication, with same-registry/no-pending-owner postconditions serialized
  against owner insertion. This bit is not native or transferred-user proof.

- `WeakTaskRegistrar::cancellation()` temporarily borrows the same original
  registrar and returns its existing one-way child cancellation token, or `None`
  after owner expiry. Actual Scope stop propagates to it; cancelling that child
  cannot cancel the Scope or siblings. It retains no Scope/runtime/task owner and
  grants no registration, native authority, join or closure acknowledgement.

### Controlled carrier native-only settlement (library)

- `CarrierAdoption::retire_native` returns an opaque native-stage settlement from
  the original control only after its physical view returns and registered
  transport/auxiliary tasks actually join. It does not wait for the external
  caller's own join, release its logical user lease, remove the charged slot,
  or issue whole-user/generation completion. Existing retire/shutdown remain full
  confirmation paths; original successful native reports are consumed there once.
- This adds no Root service or Business authority, production activation, schema
  or default budget. The real host issuer and separate user graph remain required.

### Original peer join observation without graph retention

- `HostPeerJoinObserver` shares the existing peer's sole registration/join cell;
  it does not copy a task registry, allocate identities or construct exits. Its
  read-only view retains the original `JoinedPeer`/witness/source after Binding
  removal without retaining Binding, peer group, Registry, runtime, resource
  owner or cancellation authority. Only the original Scope's actual exit
  dispatch can write that cell. This view proves neither response-task joins
  nor native/transferred-user closure.

### Original UDP response Set retirement

- A real peer binding retains at most its dispatcher's one original response
  JoinSet owner, installed before response spawning. Its read-only join observer
  shares only the original join cell and cannot retain that response/resource
  graph. Caller Scope completion does not refund the peer permit while the
  original response lease or actual response joins remain pending.
- The existing inline peer cleanup returns to that same Set's unique consumer,
  persists its opaque task-close receipt, then drives the original child's native
  cleanup. Cancellation retains the Set, actual Tokio task identities and first
  fatal source; it neither constructs component TaskIds nor acknowledges native
  I/O. A confirmed task/native close still delivers the original panic to the
  listener supervisor. Ordinary response body errors and requested cancellation
  retain their local handling. This adds no Scope, task, manager or cleanup actor
  and does not establish the rest of the transferred-user graph or enable any
  resourceful production factory.

## 0.0.1

- 经项目所有者授权清理旧 Release 和 tag，基于当前 main 源码发布统一的首个正式版。以下旧编号条目仅保留兼容性背景，不代表仍可下载的发布记录。
- 配置、控制 API 和事件信封继续使用既有 V1 契约，先前实现的能力保持不变。
- 修复 Windows TUN 路由初始化时继承系统默认 Loopback 标志的问题，以及 macOS 路由锁被继承或复制的文件描述符延长持有的问题。
- 旧版 0.0.4 至 0.0.16 的版本号高于新基线；切换至此版本需手动安装，不会通过递增版本检查自动升级。

## 0.0.16-rc.202609070904

<!-- Record implemented but unsealed compatibility changes here. -->

- 固定目标 `direct` 入站默认同时转发 TCP 和 UDP。升级前只有 TCP 的监听现在也会绑定同端口 UDP；若要保持 TCP-only，应将该入站或全局 `runtime.udp.enabled` 设为 `false`。任意一层禁用 UDP 都只绑定 TCP，热更新会重新协调监听，绑定失败通过既有事务回滚。协议配置仍为 `type`、`target` 和 `port`，不新增 `network` 字段。
- `zero build-info` 新增 `protocol_capabilities:` JSON 行，内容复用既有协议能力记录。控制器应确认当前构建中 `direct` 入站 UDP 能力为受支持后再下发 UDP 转发；旧内核接受 `udp.enabled` 并不意味着支持该转发。未编译所需 UDP 运行时的构建会在配置验证阶段拒绝启用 UDP 的 direct 入站。
- WebSocket 自定义 `Host` 现在替换拨号地址生成的默认 Host，避免发出重复 Host 头；连接地址、TLS 及其他头的配置不变。

## 0.0.16-rc.202609060636

`zero validate CONFIG` 仅校验当前构建支持的配置和资源，不再实例化 Engine / Proxy、获取 Fake-IP 状态锁或读取配额状态。安装器可在旧内核运行时执行该命令。成功不代表运行端口可用、权限满足或持久化数据兼容；后者仍由运行启动和独立状态检查负责。CLI 输出保留 `config valid` 统计，移除校验阶段误报的 `engine started` 日志。

## 0.0.16-rc.202609051609

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.202609050728

<!-- Record implemented but unsealed compatibility changes here. -->

- 空入站且无 TUN 的运行配置不再以 `NoInbounds` 退出，保持管理循环供控制器通过现有 `config.apply` 事务添加监听。首次绑定失败仍保留空配置供重试，移除最后监听后返回待命。
- API 与事件信封版本不变。依赖空配置启动失败的进程内集成应显式校验自身约束；GUI 可在导入代理配置前启动管理内核。

## 0.0.16-rc.202609041712

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.202609041703

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.202609041004

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.3

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.2

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.1

<!-- No compatibility changes in this release. -->

## 0.0.15

<!-- No compatibility changes in this release. -->

## 0.0.15-rc.4

<!-- No compatibility changes in this release. -->

## 0.0.15-rc.3

### Diagnostics 路由追踪使用真实会话语义

`diagnostics.trace_route` 现在构造 TCP 或 UDP 会话并复用代理运行时的路由追踪路径。域名规则没有直接命中且路由需要解析 IP 时，会通过真实 DNS 解析结果再次匹配 IP 规则。响应中的 `effective_mode`、`route_action` 和 `matched_rule` 因此反映实际运行时决策，不再使用简化推断。依赖旧诊断结果的控制端应以新结果为准。

### Connector backlog 改为有界工作集

Connector 不再让慢速或失联 Sink 导致进程内待投递队列无界增长：

- 配置 outbox 时，事实事件先写入持久化 journal，内存只保留有界工作集，其余记录按空位从磁盘分页恢复；
- 未配置 outbox 时，事实事件通过停止继续消费事件源施加背压，不会静默丢弃；
- `flow.updated` 与 `stats.sampled` 是可丢弃采样，只做 best-effort 投递、不会写入 outbox；工作集已满时优先替换同一 Sink 的旧采样，不驱逐事实事件；
- `api.dispatcher.max_in_memory_deliveries = 0` 表示 outbox-only 模式，因此必须同时配置 `api.outbox_path`；
- `max_retry_attempts` 默认值由 `3` 调整为 `10`，显式配置不受影响。

事件 Sink 不应把吞吐采样当作可靠事实流；需要恢复保证的消费者应使用事实事件并配置 outbox。

## 0.0.15-rc.2

### 撤销开发期固定中心 API

项目尚未发布 Connector 合同，因此开发期的节点注册、同步、traffic、presence、访问配置和私有命令设计直接撤销，不保留兼容层。

已移除顶层 `push`、`PushConfig`、`/api/v1/nodes/{node_id}/*`、中心 OpenAPI、conformance 和 production gate。外部控制器通过 Zero API/gRPC 管理节点，并使用 `config.apply` 注册通用 `api.event_sinks`。Connector 只向完整 Webhook URL 推送 `zero.event.v1`，并定义 HTTP 状态确认分类。

### 公开 Cargo feature 改用 kebab-case

构建入口不再暴露下划线式能力名。构建脚本、CI 和制品 feature 校验需要完成以下迁移：

| 旧名称 | 新名称 |
| --- | --- |
| `status_api` | `status-api` |
| `event_dispatcher` | `event-dispatcher` |
| `sink_jsonl` | `sink-jsonl` |
| `panel_connector` | `connector` |
| `grpc_api` | `grpc-api` |

Rust 函数、模块和变量仍按语言规范使用 `snake_case`；本次变化只影响 Cargo feature 名称和二进制对外报告的 feature 字符串。历史候选证据保留其原始名称，不得重写后冒充新候选。

### 引擎生成事件使用跨启动唯一 ID

旧事件 ID 仅由事件类型、进程内 flow ID/序号和毫秒时间戳组成。进程快速重启后这些值可能复用，使 Connector 接收端或 Sink 把新事实误判为已处理事件。

新语义：

- 每个 `EngineEventLog` 创建时生成一个 128 位随机 epoch；
- 所有引擎内部生成的事件 ID 均以前述 epoch 限定，在同一进程内重放时保持不变；
- 通过进程内 `emit()` 注入、由调用者拥有 ID 的外部事件保持原 ID；
- `event_id` 的内部拼接形式不是公共契约，消费者只能比较完整字符串并用于幂等去重。

该变化不修改 `zero.event.v1` 的 JSON 字段形状，但修正了跨进程启动的唯一性语义。任何依赖旧 `{type}:{flow_id}:{timestamp}` 格式解析的消费者必须删除该解析逻辑，改用 `event_type`、`payload.record.flow_id` 和 `occurred_at_unix_ms` 等正式字段。

### 用户速率改为 Zero 主体策略聚合

开发态预资格期间，`up_bps` / `down_bps` 曾被描述并执行为单条 TCP/UDP flow 的限制。该语义允许同一主体通过增加并发连接绕过带宽策略，不满足 Zero 主体策略的定义。

新语义：

- 同一 `principal_key`、`policy_revision` 和双向速率组成一个 Zero 主体策略身份；
- 该身份下的并发 TCP/UDP 会话共享上传、下载 GCRA 时间线；
- revision 或速率变化建立新时间线，旧会话在确认式清退前继续持有旧策略；
- 没有 `principal_key` 的入站默认限速仍按会话独立执行。

JSON 字段形状和当时的 `zero.panel.v1` schema ID 不变。该修正发生在首个清洁 release candidate 和正式生产签字之前；历史开发态 manifest 只能证明当时的每流实现，不得继续作为当前候选产物证据。接收端无需修改 wire payload，但容量规划和限速验收必须改为并发 TCP/UDP 聚合测试。

## 0.0.15-rc.1

### `EventSource` 统一为实时订阅

旧语义存在两个不同实现：

- `Engine::subscribe()` 返回一次性的 `Vec<RawApiEvent>` 历史快照；
- `EngineHandle::subscribe()` 返回实时 `EventSubscriber`。

新语义：

- 所有 `EventSource::subscribe()` 都返回实现 `EventStream` 的实时订阅；
- `latest(limit, filter)` 只用于读取近期历史；
- `since(sequence, limit, filter)` 用于按事件序号恢复，返回 `requested_after`、`actual_from` 和 `has_gap`；
- `has_gap = true` 时，消费者不得直接继续套用增量，必须先通过快照或 Query 重建状态；
- 包含 flow 生命周期的实时订阅仍可在增量前发送合成的 `flow.snapshot`。

进程内 Rust 实现者需要：

1. 将 `type Stream = Vec<RawApiEvent>` 替换为实现 `EventStream` 的实时流；
2. 实现阻塞 `recv()` 和非阻塞 `try_recv()`；
3. 实现新的 `EventSource::since()` 游标恢复方法；
4. 不再把 `subscribe()` 当作历史查询使用。

### EventDispatcher 投递时序

EventDispatcher 从周期性事件环扫描改为持有一个实时订阅：

- dispatcher 不再反复把历史快照当作新事件扫描；实时订阅仍按配置的轮询间隔排空并投递到 Sink；
- `flow.snapshot` 仍只用于实时客户端同步，不投递到 JSONL/Webhook；
- Webhook、重试、死信和 Sink 过滤语义保持不变；
- 外部 Sink 应继续使用 `event_id` 去重，并按 `source_id + sequence` 检测缺口。

### 对外 GUI 影响

IPC、HTTP SSE 和 gRPC 的 wire 格式保持 `zero.api.v1` / `zero.event.v1`，现有 GUI 不需要因本次待发布变更修改帧解析。GUI 仍需遵守 `0.0.15-rc` 建立的快照与增量合并规则。

## `0.0.15-rc`

### Flow 订阅改为“基线 + 增量”

包含任一 flow 生命周期事件的 IPC/SSE/CLI 实时订阅，在订阅确认后先收到 `flow.snapshot`：

1. 使用 `payload.records` **替换**当前活动连接集合；
2. 记录快照 `watermark`；
3. 按 `flow_id + revision` 合并后续 `flow.started`、`flow.routed`、`flow.updated`；
4. 收到 `flow.completed` 后从活动集合移除，并由 GUI 自行保存需要展示的历史；
5. 不把 `recent_flows` 当作断线重建或长期历史数据库。

`flow.snapshot` 是同步基线，不进入事件环，也不会投递到 JSONL/Webhook。`flow.completed.payload.record` 是自包含完成事实，新客户端应优先解析 `record`，同时容忍旧内核没有该字段。

### GUI 兼容分支建议

| 内核版本 | GUI 行为 |
|----------|----------|
| `< 0.0.15-rc` | 使用 `active_flows` 查询作为活动连接基线，并兼容旧 flow payload |
| `>= 0.0.15-rc` | 等待 subscribe ACK 和 `flow.snapshot`，之后按 revision 合并增量 |

## 新增条目的要求

后续每个破坏性或语义性变更必须在发布前补充：

- 首个受影响版本；
- 影响的通道和消费者；
- 旧语义与新语义；
- wire 标识是否变化；
- 兼容窗口和可检测条件；
- GUI/SDK/面板的明确迁移步骤；
- 对应回归测试位置。

开发期间只在版本矩阵和 `## Unreleased` 下登记，不预判最终发布版本，也不写入 Cargo 的 `-dev.N` 构建号。完整测试通过后，由 `Prepare Release` 工作流或 `scripts/release.sh` 将矩阵行、章节标题和 workspace 版本一起封板；禁止手工分别修改这些位置。

Startup, supervision, reload and transaction-drain futures separate their heap-owned phases to fit the native Windows main stack. A one-MiB-thread regression reaches actual Ready and confirms shutdown; no larger process stack is required by the execution path.


第二轮云端续作新增内部 Rust `ProbeCall`/`RuntimeHost::run_probe`、Direct Service 原件票据及
同 supervisor 的已取消原 carrier 清理轮询。嵌入使用者应驱动实际 host 生命周期，不能把
Service facts、body 完成或 Drop 当成原件关闭。System TUN ordinary Source、可选实际 owner
预算和不可变授予 CA 的 TLS backend 接线正在组合；省略字段导出、V1 配置/API/events、CLI、
FFI 和客户端面板工作流保持原契约，最终资格尚未取得。TEMP-WG-OUTER-UDP-01 未关闭，
没有扩大其 outer_udp_proxy/reconnect 边界。未发布或部署。

- Round2 Rust integration: Scope panic sources now retain the exact `Arc<tokio::task::JoinError>` through `TaskJoinError`; its `Error::source` exposes the original join error. Internal consumers that directly downcast the immediate source must follow this wrapper. This does not change V1 command/config/event or wire fields. Command cleanup, caller ACK and byte custody are separate; supplied control-book owners retire the shared command Scope after their actual frontends join.


### Engine event subscriber custody and limits

The existing Engine subscriber registry now owns one original filter, exact logical
registration, physical queue workspace and retained queued-event reservations.
Dropping a receiver removes that registration without requiring another published
event. Taking a retained event out of its queue does not return its pending-count
or statistics-byte allowance; `into_shared` transfers the same immutable original
after its next owner has prepaid custody. This is logical event custody, never an
I/O close or TaskScope completion receipt.

Six optional runtime fields preserve omitted export shape:

| Field | Default | Range | Unit |
| --- | ---: | ---: | --- |
| `event_subscriber_max_pending_events` | 1024 | 1–1024 | queued/dequeued retained events per original subscriber |
| `event_subscriber_total_max_pending_events` | 65536 | 1–1048576 | retained queued events across that Engine |
| `event_subscriber_max_registrations` | 4096 | 1–65536 | active/pending/retiring original subscriber books |
| `event_subscriber_total_stats_max_bytes` | 64 MiB | 1–256 MiB | existing serialized statistics payload allowance, overall |
| `event_subscriber_max_filter_bytes` | 64 KiB | 1–1 MiB | actual original filter Vec/String capacities |
| `event_subscriber_total_max_storage_bytes` | 8 MiB | 1–256 MiB | queue arrays, filter/book/registry storage and genuine owned initial envelope/workspace |

The existing per-subscriber statistics default remains 1 MiB. The std channel
retains its original 1024 physical slots, allocated only when that actual
subscriber is created; logical pending limits can grow on the same queue.
Registration/queue maxima are independent admission dimensions, not upfront
allocation counts. New overall defaults are kernel policy, not upstream defaults
or RSS limits. Fixed opaque std synchronization metadata is bounded by actual
registration count; the byte ledger measures its declared payload/storage envelope.

A genuine owned initial snapshot keeps its independent same-Engine storage
reservation through retained delivery and does not consume an old queued-event
slot. Production HTTP/IPC/gRPC and Connector continue their existing direct live
subscription/borrowed initial encoding paths. Budget refusal returns the existing
API error envelope/status, never a false subscription acknowledgement or fallback
subscription. `EngineHandle::subscribe_control_live` now returns `ApiResult`;
embedded Rust callers must handle admission failure. Existing EventSource wire
shapes remain unchanged.

Cold construction, pure candidate validation and final configuration publication
consume these same books. Shrink is Busy while actual retained originals exceed
the target; growth preserves the original channel/filter/event identities. Dead
weak registration/vector allocation remains charged until actually destroyed or
compacted. No live event, statistics history or filter is evicted to fit a limit.


### Resourceful command diagnostics execute their original asynchronous work

Acknowledged DNS lookup, route trace and fixed-target probe commands now await
their original boxed asynchronous producer directly. The already asynchronous
outbound-probe command uses that same path. They no longer occupy a blocking
parent thread while waiting for a child lookup/material operation in the same
blocking pool. Existing synchronous CommandService entry points still use the
caller's existing Tokio runtime; no second runtime, pool or resolver is created.
Fixed-target probing remains TCP connect-only with its original two-second limit,
endpoint selection and egress. Command and response wire shapes are unchanged.

CURRENT frontend work is strongly installed in its existing Host command row
before the first await. The original caller and existing receipt monitor retain
that exact work and cleanup continuation; no frontend execution task is added.
NoCURRENT uses the existing finite command Scope's actual Auxiliary work and
Critical cleanup registrations, retaining its one PendingCommand permission
through actual joins and registration ACK. It does not create a blocking Tokio
outcome for an asynchronous task. Original query/native/User receipts remain
separate from command/task completion.

A frontend's precise observing producer is handed off only after its actual
registration, command custody and work continuation are installed. A separately
captured producer remains a wait gate; merely holding a public observation or
counter cannot issue that handoff. This removes complete->wait_joined waiting on
its own outer reply observer. An async continuation panic retains its real opaque
payload in AsyncCommandPanic and the existing finite row's typed fault receiver;
it is not a fabricated blocking JoinError or TaskFailure. Unknown cleanup remains
owned and never authorizes refund/reopen. Known physical retirement permits
recovery while the returned original fault is still strongly retained.


Endpoint list/get/detail 生产查询改用同原配置、Source 写观测及实际 WG/流量/会话
原件的同步借阅，原响应账本先计行工作区再物化；HTTP/IPC/gRPC 既有 wire、
分页、权限、错误及详情 schema 不变。Rust 新增具体 Engine/Proxy endpoint
借阅视图与固定公共 peer-id 编码助手；不改既有结构字面量或配置省略导出形状。
原 QueryService 的 genuine owned 调用仍有库职责，生产路径不回退深复制。
借阅不授 I/O/任务/关闭权，所有 guard 在 await/退款前销毁；具体说明见
[原件借阅续作](../design/v0.0.3/compatibility.md#endpoint-control-queries-borrow-their-actual-owners-before-materialization)。
本片仍需实际统一编测，不据接口或历史局部通过宣告完整 DNS/控制域关闭。


### Shared QUIC buffer accounting continuation

Two optional runtime fields retain the omitted export shape:
`quic_max_queue_packets` defaults to256 (range1..256) per actual existing
FIFO; `quic_total_max_buffered_bytes` defaults to64MiB (range1..1GiB) across
the same Host carrier root. These are on-demand admission ceilings, not
upfront allocations, FD totals, per-identity limits or RSS guarantees.

The root issues one pure buffer book through actual ResourceAccess. A retained
backing/codec/copy/receive workspace keeps its original credit through pending,
failed and retiring custody. Transfer splits the existing credit; it does not
create a second book. Refund and policy-growth wakeups run outside both the
byte-book and original-root locks. Each actual writer retains its own inline
waiter and waits for a complete requested amount. Oversized requests fail
without waiting; a cancelled waiter has no byte credit to refund.

The pure handle remains usable for original cleanup after business cancellation,
without granting IO, task, native-close, logical-user or Scope authority. Limit
facts participate in cold recipe identity/reuse and the existing StopOld
transaction; a live original cannot silently change its queue policy. Held pure
output stays charged across an otherwise confirmed policy installation.

This common producer/configuration precondition is not completion of QUIC,
transport or budget migration: actual native/borrowed/Mask FIFO consumers and
codec peak projections still require B's complete paired implementation and
original wire/Scope regressions before domain qualification.


GET /api/v1/events now encodes the same original Engine history under its
existing short synchronous guard. It measures the legacy envelope and prepays
the actual response Vec in the same Host control book before allocation, then
releases history before native output can await. No event String/payload or
selection Vec is cloned first. Existing history/event identity, order and wire
shape remain unchanged; the genuine owned Engine.events_snapshot API retains
its library role. The superseded private HTTP DTO branch and body variant are
removed. New same-root refusal/recovery/Pending-output regression still needs
the exact frozen-input run before qualification.


The same carrier book now also issues a concrete three-size working reservation
for an existing QUIC Entry: actual receive scratch, one transmit peak and one
receive peak. Bounds come from the owning native/borrowed/Mask producer; the
helper neither parses private configuration nor truncates native GSO to a
single borrowed datagram. It stores only prepaid credits, not another frame
pool or IO resource. Actual backing must disappear before a working credit
returns to its same lane. Return wakes that lane even when the root has no
unreserved bytes; additional queued frames still acquire the same root credit.
Its fixed bookkeeping count belongs to the actual Entry admission, not a claim
that payload bytes measure opaque synchronization metadata or total RSS. This
common precondition and its new regressions still await paired production
consumer activation and qualification; no gate is enabled by the helper.


The optional runtime `component_total_max_tasks` (default 4096, range 1–65536)
limits the same Host's actual component and inbound Scope registrations across
instances. Pending concrete launches, running tasks and native-joined exits
awaiting the original supervisor acknowledgement all remain charged. Existing
per-Scope limits still apply. A cold inbound prepays its actual Main launch
before native bind; its original Scope Start fence still controls execution.
Only a positively never-spawned launch can roll back without a task receipt.
Dropping a Scope, aborting a body or observing a join does not refund a hosted
registration. This counts registrations, not OS threads, opaque metadata or RSS;
control worker and process-job books retain their separate units. Omission keeps
the existing serialized configuration shape. Policy changes enter the existing
source/component stop-before-start barrier; source-specific worker projections
and the paired Packet implementation still require activation and qualification.


Optional `packet_windows_ring_capacity` retains the original 64MiB Wintun
StartSession request. The existing SDK requires a power of two from 128KiB
through 64MiB. `packet_windows_ring_total_max_bytes` defaults to 128MiB for the
same Host and accepts 128KiB through 1GiB independently of one request. A total
smaller than one request is a valid policy: the actual provider must refuse
admission before StartSession. These units describe requested capacity, not SDK
mapping bytes, opaque allocations or RSS. Omission preserves the old export
shape. Windows device recipes include both cold facts; request changes must
retire the original session, real reader and original registration before a new
session starts. Shared credits include pending/failed/retiring sessions until
matching EndSession and actual reader join/ACK. Borrowed views reuse their
session's existing request. The actual SDK consumer now prepays from that same
Root before CreateEvent/StartSession, retains unconfirmed requests, and refunds
only after its own session settlement and reader registration ACK. Ring-only
changes affect actual Windows SDK device dependencies; plain WG UDP and other
platforms do not inherit an SDK resource identity. Windows GNU typechecking is
not actual privileged SDK operation qualification.

Optional `packet_driver_metadata_total_max_allocations` (default 16384, range
1–1048576) and `packet_driver_metadata_total_max_bytes` (default 16MiB, range
1–256MiB) describe the same Host's actual prepaid Packet-driver metadata. They
are distinct from frame, native-read, transmit and requested-ring units. Vector
growth pays old and new real capacity; surviving weak headers remain charged
until deallocation. All five lanes validate/install atomically, so a failed
shrink changes none. Omitted fields remain omitted on export. The SDK/root
consumers are installed; remaining Source/actor/helper consumers still require
complete implementation and qualification, not merely these configuration keys.

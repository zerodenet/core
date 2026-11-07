# 外部与嵌入兼容边界

必要的公开 Rust 接口迁移不放宽 GUI、面板的 V1 行为和现有 FFI ABI。历史消费者观察未包含未提交工作区内容，真实组合验证尚待 PR01/PR10。

[方案入口](README.md) · [架构](architecture.md) · [契约](contracts.md) · [迁移与任务卡](migration.md) · [兼容](compatibility.md) · [验证](validation.md) · [来源](references.md)

<!-- source-section: 16 -->
# 客户端面板与嵌入兼容边界

目标是保持现有客户端与面板的接入方式。内部 Rust 接口可以迁移，但外部兼容必须以实际消费者验证，不能预先宣称零影响。

| 表面 | 保持的契约 | 允许的内部调整 |
| --- | --- | --- |
| V1 配置 | 字段、默认值、协议名称、路径与校验语义 | 内部 Options、构建结果与资源归属 |
| CLI | run、validate、reload、status、build-info 和已使用参数、退出语义 | 命令调用新宿主 |
| IPC HTTP gRPC | 方法、信封、错误码、认证、权限与响应语义 | 服务实现及 DTO 投影位置 |
| 能力发现 | api_id、schema_id、build_features 与协议能力的含义 | 内部能力来源和缓存 |
| 事件与 Webhook | zero.event.v1、身份、顺序边界、ACK、重放与投递义务 | 内部事件源和 dispatcher 连接方式 |
| TUN | 开关参数、就绪确认、父进程退出、宿主资源拥有关系与恢复 | 设备实现与管理位置 |
| Rust 公共 API | 下游依赖和迁移路径清楚；不承诺全部源码兼容 | 允许必要调整公开 trait、构造函数和配置结构，提供接口迁移说明 |
| FFI | 导出符号、调用约定、句柄、字符串释放与当前行为 | 句柄内部实现；不隐式扩大功能 |

## 客户端 GUI

当前 GUI 通过 `src-tauri/src/kernel/zero` 的 IPC 适配器与独立内核进程交互，解析事件与配置；它还依赖固定 revision 的 zero-rule。内部 proxy trait 改动不直接要求 GUI 适配，但 zero-rule 公共 API 改动属于实际源码兼容面。

必须保留启动参数、控制 socket、父进程生命期、管理待命、TUN 启停与网络恢复。stats reset、订阅 ACK、flow.snapshot 与后续增量的边界必须和客户端解析规则一致。内核更新不得以旧版本解析成功代替真实组合测试。

## ZBoard 面板

当前发布链路读取 build-info，校验配置，执行 reload，再检查 core_instance_id 与 revision，记录配置发布结果；失败不应偷偷变成 systemctl restart。

配置 renderer 和事件接收器仍按既有 V1 配置及 zero.event.v1 工作。重复发布不重启健康内核，无变化配置不破坏现有连接。reload 成功不能早于已承诺的应用与协调完成，失败恢复不伪装成成功。

保留流量方向、单位、累计值、重置和结算语义，以及 source_id、event_id、核心实例身份和幂等处理。面板凭证和节点模型留在面板，不进入组件标准。

## Rust 和 FFI

允许必要调整 Proxy、ProxyHandle、RunningProxy 等公开 Rust 接口，不以长期兼容包装阻碍标准收敛。保留有职责的入口，逐项记录公开 trait、构造函数、配置结构及 feature 迁移和实际下游编译影响；必要过渡 facade 设定删除边界。此决定不放宽 GUI、面板的 V1 wire 行为及现有 FFI ABI 兼容要求。

FFI 目前直接使用 EngineHandle。其旧功能边界与错误处理需要先建立测试，不能借宿主统一让 zero_start 自动承担从前没有的网络副作用。

## 兼容版本规则

当前标识为 zero.api.v1、zero.event.v1，配置与控制数值版本为 1。内部重构不自动改变这些标识；发生不兼容变更时也不能为了保留标识而隐瞒变化。

新增可选字段需验证旧消费者忽略行为；修改 ACK、快照、重放、计量或恢复语义即使 JSON 形状相同，仍属于需要登记与评估的外部变更。相关现行规则见 [兼容性账本](../../release/breaking-changes.md#L39)。
<!-- /source-section: 16 -->

## TaskScope build 阶段登记迁移

公共 Rust `SpawnError` 新增 `NotStarted`：宿主以 `TaskScope::suspended(runtime, declared_limit)` 创建实际组件作用域，工厂 build 只取得受限 `TaskRegistrar`；真实所有者安装后，宿主在 lifecycle start 前通过不可克隆的 Scope owner 调用 `enable_registration()`。保留的 registrar 无法开放登记，也无法重新开放已关闭的 Scope。依赖此枚举作穷尽匹配的 Rust 调用方须新增该分支；SS PluginFactory 与 ReplayFactory 将其映射到 `InvalidConfig` 并保留原始来源。

`TaskScope::new` 与 `empty` 的直接登记语义保持，`None` 任务预算在开放后仍为零。此变化不增加 V1 配置字段、IPC 方法或 FFI 符号。新增回归覆盖 build future 未被轮询、实际 start 的 TaskId/退出确认，以及关闭后无法重开。该批尚待父任务的集成编译与资格检查；不代表完整 RuntimeHost 或 M4 用户/I/O 关闭确认。

Datagram 关闭确认持有者由 Proxy 构造并在所有执行服务中共享同一 `Arc`；DNS 回调仅持 `Weak`。本阶段独立宿主策略 `RETIRED_DATAGRAM_LIMIT = 4096` 限制原生 active 与 retiring 控制器总数，它不是旧 UDP packet queue 的容量或既有 session 预算，不驱逐未确认关闭的会话。实际新 open 必须先预留该持有者的容量，旧调用方不能保留失败 Control 时不接入生产。运行时在消费者结束后、组件退役前调用真实 `wait_closed`，超时与失败保留未完成控制器，不伪装为关闭成功。该批接线尚待关联实现的集成验证。

入站 listener 的实际 driver 与 carrier 生命周期登记在 Proxy 构造的持久 `InboundOwnerRegistry`，不放在可取消的 run/activate future 内；角色为空的实际 carrier 使用同一组件分配器的真实 `ComponentInstanceId`。作用域预算由实际外层 driver 一个任务加已准备操作声明的内部 driver 数量组成，TCP+datagram 为两个；group 递归登记，不增加无上限重启。原监听 JoinSet 仅观察真实 Scope，退出 ID 与原始错误先写回持久 owner。停止等待不再使用 watch 接收端消失作为确认，候选退役未确认时保留 owner 并拒绝替代。

该批确认范围是实际 listener/UDP driver task 的退出及协议 owner 报告的直接 Child 回收；它不证明旧 UDP dispatch 的所有逻辑 association、DoH 等连接池、进程后代或所有 transferred I/O 已关闭。旧 inbound process spawn 仍未统一到新的 native ProcessRequest grant，不以同状态生命周期 handle 冒充授权迁移完成。新增真实 socket pending/observer 取消/TaskId 回归尚待父任务集成运行。

`PacketRouteClose` 的接受与迟到 receipt 由 Proxy 持久 Scope 持有；预算直接取引擎现有 live packet route 上限 4096（新增只读 Rust `Engine::packet_route_capacity()`），不另造并发默认值。Scope 容量检查先于实际 close claim。原 5 秒 ACK 超时不等于 Closed，同一任务释放配置锁后继续等待实际 route lease 与 response pump 两项确认，再记录一次原事件。调用方取消、关闭等待超时或轮询被取消不会丢控制器/TaskId/错误来源。停止后禁止新 close，真实 join 在 TUN、入站与 outbound-device owner 停止之后；它不占配置串行锁等待可能使 receipt 完成的 TunStop。Scope 惰性绑定现有 Tokio Handle，Proxy 冷构造不依赖运行时。新增回归尚待集成运行，未扩大 V1 方法或响应成功条件。

TCP+peer UDP 的首轮结构化清理保留原动态 peer 上限 4096：TCP/UDP 任一报告错误先停止输入，清除实际 peer 输入 sender 后继续驱动每一个真实 JoinHandle 到退出，再返回错误；不在首个错误时丢弃 sibling future/JoinSet，也不以 abort 发出成功确认。成功 peer 的实际 JoinSet 结果正常消费回收，不向固定 listener entry 永久累积历史 ID；首个意外 peer panic 停止该 listener，剩余失败的真实原生 JoinError/ID/来源在有限 drain 中保留。该批尚不声称所有 peer 均已迁入公共 TaskScope，也不扩大为 mux、逻辑 Datagram 会话或全部 M4 的关闭确认。新增实际两个 pending peer socket、TCP 错误与原生 peer panic 来源回归待集成运行。


## 本轮云端 Config / Engine 与 Hysteria2 出站迁移

Config 的 `compile` / `rule_sets` 资源运行准备迁到 `zero-runtime::configuration`；进程内 Rust 调用方使用 `PreparedConfiguration` / `PreparedRouting`，Engine 保留既有构造、领域快照、策略和 revision 入口。Engine-only 与 FFI 不因此启动 Proxy。`ConfigApplyReconciler` 新增的生命周期默认实现拒绝无法证明的候选退休；自定义应用服务应提供真实退休和提交确认，不能依赖默认成功。

Hysteria2 的根出站和既有 first-Datagram 路径通过实际标准角色消费一个协议池，root 私有 TCP/UDP 及无调用者的 SessionTcpHandshake/SessionTcpConnectOperation 桥删除。协议库既有公开 standalone API 保留原有范围，不能作为 root 生产回退或完整宿主资格。通用 QUIC 包队列保留原始 ECN、源/目的地址、GSO/GRO 与 PMTU 能力事实。

V1 配置/API/events、CLI、FFI ABI 和协议上游版本未调整。原始错误、投递信封、最终 ACK 与源文件恢复边界保持；GUI/面板真实组合和各平台资格仍须用最终候选本地验证，不能用编译或历史结果代替。此记录不关闭 TEMP-WG-OUTER-UDP-01 或其他原方案职责域。

Rust `MessageTransfer::Upgraded.upstream` 改为 `Box<TcpRelayStream>`，降低常见 Complete 值的枚举体积；构造方显式装箱，消费方继续使用同一个实际流，原始 I/O 所有权和 V1 wire 不变。TCP source 的 `next` 实现改用等价 async fn；有限原件计数使用溢出检查 CAS，兼容旧 Rust 并避免 1.99 的原方法弃用。

配置通知直接使用 runtime 提供的具体通知值，Engine 保留原同步 `subscribe_reload` 语义并提供同一领域通知视图；删除旧 blocking 线程桥，没有新增任务、Host 或 provider。DNS 仅在完整配置、实际 compiled dispatch 和原 backend 均相同时保留同一代次、cache 和 lease；真实依赖变化仍换代。Root HY2 将真实 CA 字节纳入身份，无变化公开 reload 保留活跃连接。恢复精确已发布 recipe 时创建新代次和取消域，旧配方 crypto 可达，不重新读取仍损坏的路径。

Config 的 Rust 便利文档加载 `RuntimeConfig::load_from_path` 删除；调用方改用 runtime 的 `ConfigurationSource::load_document`，或保留同一源 Arc 经 `load_initial_configuration`、`PreparedConfiguration::prepare_startup` 交付 Engine。Config 的生产依赖不反向依赖 runtime。已准备校验可使用 `zero_proxy::validate_prepared_configuration`，既有 `validate_config` 保留。CLI validate/connector-state/reload 使用原始文件真实关闭后的文档，V1 和 FFI 没有新增字段或符号。

启动 URL 规则下载移除隐藏 native 所有权的 ureq 调用链。现有应用原始 TCP/TaskScope 实现也服务启动下载，有限流式队列、同原件关闭后重定向和不可变缓存保留既有能力；不是新 provider、第二 RuntimeHost 或兼容池。缓存原件、HTTP/TLS 任务和转交用户分别确认后才交付启动配置。

## 第二轮云端 Rust 接口与可选预算迁移

本轮仍未完成所有职责域，精确候选与实际资格见 [round2 执行记录](cloud/round2/execution.md)。
没有修改消费者、配置/API/event 数值版本或既有 FFI ABI；GUI/面板同产物资格尚须实际执行。

- `UpstreamOpener` 增加默认拒绝的 controlled acquisition、observed selection 与 pending cleanup 入口；
  `UpstreamTransport` 提供同实际 opener 的弱 pending observer。显式 observer hint 不授权 I/O；
  原实现的必需方法和旧 pool 所有权保留，缺少实际账本的 caller 不因此具备即时取消资格。
- `CarrierAdoption::retained_loan` / `retained_driver` 返回 sealed 原件保留观察，
  分别证明同 receiver 的实际 LoanOwner 与同原 host/pool Scope driver，不能当作 Closed。
  `retained_driver` 还要求原 physical user 已实际转交 driver custody；仅存在 H2 driver
  不足以丢弃尚由 caller 持有的 native 债务。`CarrierGrant::cancellation()` 是同原 slot
  的 stop token，不赋予新 grant 或关闭凭证。
- `ComponentCatalog` 增加 accepted device cold pass、明确 deferred start、exact candidate discard
  和保留原 prepared protocol material 的 device recovery。普通 `prepare` 仍是读者复用入口。
- `ConfigurationSource` 增加同源 initial control material / retained quota storage；
  Engine 的显式 quota shutdown 等原 worker 与 lock 原件归还。Engine-only/FFI 不启动 Proxy。
- `zero-tun` 增加原 acquisition / channel custody / native receipts，Windows Wintun 增加
  actual Adapter/Session consuming close receipts。公开 convenience helpers 不被当作生产第二 owner。
- `PacketQueue` 支持显式有限 frame/control/byte limits、实际 `poll_ready` 和同 END 保留位等待；
  背压不丢已接受 frame，等待取消归还原未提交许可，不增加队列或 detached 上传任务。
- Rust config struct literals 需适配新增可选 DNS/Mieru/HY2/control/native-stream budget 字段与
  Direct Packet `write_queue`，以及 `vmess_grpc_max_originals`（所有用途共享同一 N）。Serde 省略默认字段的 V1 形状保持，边界/耗尽与资源身份变化
  通过同 host 事务；其计量口径不等于全进程 RSS/FD 或 1c1g 压力资格。

- `ResourceDeclaration` 新增 `ControlledPacketDevice`；穷尽匹配者须处理该变体。
  `ResourceAccess::open_packet_device` 默认拒绝，具体 provider 在 acquisition 前保留同
  原 owner 和实际 Scope witness，只转交唯一 `GrantedPacketDevice.io`。identity 元数据
  不可铸造 native close ACK；尚未接普通 Direct Packet/System TUN source。
- `PacketTx` 增加默认拒绝的 owned-send 方法和 `send_owned` / `OwnedSendFailure`。
  实现者在 Pending 前接管同原 IpPacket，NotSubmitted 归还原 buffer/association；Unknown
  不可重试。`OwnedSendFailure.failure` 使用 Box，结构体构造者须相应装箱。
  `ConnectObserver::packet_io` 有默认实现，只报告实际 PacketIoFact，不授权 I/O。
- `ResourcePreparation::prepare_component` 与 plan 的 Packet getter 仍用同一实际 allocator、
  cold union 和 roleful identity，不能另建宿主或把 roleless 资源当 Packet source。
- `UpstreamOpener`/`UpstreamTransport::has_tracked_business_caller` 是默认 false 的只读观察，
  仅为原 user custody 选择提供实际 caller 线索，不能授权 I/O 或关闭；native grant 仍检查
  同一个 sealed caller。真实 VMess Business 使用普通 user view，Service/旧无 caller 保原 transferred 职责。
- RFC6761 localhost（含子域/大小写归一化）保持纯本地解析，System/None 和配置 Node 均不
  为该答案索取 native TaskScope；仍检查 local lifecycle/generation/族策略，停后拒绝。
  非 localhost 的受控准入规则不变。
- quota inspection/startup 保留同 Source/lease 的 Buffered Read，`read_quota_snapshot`/
  `QuotaStorage::read_with` 在实际原文件关闭后才交解码结果，不把整个文件装入 Vec。
- `ResourcePreparation::require_fresh_resource` 仅标记本 cold pass 已请求的真实组件，
  保留 canonical ID 并要求新 generation；不代表实际 StopOld、native close 或 I/O 授权。
  单次原件配方重新准备后不能按相同 spec 换回旧 slot；复用需匹配原 Scope 和依赖代次。
- 统计 sampler 由同一个 orchestration inline drive 和单 blocking Scope 收尾；停止先等
  实际 join，之后才关闭 Engine。endpoint Ready/persist/final commit 保留同原候选，
  DNS token 最终收尾与真实设备 incarnation facts 刷新均在原串行事务内。
- VMess gRPC/Mieru 的尚未迁跨执行方式热切换，在修改源文件、停旧与启动新原件前
  校验同 adapter 的真实旧债务；未具备正面关闭证明的切换会拒绝并保留原 revision/TCP。
  这项限制是未完成迁移的行为边界，不代表既有组合已整体迁移或可以永久关 gate 结题。

Packet/provider、部分 transport/process、设备 Scope、Windows pipe 和剩余预算的实现缺口
单列在执行记录；没有因为新增上述接口而关闭整轮或 TEMP-WG-OUTER-UDP-01。

## 第二轮续作的关联 Datagram 与实际设备原件

`ResourceDeclaration::ControlledAssociationPool` 是同一实际 carrier owner 的 TCP 控制与
协议协商 Datagram 原件声明；穷尽匹配调用者须处理新增变体，两个原件消费同一总额度。
`UpstreamOpener::resolve_related_datagram` 默认拒绝，实际 host 实现使用同 Node DNS；
`CarrierGrant::link_related_datagram` 只绑定真实已取得的同 owner 控制原件，不授予 root I/O。
`CarrierOrigin::related_control` 是只读关联责任，物理 parent/native_origin 仍是实际 UDP 来源。
`DatagramLoan::into_session` 与 `BorrowedDatagram::into_datagram_loan` 转交原 Tx/Rx、
唯一 view 与同 LoanOwner，不新起协议泵，不把 view 返回当原件关闭。

`PacketDeviceBinding` 增加 `Create` 与 `ExistingWindows`，原 descriptor 借入模式保留。
具体 provider 在配置前安装原拥有者，Windows existing 不在失败时创建替代 adapter。
`IpPacket::payload_mut` 原位修改同 buffer，调用方保持已校验 family/长度和 ingress/return 元数据。
这些是续作实现前置；全部生产调用者、source、TaskRegistrar 与旧链删除仍按剩余表完成，
不以此声明架构、GUI/面板、平台或压力资格通过；V1 wire 与 FFI ABI 不变。


## 第二轮续作的内部 Rust 原件接口

新增 `RuntimeHost::run_probe` / `ProbeCall` 接续实际 diagnostic/URLTest 调用，
在原 Direct Scope 登记真实任务，并持有原全局额度至实际任务和原件清理完成。
`CarrierServiceGrant` 的 Direct 用法必须由同 host 的私有 fence、原注册和真实调用任务签发；
公开 Service facts、observer、裸 Direct 描述不产生 I/O 权限。
`CarrierOwnerRegistration::poll_cancelled_retirements` 只轮询已取消的原 slot，
不创建任务、Scope、协议池或关闭凭据。嵌入调用者仍须驱动原 host supervision/关闭流程。

System TUN 使用同 inbound registry 的 Source、实际原 Registrar 与原设备/route/monitor，
新增可选 worker/Packet return/诊断/gRPC/SS 安全历史预算保持省略字段导出形状。
普通 UDP 构建包含 neutral raw IP source，不因此启用 WireGuard 协议。
TLS `OwnedPreparedClient` 接收实际授予的不可变 CA 材料，并沿用原 backend、安全策略及缓存隔离。
这些内部接口的新增不构成 GUI/面板、特权设备或跨平台的组合资格；V1/FFI/events/CLI 验证仍以
最终精确候选为准，未修改消费者。


第二轮续作的 Linux `ResourceAccess::prepare_owned_process` 返回由原 Host 保留的 cold
`OwnedProcess`；实际已登记 actor 执行启动/原生树清理，外层在该真实 actor join 后显式关闭
原始 fd 并取得 matching `ProcessTreeClosed`。这属于内核 Rust 资源接口；V1 配置/API/events、
CLI/FFI 和客户端面板不变。原 `spawn_process` 暂仅未迁调用者使用，直接 child reap 仍不能
取得 Process 声明覆盖。生产 SIP 消费、总体预算与适用跨平台实现尚未完成，不能把这些
接口或 Linux 底层回归视为全域迁移资格。

续作将该原件接口扩展到 Windows suspended root / JobObject，并增加同一个 job 的
`ProcessEndpoint` 预留、原始 readiness probe 和真实依赖消费授权。网络用户的真实
Carrier 原件、Process 原件和各自 Scope 保持独立；不能用逻辑远端地址冒充实际 loopback
peer 的原生 ancestry。Windows GNU 源码检查不代表真实 Job/NamedPipe 运行资格，macOS
进程树责任和生产 SIP 消费仍有未完成实现。

可选 `runtime.process_max_jobs`（默认 64）与 `process_max_jobs_per_instance`（默认 1）
限制同一内核中待启、失败及退休中的实际 job；每实例额度不超过总体额度。这不限制
单个 job 内合法后代数量。退款等待真实原件关闭、实际 actor join 和 Host ACK。
省略仍不导出这些字段；穷举构造 `RuntimeOptionsConfig` 的 Rust 调用者应添加 `None`
或沿用 `..Default::default()`。

Packet 的内核 Rust 契约补充 `PacketRequest::return_mtu`、实际
`GrantedPacketDevice::native_read`、`PacketStorage::allocation_bytes/derive_zeroed` 和
原声明序号绑定的 `PacketDatagramReads`。标准装配使用同一个实际预算总账；缓冲移动、
分片和重组不产生额外池或原生关闭证据。自定义 storage 的默认 derivation 明确拒绝
未预付分配；内核生产读取必须提供真实原件 grant，不能以库默认分配补齐缺失责任。
H2 客户端继续使用原 `connect_h2*` API，原连接由当前调用者推进，保留半关闭和真实
流控；未新增第二执行宿主或公开替代协议 SPI。上述接口不改变 V1 JSON/API/events、
CLI/FFI 或消费者，但全部生产路径和最终统一候选尚未取得完整资格。


## 原 Packet source 与按需进程的 Rust 接线续作

`ResourceAccess::open_packet_device` 返回原异步获取 future，调用者须在同实际
Scope/driver 与原启动期限内 await；`GrantedPacketSourceDriver` 同时转交原 origin
和唯一 checkpoint。暂停只限制新借用，并由实际 main/wire（Direct 为原单 driver）
确认，不撤销已转交 I/O，也不提供 native close、任务 join 或预算退款凭据。
`ResourcePreparation::prepare_component_recipe` 将既有完整 typed recipe 及其原 native
binding 交给同一个冷分配器，保留失败后整次准备失效的规则。

`ProcessEndpointStreamRequest` / `ProcessEndpointDatagramRequest` 的 `startup_deadline`
由原协议启动责任给出，再受实际调用期限约束；同 Slot 保留唯一 initializer，首借用者
取消后由现有 supervisor 推进。它不另建进程、池或清理任务。只读
`shadowsocks::component::PluginRootExit::status()` 保留原退出状态的类型信息。

新增可选 `runtime.ss_udp_max_pending_responses` 和
`runtime.ss_udp_total_max_pending_responses` 均默认 4096、范围 1..65536、省略不导出；
分别限制同实际 Flow 与同 adapter 账本的响应票据，pending、失败退休及收到未取走均计额。
该 schema 的普通生产消费仍在同一轮实现，字段存在不代表该职责域取得资格。
V1 信封、控制 ACK/revision、事件与计量、CLI/FFI 及客户端/面板工作流沿用现契约。

## 同原件故障交接与控制 Scope 续作

`ChannelError` 的可选 `CleanupFailureReceipt` 是同一控制器有限错误记录的交接元数据。
生产者先一次绑定同一稳定 payload，接收方先强留实际返回的错误 Arc，再显式 ACK；
晚到次错误继续进入同一 payload。未绑定或 foreign receipt 不能 ACK。转发错误须保留
该结构化字段，诊断 `Error::source()` 不授予交接权限。回执不证明 native close、view
归还、业务用户结束、TaskScope join 或 Host registration ACK，也不授予预算退款。

`TaskRegistrar::retirement_observation()` 与实际 issued `WeakTaskRegistrar` 的同名观察
方法只读同一个 Scope 的封闭、实际 join 与 registration 归还状态；不重建任务权限，
不创建/取消任务，不消费退出或清理原件。Scope identity 仍只有身份匹配职责。

CLI connector-state 使用 `inspect_principal_quota_state_with_source`，接续加载文档的真实
ConfigurationSource；独立库检查仍有显式 standalone 入口，均经过同原件关闭屏障。
状态采样器在 ApplicationServices 中先安装冷 owner，再注册唯一 Critical TaskScope
任务；正常配置替换转移监督到下一实际 owner，取消关闭等待不丢失原 Scope。
终止故障继续保留类型和来源，原任务、文件和用户的实际退休单独判断。

上述接线仍在整域迁移与必要回归中，不代表五域结题、统一候选 CI、跨平台或同产物
GUI/面板资格已通过。配置/API/events 与 ACK/revision/计量标识继续保持，未改消费者。


## 第二轮完整调用者续作的当前 Rust 边界

本节登记当前实际生产调用者及 Rust 接线，运行资格按 round2/verification.md 的精确
输入分别判断。五域仍在收尾，不将历史局部限制、公开接口出现或单项通过当作最终资格。

- System TUN 使用同一 Catalog 的 `prepare_resource_start` / `PreparedComponentStart`。
  该 token 只在原 StopOld 与同 Root 额度安装后返回；`refresh_device_resources` 在真实
  TUN Source 改变网络代次后，接续同一份已接受 cold 材料，拒绝替换已构造候选。
  `prepare_restarting_device_resources` 先描述完整受影响集合，活跃 Business 仍为 Busy，
  旧 TCP 不因此撤销。取消/Drop token 只释放串行锁，实际 Replacement、原件与清理责任
  仍留 Catalog；不是原件关闭凭据。生产调用者是 Proxy 的 TUN 初启、换代及恢复。
- `prepared_candidate` 的同配置 device cold 候选优先于旧 current；只读存在不表示 Ready。
  Source 真正 BuildNew 后，其既有协议恢复 capsule 重新选择实际 linked binding，不能
  复活 first-cold Pending 图。原 Main/Wire 两阶段 checkpoint 仍与 native/Scope 关闭分证据。
- `CarrierGrant::adopt_service_stream_loan` 的生产调用者是既有 per-connection Node Service
  前缀。仅接受其真实 ControlledServiceStream 声明、完整 endpoint/dial/binding、无业务
  Session 与存活 Scope；先安装同一 strong LoanOwner 再校验。普通空前缀 native 规则不改。
  direct-dependency Stream loan 还须实际 Catalog Slot、同 host fence 与原 grant 的 seal，
  不能用角色 ID、裸 Stream 或测试登记替代。无第二协议池或资源缺失 fallback。
- 独立 connector-state inspect 消费加载配置的原 ConfigurationSource 及现有
  DispatcherResources；`ConfigurationSnapshotReader` 只借原捕获长度内的 Read/Seek，
  不授予 clone、写入或原生 handle。实际 File/SQLite/scratch 关闭与原失败分别保留。
  Engine-only/FFI 不因此启动 Proxy。quota 最终 std worker 的错误在真实 join 后回到同一
  Source；已接历史故障与仍未知的 native/worker 清理分开，不凭 Drop 放行下一原件。
- 新增 `runtime.event_log_stats_max_bytes` 与 `event_subscriber_stats_max_bytes`，默认
  分别 8 MiB 和 1 MiB，范围 1..=256 MiB，省略不导出。前者是同 Engine 统计历史的
  序列化字节，后者是每个实际订阅未取走的统计字节，独立于控制出口预算。缩额低于
  真实 held 用量返回 Busy，保留当前配置、revision 与事件；新订阅接续同一实际限制。
  既有统计采样溢额/历史前缀缺口语义、控制 ACK 与流量结算保持。

普通 Direct Packet 的实际 provider/路由/returns 及 System TUN Source 已在生产装配
接线；完整 TX/SDK/所有预算投影和换代回归仍继续，不能再用“尚未开启 gate”概括当前
实现。特权 TUN、真实 Windows/macOS、GUI/面板同产物及 1c1g 资格仍须适用环境验证。
TEMP-WG-OUTER-UDP-01 仅保原 outer_udp_proxy/reconnect 历史范围，仍单独未关闭。


## 同原件 Packet 内存与 Source 生产预算续作

本轮进程内 Rust 调用者须保留 `PacketBuffer` 原存储所有者，不能把生产缓冲转回裸
`Vec` 再补记额度。`zero_stack::packet_output::ObservedPacket.packet` 改为
`PacketBuffer`；显式库用 Vec sender 仍有原职责，但不能充当资源缺失时的生产后备。
`PacketStorage` 增加拒绝型默认的同步派生/预付和唯一 Vec 转交；SDK 转交实际分配时
继续持有原 custody。内存归还不证明 native、用户或 Scope 关闭。

内部 `PacketRequest` 新增 `return_memory_layout`；实际 Source 必须给出其原 codec 的
正向事实，`None` 不推断 Native。`PacketDatagramReads` 增加可选
`control_workspace`，由同 Host、同 Scope 在原 native 尝试前预付；WG 控制使用原
148 字节工作区。协议只能拿到同 read0 的 allocation view，不拿 Root policy setter。
TUN/source/返回/翻译回包在原 FIFO 前预付完整有限责任，拒绝保留原输入。

新增 `metadata_max_probe_sockets`、`metadata_max_probe_attempts`、
`metadata_max_send_calls`、`quic_source_max_connections`、
`quic_shared_endpoint_max_users`、`inbound_max_tasks_per_carrier`、
`inbound_max_resources_per_carrier`、`inbound_max_in_flight_requests` 均为可选
runtime 配置。实际协议/查询保留各自省略默认；资源显式0保留原 NoResource 语义。
配置变化仍由同宿主 StopOld、真实原件和任务回执后安装，新限额拒绝新增而不驱逐活跃
业务或有效安全历史。字段省略不改变既有导出形状，V1 API/events/ACK/revision、
CLI/FFI 与客户端面板协议保持。

这些是开发中源码接续，尚非完整职责域或统一候选资格。TEMP 软件原件转交、适用
helper、剩余调用者/旧执行链删除和必要回归继续，唯一 TEMP 范围不扩大。

## 同一控制原件的 Pipe 工作区额度续作

可选 `runtime.control_pipe_max_workspace_bytes` 默认2MiB，范围1..=256MiB，
省略仍不导出。Windows Pipe 的原 Entry 在首次 native open 前向同 ControlBudget
预付该 provider 的完整 payload 工作区（原 pending read/write 各一份，加各自两个
队列位置，当前合计24576字节）；这不是OS pipe buffer/RSS测量，也不借事件字节额度。
确认原始 handle关闭、std worker及Scope真实join、原连接任务归还和Host ACK后才释放
原连接/工作区账额。未知清理始终保留原件；已关闭provider显式丢弃原payload Vec，
诊断owner clone不能让已退额度的工作区继续驻留。任务body/EOF/Drop不充当关闭凭据。

进程内Rust `ControlBudgetLimits` 与 `ControlBudgetUsage` 新增
`pipe_workspace_bytes`，`ControlCharge` 新增 `PipeWorkspaceBytes(usize)`；
自定义literal按实际root提供额度，普通生产装配由既定runtime配置投影。
V1配置/API/events、CLI/FFI、ACK/revision/订阅和计量语义保持。
代码与真实Windows回归已补，当前尚待该输入编译和真实平台验证，不算G8或整轮关闭。


## 同一 DatagramLoan 的实际 wire 与逻辑 association 续作

进程内 Rust `LoanDatagramIo` 区分当前接收者的逻辑 association 与原 Tx/Rx 实际绑定的
wire association。初次构造二者相同；真实多层转交继承原 wire 绑定，当前调用者保留其
自己的逻辑 ID。`association()` 和 `wire_association()` 只读，不授予原件、任务或关闭权限。
`send` 在构造原有 owned Datagram 前填实际 wire ID，`receive` 按该 ID 筛选；不克隆独占
PacketBuffer，也不另建队列。原 native 对错误 association 的拒绝仍保留。

实际 `into_session` 消费者应在转移前读取 `io.wire_association()`，用在其本来就拥有的
编码 frame 上，收到原 wire 后再返回当前逻辑 ID。现有 SS UDP 借入 codec 已配对适配；
SOCKS native 初次转交的两个 ID 本来相同。没有第二协议 SPI、resolver、pool 或资源后备。

Rust `ResourceDeclaration` 新增 `ControlledServiceDatagram`，穷举该内部契约的集成代码
需处理新变体；它仅消费声明 endpoint/dial 的真实非空 prefix DatagramLoan，无 root
native 或 cached-driver 权限。`BorrowedPacketSegment` 接既有 Packet 队列及同 Factory
Scope 的实际单 Aux，原 LoanOwner 先存原 slot；失败或取消继续持有，真正 join/Host ACK
及原控制归还后才返原队列。目标生产调用者是既有 per-connection QUIC 出站，完整 producer
仍在续作，不能以公开类型或局部回归替代普通装配资格。

V1 配置/API/events、CLI、FFI、客户端及面板无需改动。实际编译、失败和运行资格逐输入见
[第二轮验证记录](cloud/round2/verification.md)，五域仍未完整结题。

## 控制原请求与序列化输出预算续作

新增可选 `runtime.control_*` 消息预算保持字段省略时不导出；V1信封、方法、事件、ACK和
revision不变。根账分别限制在途请求/响应字节及命令、gRPC请求数；实际原Connection的
IPC命令/事件、SSE帧、gRPC stream使用同一局部issuer，克隆issuer不分裂额度。
`control_sse_max_event_frames` 默认32，范围1..4096；IPC/SSE物理队列按需分配至schema
ceiling，默认真实held额度不变，另受原根字节上限约束。pending写仍持原frame额度，
缩额检查真实held/retiring原件；队列或Tonic交接不是native写入/关闭凭据。
当前仍在完成命令最终ACK、gRPC编码存储与Engine DTO首次物化前的实际计额；新增字段
或消费者接线不代表控制和预算职责域已完成。无需修改客户端或面板。

本次 gRPC 内部接线复用原 Hyper H2 stream 的 AcceptedTasks/TaskJoinWitness；先保存同原件
再执行IO，真实join/registration ACK后才移除Root请求行。默认请求解码上限仍4MiB、物理
H2并发32及send buffer32KiB不变；可选已验证上限由原body长度观察器在Tonic首次增长前
执行。Pinned Tonic 0.12.3的8192/32768是其原codec几何，不是新增生产额度或扩大缓冲。
请求/JSON/Prost/保留的encoder workspace和实际Wire Bytes分别预付；这是字节存储上界，
不宣称测量allocator/RSS。Bytes克隆/切片持有同原计额，交接不声称实际写入。
同Root的reserve_encoded_response只原子接纳该次输出的三个存储责任，失败不产生部分
退款或自唤醒；没有新调度器、任务或池。已受理命令future在断开后由原H2任务inline续驱动，
不另开清理任务；真正Apply原操作超时后的join观察和Engine DTO物化前计额仍待完整接入，
不能把此局部接线当作控制/G8或预算职责完成。

## 既有DatagramCodec固定工作区及MUX原件前置

现有DatagramCodec增加纯encode/decode_buffer_lengths与encode/decode_buffered、
DatagramCodecOutput和DatagramCodecBufferError；固定slice承担所有实际中间和最终字节，
容量先验后才nonce/replay/RNG。SS全现有cipher及HY2/Mieru实现同一原codec方法，不引入
替代SPI、resolver、池或资源调度器。旧Vec方法仅保显式库职责；Kernel仍等完整flat路径
多层预付消费端迁完再切换，不用默认Unreserved拒绝旧合法内建配置。移动Domain Vec
必须由原Target/workspace持同原额度到真实用途结束。当前仍在开发，不是预算域资格。

PoolFactory.build_with_registration返回同一个cold ComponentInstance和其原CarrierOwner
登记观察，供VLESS/VMess真实start在可失败的材料准备前安装；它不Ready、claim或启动IO。
原start仍检同原owner/Scope/ordinal，重复只允许同一实际登记；错误/空原件的真实关闭、
驱动及Host ACK另验。V1配置/API/事件/消费者保持不变。

## 同一 Engine 配置视图与 gRPC 状态存储续作

内部 Rust 新增 `EngineRuntimeSnapshot::control_config_query`：Config/Policies/Policy
同步借阅同一个真实快照和 group guards，先测量、再取得原响应额度、再编码；释放 guard
之后才允许等待 I/O/容量或退款通知。HTTP 保持原 flat body，IPC/gRPC 保持既有
QueryResponse 信封、错误消息、身份、revision 和 ACK 语义。公开 Engine-only 查询仍有
原有进程内职责，不因此创建 Proxy、第二快照池或资源宿主。

普通应用 gRPC 使用 `GrpcHandle::start_with_engine`，传入其 ProxyHandle 的同一
EngineHandle；原 `start` 保留通用库服务职责。原始响应 Bytes/header 克隆持有同一
ControlBudget credit；原请求实际 join 与 wire 所有权分别计额。该接线的局部定向结果
不表示完整控制 DTO、H2/QUIC workspace 或 Z003-07 已完成。

`PacketSegment::retire_final` 用于真实 endpoint users/drivers 退出后的最终 FIFO 封闭与
排空；UDP hop 的 `retire` 保留同队列。Scope 取消先停止业务，原 Aux 保留已接受 native
send，超时不授关闭/替换或退款凭据。四项真实 Packet 定向已验，普通 H3 尚待新输入复验。

### 续作控制借阅与取消（开发中）

Config/Policy、Runtime/Flows/Stats、PrincipalFlows、Sinks 原表借阅视图保持既有 V1 wire，
不启动额外 Proxy。gRPC 持久错误头保留实际字节计额，已结束 RPC 并发额度独立归还。
控制连接 stop 传递同次 Apply/Probe 调用：接受前取消，接受后保留原补偿与 Scope ACK；
coalesced Probe 的其他调用者保持原业务，最后调用者取消同一原件。其余 DTO、事件与
Target/DNS/Trace 资源责任继续实施；上述接口存在不表示完整职责域关闭。

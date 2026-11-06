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

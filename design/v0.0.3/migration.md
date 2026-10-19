# 迁移映射与云端 PR 任务卡

以下保留旧接口去留、逐接口逐协议映射和串行评审边界。[独立任务卡](task-cards.md)补齐执行字段；本次仅执行 PR00。

[方案入口](README.md) · [架构](architecture.md) · [契约](contracts.md) · [迁移与任务卡](migration.md) · [兼容](compatibility.md) · [验证](validation.md) · [来源](references.md)

<!-- source-section: 15 -->
# 现有接口保留合并与移除

下表定义目标取舍。移除必须在替代路径验证并完成调用迁移后执行；兼容桥允许短期存在，不成为新的稳定契约。

| 现有对象 | 目标处理 | 保留的真实职责 |
| --- | --- | --- |
| ProtocolMetadata | 保留并与实例能力校验 | 基线、成熟度、限制 |
| ProtocolSupportCapability | 拆分 | 类型登记归工厂，验证归协议，元数据归描述符 |
| 空 TcpOutboundCapability 与 UdpFlowCapability | 删除 | 用真实 Stream 和 Datagram 接口替代 |
| 空 UdpPacketPathCapability | 删除 | 所需 carrier 行为由真实数据报能力提供 |
| OutboundLeafClaim 与 ClaimedOutboundLeaf | 合并进已构建的角色句柄 | 目标身份与实际能力绑定 |
| ClaimedTcpOutboundLeaf 等 | 合并进标准出站能力 | 准备时验证配置，调用时建立通道 |
| ProxyTransportLeaf 与 PreparedTransportLeaf | 移除重复包装 | 端点信息与实际协议对象直接关联 |
| ProxyTransportTcpLeaf 与 SocketTcpHandshake 等 | 迁移为公共静态契约 | 协议连接与包装由 owning protocol 实现 |
| PreparedTcpConnectOperation 与 PreparedTcpRelayOperation | 从协议边界移除 | 中立的执行、计量、错误映射保留在执行器 |
| ProxyTransportUdpLeaf 与 ProxyRelayTwoStreamTransportLeaf | 合并为 Datagram 能力 | 双流、重连和复用封装在协议实例 |
| ManagedUdpFlowResume 的 Any 分发 | 删除 | 逻辑会话和连续性语义保留 |
| ManagedUdpHandlerProvider 与 UpstreamUdpHandlerProvider | 移除协议注册用途 | 会话对象直接收发；公共设施仍可复用 |
| ManagedStreamConnectorParts 与 FlowBuild | 合并删除同义转换 | 必要的私有缓存身份留在协议内部 |
| InboundListenerCapability | 重构为构建与接入契约 | 监听预留、就绪与真实接入行为 |
| PreparedInboundListenerOperation | 不再作为协议标准 | 中立监听辅助实现可继续存在 |
| InboundClientResponse 与 Control 及 fallback | 保留并补行为约束 | 协议响应与特殊接入的真实语义 |
| 各种 Inbound multiplexer | 保留不同底层形态 | 并发接入及标准业务交付 |
| ClaimedPacketLeaf | 迁移为 Packet 能力和属性 | 原生回程、MTU、转换条件和交接语义 |
| OutboundDeviceLifecycleCapability 与 PreparedOutboundDeviceState | 合并为可选资源生命周期 | 共享设备事务、退出确认 |
| Endpoint observation 与 control | 保留为独立可选角色 | 资源状态和管理，不强制所有协议实现 |
| ProtocolRegistry | 缩小 | 类型、工厂、重复检查 |
| ProtocolInventory | 作为内部兼容门面逐步收敛 | 运行期查找，不暴露具体协议 |
| TcpExecutionServices 与 UdpRuntimeServices | 留在内部并缩小 | 内核执行上下文，不进入协议 SDK |
| UpstreamConnectServices | 抽取中立接口 | 受约束的载体打开与解析 |
| ConfigApplyReconciler | 迁移为宿主事务参与者 | 参与准备、应用、补偿，统一最终确认 |

TcpTunnelProtocol、TcpSessionProtocol、DatagramCodec 等现有低层工具可继续使用，但不强制每个协议经过全部工具。Managed 缓存和队列设施也按需复用，不把内部 manager 类别暴露为协议接入规范。

当前 AGENTS.md 包含空能力接口、prepared operation 及具体文件路径的约束。实施对应阶段时同步修订它和 crate README；外部行为变化写入 release/breaking-changes.md。本文本身不修改这些规则。
<!-- /source-section: 15 -->

<!-- source-section: 17 -->
# 现有代码分阶段迁移

## 版本与分支边界

实施安排已确定：在 GitHub 仓库 zerodenet/core 的 develop 上完成 v0.0.3 架构重构，通过短期工作分支提交 PR 集成，不再保留版本与分支待选项。

| 项目 | 已确定安排 |
| --- | --- |
| 产品版本 | v0.0.3，开发期间沿用现有 0.0.3-dev 时间戳规则 |
| 集成分支与 PR base | develop |
| 实施方式 | 云端从最新 develop 建立 codex/ 短期分支，每个评审单元通过 PR 合入 |
| 当前核对基线 | GitHub develop 为 f0c1cc0930df8c34f1822cdd182928511e319b76；每个实际任务重新登记 base SHA |
| v0.0.2 边界 | 正式发布仍由既有发布流程单独收敛，不与本重构混为同一任务 |
| 外部契约 | V1 配置、zero.api.v1 和 zero.event.v1 的兼容要求保持 |

M0 至 M6 是 v0.0.3 内部的迁移阶段；PR00 至 PR11 及其 A/B 子单元是具体评审边界，均以 develop 为集成目标。先完成 PR00 将方案保存到版本控制，再按前置依赖实施。产品开发版本不代替配置、控制 API 或事件契约版本。

## 迁移阶段

每阶段保持可编译、可运行、可单独评审与可回退，默认不改变配置和外部行为。阶段顺序按边界依赖安排；云端默认串行处理一个 PR 单元，前置 PR 合入后再基于最新 develop 继续。

| 阶段 | 变更范围 | 完成条件 |
| --- | --- | --- |
| M0 行为与边界基线 | 列出公共 API、配置样本、客户端与面板调用、资源和性能基线 | 可重复的兼容与行为场景；冻结本轮范围 |
| M1 中立契约 | 在 traits 和 core 补齐角色、通道、错误、网络服务与生命周期标准 | 协议标准不引用 proxy 私有类型；旧路径仍工作 |
| M2 实例与出站 | 准备时构建角色句柄，普通 Stream 与 Datagram 逐实现迁移 | 去掉该路径的重复 claim 包装；直连和链路行为等价 |
| M3 UDP 与接入 | 用可执行会话替代 Any resume 和 handler 注册，接入返回标准请求 | 多目标、共享连接、MUX、连续性、关闭与背压均保持 |
| M4 宿主与配置事务 | 提取 zero-runtime，迁移 engine 持久化与 proxy/application 事务协调 | 全入口只有一个最终应用确认；失败恢复和幂等发布通过 |
| M5 服务与 Packet 收敛 | 迁移 DNS Network 注入、规则准备、Endpoint 共享资源与转换入口 | 依赖无环，拨号约束、回程、网络代际和系统恢复正确 |
| M6 移除过渡层 | 删除无人调用的旧接口，清理 feature 和文档规则 | 兼容矩阵与 workspace 检查通过，公开迁移清单完整 |

M2 和 M3 使用现有协议覆盖不同执行形态，不新增一个协议作为重构理由。普通 SOCKS、加密流、复用连接、QUIC、共享 Packet 设备都是标准的验收对象。

代码责任迁移：

- `crates/config/src/compile.rs` 中资源准备与运行构建迁入宿主构建过程；配置结构验证继续留在 config。

- `crates/engine/src/plan` 接收中立的目标描述；不再在执行期把公共协议配置投影给 proxy。

- `crates/engine/src/runtime/configuration.rs` 的持久化和跨组件更新由宿主接管，领域层保留快照与 revision 语义。

- `crates/proxy/src/runtime/handle/configuration.rs`、`runtime/orchestration` 与 `src/application/services.rs` 的协调收敛为宿主事务。

- `crates/proxy/src/adapters` 逐步只保留一次配置转换和装配；协议运行实现进入所属 protocol，不能把转发壳搬进去后原样保留。

- `protocol_registry/context` 的网络能力抽为中立接口；会话执行服务仍是 proxy 内部实现。

- API、CLI、gRPC、FFI 和 Connector 的适配与内部重构分别提交和评审。

## 回退边界

每阶段先保留旧 facade，再迁移调用，最后移除旧实现。回退默认是回退该阶段源码，不同时维持两套生产数据路径。

二进制降级必须单独校验持久化格式和 outbox 的读取兼容；不能假定旧二进制能理解新状态。失败注入只在一次性测试实例或隔离环境执行，不在当前运行内核上试验。

## 逐接口迁移落点

| 当前接口或位置 | 最终处理 | 承接对象与 PR | 迁移完成证据 |
| --- | --- | --- | --- |
| ProtocolSupportCapability 与 register_core/managed/upstream_capability | 合并元数据、验证和工厂注册；删除空能力约束 | ComponentRegistration、ComponentFactory；PR02/03 | 仅 Stream 出站无需实现入站或 UDP 空 trait |
| OutboundLeafClaim、ClaimedTcpOutboundLeaf、ClaimedUdpFlowLeaf 等 | 从协议接入面删除 | 实例实际角色句柄；PR03/04/05 | 请求路径取得已构建角色，不再逐次 claim 全局 leaf |
| PreparedTransportLeaf、TransportLeafTcpConnectOperation 及 relay 包装 | 合并同义转发，删除公开接入要求 | StreamOutbound 与 UpstreamTransport；PR04A/B | 直接和链式使用同一 connect；计量、半关闭和错误一致 |
| PreparedTcpConnectOperation/PreparedUdpFlowOperation | 退出协议边界；过渡期间内部适配后删除 | 会话执行器直接调用角色；PR04/05 | 不要求协议认识 proxy operation 类型 |
| ManagedStreamConnectorParts → FlowBuild → Flow | 删除重复拼装契约 | 协议自有 DatagramSession 构建；PR05B | 无重复 tuple 包装，不要求接入者理解公共 manager 模板 |
| ManagedUdpFlowResume 的 Any、downcast 和 handler supports 扫描 | 从正常数据路径删除 | 会话保存可执行 Tx/Rx/Control；PR05A/B | 所有现有协议 UDP 路径不用 Any 恢复状态 |
| InboundListenerCapability 的 bind/prepare/update/rollback 多入口 | 合并为构建配方和标准请求源 | InboundSource、ResourceLifecycle；PR06/07 | 各入口失败归同一事务；协议拥有握手，公共执行器拥有策略 |
| InboundClientResponse、Control、Fallback、各类 multiplexer | 保留真实语义，统一关联和擦除入口 | AcceptedRequest 及所属角色；PR06 | 响应一次、预读一次、逻辑子流独立，无普通流强制转换 |
| ClaimedPacketLeaf、设备生命周期与 observer/controller | Packet 角色和唯一资源所有者分离，观测控制保持可选 | PacketSession、ComponentInstance；PR09 | 一个 Endpoint 多角色只创建和关闭一次 |
| engine::runtime::configuration 的文件持久化和 stage/commit | 迁入宿主事务；engine 保留领域配置视图及策略事实 | RuntimeHost；PR07A/B | CLI、IPC、HTTP、gRPC 不再各做一套补偿 |
| proxy::handle::configuration 与 ConfigApplyReconciler | 过渡转接后合并为宿主参与者 | RuntimeHost 的有界生命周期调用；PR07B | 应用服务失败也按同一提交/恢复结果确认 |
| DNS 重载、监听 reconcile、应用 event dispatcher 装配 | 按依赖进入宿主事务，不由 proxy 取得全局所有权 | DNS/Network/Connector 自有组件；PR07/08/10 | 无相互调用私有事务，停旧再建且控制面保留 |
| transport 内协议命名的请求/流计划与桥 | 迁回协议 runtime；公共 carrier、TLS、计量包装保留 | protocols/* 与通用 zero-transport；PR04B/05/06 | transport 不再拥有协议私有 config/flow/resume 决策 |
| src/main.rs、application、FFI 入口 | main 保持薄；应用调用宿主，FFI 保留原功能范围 | PR07A/10 | 不因抽取宿主让 FFI 自动启动完整数据面 |

核对入口包括 [当前能力和 claim 定义](../../crates/proxy/src/protocol_registry/capability.rs#L1)、[实际注册泛型约束](../../crates/proxy/src/protocol_registry/registry/build.rs#L100)、[编译协议清单](../../crates/proxy/src/register.rs#L16)、[UDP 类型恢复](../../crates/proxy/src/runtime/udp_flow/managed/flow/resume.rs#L1)、[现有跨层应用事务](../../crates/proxy/src/runtime/handle/configuration.rs#L63)。上表是目标去留，不表示这些接口已经删除。

## 逐协议迁移与不可丢失的语义

下表列出必须覆盖的接入形态；实际支持范围仍以各协议当前固定上游、配置能力和可执行基线为准，不借重构新增原来没有的能力。

| 组件 | 协议最终承担 | 内核承接与关键回归 |
| --- | --- | --- |
| SOCKS5 | 版本/认证/命令握手、客户端回复、UDP ASSOCIATE 控制和报文编解码 | 标准 Stream/Datagram、来源约束、控制连接关闭与 UDP 寿命关联 |
| HTTP CONNECT | 请求认证、CONNECT 响应、隧道建立后的字节流 | 有界请求头、拒绝/上游失败回复、预读字节保留；不默认增加 UDP |
| mixed | 通过公共有界嗅探选择 SOCKS/HTTP 接入实现 | 保持入站组合角色，不把它变成通用运行时中的协议分支 |
| Shadowsocks | cipher、AEAD 状态、UDP 关联、重放窗口及响应编码 | 流对象和数据报会话；有效安全状态不能被容量 LRU 淘汰，旧版和 2022 基线分别回归 |
| VLESS | identity、flow、延迟响应、MUX、UDP continuity、协议加密及反向控制状态 | 通用 carrier 与标准通道；无端点逻辑出站不伪造服务器，Control/Multiplex 保持独立 |
| VMess | 用户认证、时间/重放检查、响应、AEAD/framing、MUX 和 UDP 状态 | 现有 VmessAeadStream 接入 Stream，UDP session 自执行；不能把响应验证塞回 proxy |
| Trojan | 密码认证、协议请求、TLS profile 含义、UDP framing 和已有复用状态 | 共用 TLS/carrier，协议私有 password 和身份解析留协议；relay 不重复构造 TLS 私有参数 |
| Hysteria2 | QUIC 认证、拥塞参数、TCP 子流、UDP 单读者分发与重组、共享池 | 内核逐逻辑业务准入和计量；单个 UDP 会话关闭不杀共享连接；传输带宽与业务限速分开 |
| Mieru | 自有 config crate、加密/会话/重传、tunnel 中 CONNECT/UDP ASSOCIATE 协商 | 标准多路请求源和通道；保持已选载体与固定上游，不形成 config/runtime/transport feature 环 |
| WireGuard | 握手、peer、密钥、allowed-IP 授权、协议会话和设备动作 | 一个 Endpoint 的 Packet 角色及公共 raw-IP I/O/stack 生命周期；不按外层 UDP 来源当 peer 身份 |
| direct/block | 无外部协议状态；目标含义由 engine 决定 | direct 的实际 socket/Packet 执行由标准网络与 proxy 执行器负责，block 不构造假通道 |
| DNS、TUN、公共 carrier | 各自组件/设备/解析职责 | 同样遵守作用域、代际、错误和资源约束，不强制实现代理协议角色 |

当前协议包版本仍分别跟随其固定上游；v0.0.3 是产品版本，不能统一写入协议包。互通任务必须记录实际 immutable upstream commit，不用“版本号相同”代替功能完整性证明。

## 三条代表性路径的设计验收

1. VMess/SS Stream：协议 Options 一次验证 → Factory 构建 → 已选 upstream 打开载体 → 协议返回 AEAD 流 → 公共客户端响应/转发/限速/计量 → 单流关闭。通用运行层全程不读取 cipher、UUID、响应头或协议枚举。

2. Hysteria2 或 SOCKS5 Datagram：协议建立关联 → 返回独立 Tx/Rx/Control → 内核按目标和逻辑身份路由与记账 → 一个接收者获取完整业务包 → 关闭该逻辑会话并等待其资源退出。QUIC 单 datagram reader、SOCKS5 控制连接和重组状态留在协议。

3. WireGuard Packet Endpoint：一个资源所有者构建设备 → 入站/出站角色共享 → 协议解密及来源授权 → 中立 Packet 路由或已有 opt-in Flow 转换 → 关联回程 → 关闭时确认 I/O、stack 和自有平台状态。native、flow、packet、translate 的现有地址和 ICMP 语义不变。

这些路径先写成行为用例，再迁移实现；不能只让一个最简单的新协议通过接口编译，就认定标准覆盖了已有内核。

## 不同 PR 之间的过渡规则

PR02 只增加公共契约和必要擦除辅助，旧路径继续执行。PR03 的实例和角色注册先通过单一内部适配接旧执行入口；每一协议迁移后只有一个真实生产执行者，禁止对同一流同时运行新旧路径。

PR04/05 迁走协议私有 transport plan 与 UDP 状态时，同一 PR 同步修改拥有方和调用方；保留的 carrier 泛型公共设施不得反向依赖协议 runtime。PR06 把握手、响应、Control/Fallback 和逻辑源按真实责任迁移，不把 source 变成接入者必须理解的另一个通用 operation。

PR07 的单实例替换规则与现有 active guard 保留策略可能不同，必须在该阶段显式列入兼容性账本、更新 AGENTS.md 对应条目并增加失败恢复及断连影响测试。旧规则不能直接套用来偷偷保留退役池，也不能在 PR00 先删光现有工程边界。PR08 至 PR10 再收敛 DNS/Network、Packet 和外部投影；PR11 删除全部已无职责的过渡层。

每份任务卡必须列出本次触及的旧接口、目标接口、调用者迁移、旧路径删除时点、能力不丢失证明及回退 commit。若一个评审单元实际过大，按既定职责再拆 PR，并更新依赖图；不得把尚未实现的能力长期变成 Unsupported。
<!-- /source-section: 17 -->

<!-- source-section: 18 -->
# 云端 PR 执行计划

## 仓库分支与任务边界

执行仓库为 [zerodenet/core](https://github.com/zerodenet/core)，PR 的 base 统一为 develop。2026 年 10 月 16 日核对的 GitHub develop 与本地一致，提交为 f0c1cc0930df8c34f1822cdd182928511e319b76，当前 workspace 版本为 0.0.3-dev.202610152220。这是设计核对基线，不是后续所有任务强制回退的提交。

每次任务从当时最新的 develop 建立 codex/ 开头的短期分支，在 PR 中记录实际 base SHA 和 head SHA。推荐形式为 codex/v003-pr00-plan、codex/v003-pr01-baseline；名称是任务约定，本轮尚未创建这些分支。

默认串行执行下表评审单元，前一个依赖 PR 合入后，下一任务重新基于 develop。不要把多个彼此依赖的实现同时从旧基线展开，也不要把长时间运行任务的陈旧提交直接覆盖新 develop。需要进一步拆分时，沿既定职责拆分并更新任务卡，不改变目标契约。

PR 的创建、检查、评审、合并、nightly 打包、正式发布、部署和生产验收分别记录。云端任务可创建其范围内的 draft PR；不自动合并，不直接推送 develop 或 main，不创建发布 tag，也不修改发布流程来绕过校验。

本地有 GitHub 与 GitLab 两个 remote；云端以 GitHub 仓库和 PR base 为准，不能假定本地名为 origin 的地址就是 GitHub。GitLab 镜像同步不在本任务内。

## 首个 PR 保存工程设计

PR00 将本方案和执行计划保存到版本化的 design/v0.0.3/，建议拆为 README.md、architecture.md、contracts.md、migration.md、compatibility.md 与 validation.md。这些是工程设计，不是公开用户文档；不放入被 Git 忽略的 docs/，不修改 .gitignore 来追踪个人笔记。

迁移任务卡记录：目标 ID、前置 PR、范围、明确不做的内容、实际基线、契约变化、测试要求、待外部验收项目和完成证据。README 提供总入口和完成状态，状态必须有 PR 与验证记录支撑。

AGENTS.md 中现有接口约束只在对应实现迁移时同步修订，注明过渡边界，不先大面积删除工程要求。公共用户文档的最终变化仍遵循独立文档仓库约定。

私有 Page 和本机绝对路径不是云端可读性的保证。PR00 使用随任务上传的完整交接 Markdown；保存后，后续任务以仓库内方案为执行依据。设计资料只作为源文档，不能成为 build 或 CI 运行依赖。

## PR 拆分与依赖

下表为 17 个评审单元。每个单元默认一个 PR；如果实际 diff 无法独立评审，可进一步拆小，但不能把未完成的协议路径和失败检查留给后续 PR。

| PR 与阶段 | 主要范围 | 前置与验收重点 |
| --- | --- | --- |
| PR00 M0 方案入库 | design/v0.0.3 工程方案、目标与任务卡 | 无代码行为变更；版本分支唯一明确，路径和链接可在云端理解 |
| PR01 M0 基线与门禁 | V1 配置与 wire 样本、行为场景、依赖和性能记录、CI 覆盖清单 | PR00；先记录既有失败，区分真实缺陷与环境限制 |
| PR02 M1 标准契约 | traits/core 的角色、通道、错误、网络与生命周期；公共类型擦除辅助 | PR01；无 proxy 私有依赖，不改变旧执行路径，验证轻量 feature |
| PR03 M2 实例注册 | 工厂、一次配置转换、角色句柄、实例身份与共享资源描述 | PR02；旧 facade 可用，registry 不承担连接执行 |
| PR04A M2 Stream 基础 | 普通 socket 与 session 连接及 relay 接入标准 | PR03；适用协议载荷、半关闭、错误、取消与链路等价 |
| PR04B M2 Stream 载体 | carrier、TLS、复用、QUIC 与延迟响应的出站集成 | PR04A；多载体和复用通过标准入口，相关固定基线互通 |
| PR05A M3 Datagram 会话 | 数据报会话、单接收者、协议内部关联与共享连接 | PR04B；多目标、缓存隔离、响应归属、队列和关闭 |
| PR05B M3 UDP 流式与关联 | stream UDP、控制关联、relay 和 continuity；迁出 Any resume 分发 | PR05A；不重复投递，逻辑结束与传输脱离区别保留 |
| PR06A M3 普通接入 | stream 接入、客户端响应、fallback 与 Control | PR05B；准入与响应一致，取消无泄漏，旧监听行为保持 |
| PR06B M3 复用接入 | MUX、原生 QUIC、datagram 接入和并发请求交付 | PR06A；慢握手不阻塞，子流和连接关闭独立，保留 raw-IP 过渡入口 |
| PR07A M4 宿主提取 | zero-runtime、构建与生命周期装配，Proxy 等 facade 转接 | PR06B；行为保持，所有入口仍按既有功能范围启动 |
| PR07B M4 配置事务 | 持久化、候选准备、资源切换、确认、补偿与代际发布 | PR07A；全入口统一；停旧确认后串行替换，无双份资源；整次提交，恢复失败隔离并保留控制面 |
| PR08A M5 DNS 与 Network | 显式注入、bootstrap、网络代际、拨号约束和池身份 | PR07B；解析不递归、不绕过路径，网络切换正确 |
| PR08B M5 构建与路由 | config 资源加载迁出、中立计划、单一纯求值和异步编排 | PR08A；规则、模式、bypass、DNS 补充与目标选择一致 |
| PR09 M5 Packet 与 Endpoint | 共享资源、Packet 标准、公共转换、raw-IP 过渡移除与 TUN 退出 | PR08B；回程、MTU、交接与源地址正确，平台资格项明确 |
| PR10 M6 外部投影与兼容 | 控制适配、观测计量、Rust facade、FFI 及消费者回归 | PR09；字段和语义都兼容，外部项目仅运行验证 |
| PR11 M6 清理与资格验证 | 移除旧桥、同步工程规则、全目标检查和性能对比 | PR10；G0 至 G10 全部有证据，未验证项不能标记通过 |

目标完成以真实调用路径为准。仅增加新 trait、搬目录、留 TODO、用 Unsupported 替换既有能力或保留双份长期执行框架，都不能关闭对应任务。

## 每个实现 PR 的交付格式

PR 描述应包含：具体问题和最终行为；目标 ID 与前置 PR；变更路径和接口去留；配置及外部兼容结论；执行命令和结果；未完成的外部验收；主要回退边界。报告 head SHA，不能用前一个提交的 CI 结果代替当前提交。

通过变更涉及的检查后才能提出合并。环境缺工具时先补齐合法构建依赖；仍无法执行的项目列为未验证，并说明由哪个现有 CI 或平台任务接管。不得删除测试、移除 ignored 标记来伪造覆盖，或扩大过滤器掩盖失败。

合入后的 develop 必须可运行。当前 nightly 会定时处理 develop，中间提交可能被打包为开发版本；因此不能把关键功能缺失留到最后一个 PR 再恢复。v0.0.2 正式发布由既有发布流程单独处理，此任务不修改 main、promotion-source 或发布工作流来推进重构。
<!-- /source-section: 18 -->

# 执行任务卡入口

[task-cards.md](task-cards.md) 保存全部 17 个单元的逐项任务卡：目标与依赖、实际基线、范围与不做事项、契约变化、调用者和删除边界、验证及外部验收、完成证据和回退 commit。

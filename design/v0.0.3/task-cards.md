# PR00 至 PR11 逐项任务卡

[方案入口](README.md) · [完整迁移映射与依赖](migration.md) · [验收门禁](validation.md)

所有未来单元开工前必须读取本目录完整设计和当前 AGENTS.md，确认前置已合入，核对需求重叠及实际 develop。每张卡均需记录本次触及旧接口、目标接口、调用者、删除时点、能力不丢失证明和回退 commit。下列范围继承正文全部细则，不以卡片摘要替代详细契约。

<a id="pr00"></a>

## PR00 M0

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G0；前置 无 |
| 状态 / 实际基线 | 本分支文档已保存，待评审合入；base `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a` |
| 范围 | 保存完整方案、G0–G10、M0–M6、17 单元任务卡和验收分工；登记来源与实际基线。 |
| 明确不做 | 不修改运行代码、依赖、产品版本、锁文件政策、CI 门禁、发布或生产配置；不实施 PR01。 |
| 契约变化 | 无接口修改；原文所有 Rust 签名、状态机、迁移映射及门禁原样保留，链接按来源规则转换。 |
| 调用者迁移 / 删除时点 | 无调用者迁移、无旧路径删除；AGENTS.md 保持原状。 |
| 测试及能力保持证据 | 全文分节摘要与源快照一致、目标/阶段/任务覆盖、代码围栏、相对链接/行号和 git diff --check。 |
| 待外部验收 | Rust/互通/平台/消费者/RS01 都未执行，不归本单元验收。 |
| 完成证据 | 本目录全文、来源/摘要、检查记录、独立分支 Draft PR；合入状态待记录 |
| 回退边界 / commit | 仅回退本次 design/v0.0.3 文档提交；提交 SHA 见 PR，不改变任何运行状态 |

<a id="pr01"></a>

## PR01 M0

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G0、G9、G10；前置 PR00 |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 冻结旧内核与实际可检出消费者版本、V1 原始样本、行为/依赖/feature/性能与资源基线，登记已有失败和 CI 覆盖。 |
| 明确不做 | 不切换生产执行路径、不改变现有超时/安全窗口/配置；不修改消费者项目或猜测数字。 |
| 契约变化 | 建立 V1-CONFIG/CONTROL/EVENT、TX-BOUNDARY、RECOVERY、IDLE、GUI-V1、PANEL-V1、RUST-FFI 的来源和预期；RS01 实测后冻结预算和阈值。 |
| 调用者迁移 / 删除时点 | 盘点公开 trait/构造函数/配置/feature 及实际调用者；只建迁移清单，不删除旧接口。 |
| 测试及能力保持证据 | 固定旧内核真实运行和配置/wire 捕获；依赖树与最小 feature；同负载 RS01 的启动、稳态、重载、恢复、拒绝及资源回收。 |
| 待外部验收 | 真实 GUI/captures、面板源码权限、平台/网络设备和受限 1 CPU/1 GiB 环境若缺失逐项列待验收。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr02"></a>

## PR02 M1

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G1、G6；前置 PR01 |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 定义中立身份/角色、Stream/Datagram/IpPacket、错误、网络和生命周期契约及集中擦除辅助。 |
| 明确不做 | 不切换任何协议生产路径；不引入 proxy 私有类型、万能 context、额外通用运行时或提前提取 zero-runtime。 |
| 契约变化 | traits/core 纯类型保持 no_std；Tokio 运行契约在 platform-tokio::contract，运行错误归 zero-error；关联类型、Pin 和拥有关系按详细签名。 |
| 调用者迁移 / 删除时点 | 新增公共 contract 和泛型装配辅助；旧 capability/operation 仍执行，禁止双生产执行者。 |
| 测试及能力保持证据 | CF01：仅 Stream、非 Sync Rx、非 Unpin 流、动态注册调用；CF02：no_std/validation-only 最小 feature、依赖 DAG 和 Future Send。 |
| 待外部验收 | 实际下游 Rust 编译与平台 feature 矩阵记录可运行范围，不把文档签名算编译通过。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr03"></a>

## PR03 M2

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G2；前置 PR02 |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 工厂注册、一次 Options 转换、ComponentId/RoleId/资源身份和实际角色句柄；旧 facade 通过一处适配继续运行。 |
| 明确不做 | registry 不执行握手；不按 connect 重解析全局配置；不实现未声明角色的空 trait。 |
| 契约变化 | ComponentRegistration/Factory、ComponentSpec/Instance；describe/build 无运行 I/O，先持有生命周期所有者再 start。 |
| 调用者迁移 / 删除时点 | 迁移 ProtocolSupportCapability、register_*_capability 与 leaf claim 的登记职责；调用者取得已构建角色，已迁移路径删除同义登记。 |
| 测试及能力保持证据 | 重复注册、未知/未编译协议、字段路径、一次转换、共享 Endpoint 角色身份及最小 feature；工厂失败仍有关闭对象。 |
| 待外部验收 | 相关固定协议与消费者 baseline 不变；未迁移出站保留真实旧执行路径。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr04a"></a>

## PR04A M2

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G3；前置 PR03 |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 普通 socket/session Stream 连接和 relay 进入标准通道，验证直接及链式同入口。 |
| 明确不做 | 不提前迁移所有 carrier/MUX/QUIC；不重写协议加密或静默绕过上游前缀。 |
| 契约变化 | StreamOutbound/Connected/StreamIo、UpstreamTransport 和标准错误；协议交付具体流，内核转发/背压/限速/计量。 |
| 调用者迁移 / 删除时点 | 迁移普通连接的 ClaimedTcpOutboundLeaf/PreparedTcp* 调用者；替代路径验证后删除该路径重复 claim/operation，保留下一单元所需桥。 |
| 测试及能力保持证据 | IO01 载荷、预读、EOF/半关闭、取消、错误阶段、硬拨号约束、直连/链路等价与内外层计量。 |
| 待外部验收 | 适用 SOCKS5/HTTP/SS/VMess 等现有角色按固定基线互通；实际名单以 PR01 能力表为准。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr04b"></a>

## PR04B M2

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G3；前置 PR04A |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 集成 carrier/TLS、复用/QUIC、延迟响应和协议私有 transport plan 的出站路径。 |
| 明确不做 | 不把帧式 MUX 与原生 QUIC 强行统一帧格式；不制造远端确认或为每层新增驱动任务。 |
| 契约变化 | PeerConfirmation/标准建连观察器、受约束 opener、协议自有信任/池身份；carrier 只保留通用 I/O。 |
| 调用者迁移 / 删除时点 | 同步迁移 transport 协议命名 plan/bridge 与 owning protocol 调用者；标准通道覆盖后删除重复 PreparedTransportLeaf/relay 包装。 |
| 测试及能力保持证据 | IO01 首业务字节依赖的延迟确认、响应失败、TLS/多载体、复用隔离、取消、半关闭和链式载体等价。 |
| 待外部验收 | 受影响协议固定 immutable upstream 的真实互通；原生 QUIC 与不同平台结果分别登记。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr05a"></a>

## PR05A M3

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G3；前置 PR04B |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 引入协议自执行 DatagramSession、单接收者、Tx/Rx/Control 拆分、协议关联和共享连接隔离。 |
| 明确不做 | 不新增通用可靠 UDP/全局 handler 猜测；不把 UDP 当 IpPacket，不为了容量淘汰活跃会话或安全状态。 |
| 契约变化 | DatagramOutbound、DatagramLimits、SendReceipt/Failure、NotSubmitted/LocallyAccepted/Unknown、明确单包和关闭语义。 |
| 调用者迁移 / 删除时点 | 原生 datagram 角色和 packet-path carrier 的收发改用会话；协议保有私有缓存和重组，迁移一条路径即移除其旧分发。 |
| 测试及能力保持证据 | IO02 多目标、零长有效报文、完整边界、超长、队列压力、来源/关联、单读者、共享连接逻辑关闭与缓存隔离。 |
| 待外部验收 | 适用 HY2/SOCKS5/SS/Mieru 等固定互通；不以本地 loopback 代替协议资格。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr05b"></a>

## PR05B M3

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G3；前置 PR05A |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 迁移 stream UDP、UDP ASSOCIATE 控制、relay、continuity；清除正常 UDP 数据路径 Any resume/handler supports 扫描。 |
| 明确不做 | 不重放投递不确定的业务包；不把传输脱离等同逻辑结束，不扩大协议原有能力。 |
| 契约变化 | 会话 Tx/Rx/Control 执行自身行为，continuity key 协议验证并按接入/主体隔离；保留协议序号去重恢复。 |
| 调用者迁移 / 删除时点 | 同步迁移 ManagedUdpFlowResume、Managed/UpstreamUdpHandlerProvider、ManagedStreamConnectorParts→FlowBuild→Flow 调用者；验证完成即删除旧类型恢复链。 |
| 测试及能力保持证据 | IO02 取消中途写/残帧、无重复交付、控制连接关闭、迟到响应、双流/relay、身份和代际隔离及结束后不可复活。 |
| 待外部验收 | 所有现有协议 UDP 固定基线互通；真实流式/复用关联不可只用 mock 替代。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr06a"></a>

## PR06A M3

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G4；前置 PR05B |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 普通 Stream 接入、握手/请求拆分、标准客户端响应、预读、Fallback 与 Control。 |
| 明确不做 | 不把整个业务会话交给万能 Process；不让协议执行内核准入/路由/全局配额。 |
| 契约变化 | InboundSource/Handshake、AcceptedRequest、ReplyOutcome；一次响应、一次交付、预读一次，身份归协议认证事实。 |
| 调用者迁移 / 删除时点 | 迁移普通 InboundListenerCapability/PreparedInboundListenerOperation，保留 Control/Fallback 真实职责；拥有方和调用者同 PR 切换。 |
| 测试及能力保持证据 | 有界握手、拒绝/失败回复、延迟确认死锁防护、取消不泄漏、fallback replay/计量、无业务目标 Control 准入。 |
| 待外部验收 | 各普通接入协议互通及控制/回退真实场景；平台监听资源资格单列。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr06b"></a>

## PR06B M3

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G4；前置 PR06A |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 迁移 MUX、原生 QUIC、datagram 接入，统一标准业务交付并保留 raw-IP 过渡入口。 |
| 明确不做 | 不强制 u16 流 ID 或统一 reader；不提前删除 PR09 仍需的 Packet/raw-IP 路径。 |
| 契约变化 | RequestMultiplexer 和有界 PendingInbound 执行；逐逻辑业务身份/路由/计量/关闭，共享载体所有者明确。 |
| 调用者迁移 / 删除时点 | 迁移各 Inbound multiplexer 的交付调用者和协议私有接入状态；标准源覆盖后移除重复 proxy 接入操作桥。 |
| 测试及能力保持证据 | 慢握手不阻塞同连接其他子流或 listener；子流 END/载体断开区别、单读者、并发准入和取消、共享连接不误关。 |
| 待外部验收 | 真实 MUX/QUIC/HY2/Mieru 等适用基线；raw-IP 旧路径可运行证据保留到 PR09。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr07a"></a>

## PR07A M4

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G5；前置 PR06B |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 提取 zero-runtime 的可复用宿主、显式装配与生命周期作用域；Proxy/Handle/Running facade 转接。 |
| 明确不做 | 不为每个 Manager 新增 crate；不让 FFI 的 EngineHandle 入口自动启动完整数据面；不提前混入全部事务语义切换。 |
| 契约变化 | RuntimeHost/ResourceLifecycle/轻量 TaskScope；复用现有 Tokio Handle，关键/辅助任务监督和退出确认。 |
| 调用者迁移 / 删除时点 | 迁移 application/proxy 整体装配，保留实际功能范围的入口和必要过渡 facade；领域策略仍归 engine。 |
| 测试及能力保持证据 | 启动/停止、登记关闭竞争、完成句柄回收、I/O 关闭确认、最小与可选 zero-runtime feature、依赖 DAG 及现有入口行为。 |
| 待外部验收 | 三平台资源所有权/停止资格，Rust 嵌入和 FFI 原功能范围与 ABI 回归。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr07b"></a>

## PR07B M4

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G5、G9；前置 PR07A |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 统一全入口配置事务、持久化、代际提交、局部重建、失败补偿和 Faulted 隔离。 |
| 明确不做 | 禁止同资源新旧实例并行、提前占备用监听、长期退役池排空；候选清理未确认禁止恢复同资源旧实例；不伪造全局原子性。 |
| 契约变化 | Validate/Plan/StopOld/BuildNew/Persist/Commit/Acknowledge/Compensate；revision 与健康分离，PreparedUpdate 仅有界计划；普通更新保留连接、撤权终止、限速即时。 |
| 调用者迁移 / 删除时点 | 迁移 engine 配置持久化、proxy handle/ConfigApplyReconciler、DNS/监听/应用服务协调；更新 AGENTS 旧预绑定/active guard 规则及兼容账本，删除第二套最终确认。 |
| 测试及能力保持证据 | LF01/LF02、TX-BOUNDARY/RECOVERY 全阶段注入；调用方超时、崩溃记录、候选清理/恢复失败、无变化、仅受影响断连；RS01 重复切换无重叠。 |
| 待外部验收 | 平台路由/DNS/设备恢复与 GUI/面板应用回执；真实 1 CPU/1 GiB RSS/任务/FD 峰值证据。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr08a"></a>

## PR08A M5

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G6；前置 PR07B |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | DNS/Network 显式依赖、查询角色、bootstrap 环检查、硬拨号约束、网络代际和池身份。 |
| 明确不做 | 不偷偷系统 DNS/direct；不因网络变化全局清空无关池；不让普通出站取得绕过 upstream 的全局 Network。 |
| 契约变化 | DnsService/NetworkService 和 UpstreamTransport；静态/动态递归检测、出口身份、真实网络事实。 |
| 调用者迁移 / 删除时点 | 迁移 protocol_registry/context 的网络能力与 DNS 私有 proxy 依赖；上层注入中立服务并同步所有调用者。 |
| 测试及能力保持证据 | RT01 bootstrap/经出站递归、服务器与业务解析角色、取消缓存提交、硬族/源/接口约束、旧连接不在新代际发送、相关池隔离。 |
| 待外部验收 | 三平台物理出口与网络切换实际证据；hosted runner 无场景时列待验收。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr08b"></a>

## PR08B M5

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G6；前置 PR08A |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 迁出 config 的资源运行构建，engine 接收中立计划；统一纯路由求值与异步 NeedFacts 编排。 |
| 明确不做 | 不在逐会话加载规则或解释协议配置；不重复 bypass/模式优先级，不用空结果无限重新解析。 |
| 契约变化 | ValidatedConfig/中立目标、固定求值代际 RequestFacts→RouteDecision/NeedFacts；direct/block 领域语义保持。 |
| 调用者迁移 / 删除时点 | 迁移 config::compile、engine::plan 运行投影及 proxy 路由重复逻辑；资源加载归宿主，旧路径调用归零后删除。 |
| 测试及能力保持证据 | RT01 固定旧配置/事实对照，fake-IP 恢复/缺失、组/健康、规则/bypass/模式、事实循环上限和代际一致性。 |
| 待外部验收 | 冻结消费者 renderer 产物和实际固定协议执行；不把纯求值单测当网络执行证据。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr09"></a>

## PR09 M5

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G7；前置 PR08B |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | PacketSession、Endpoint 多角色共享资源、公共 Packet/Flow 转换、raw-IP 过渡移除与 TUN 关闭确认。 |
| 明确不做 | 不新增 ICMP Flow，不把 native Packet 布尔值当转换可执行；不按外层 UDP 来源选择 WG peer，不关闭借入设备。 |
| 契约变化 | PacketTx/Rx/Limits、唯一生命周期所有者、MTU/地址/回程与发送交接；保留 auto/packet/flow/translate 语义。 |
| 调用者迁移 / 删除时点 | 迁移 ClaimedPacketLeaf、OutboundDeviceLifecycle/PreparedDevice、私有 PacketFlowAdapter 和 raw-IP 调用者；完整替代验证后删除过渡入口。 |
| 测试及能力保持证据 | IO02、LF02：IPv4/6/MTU、回程关联/源地址、交接前后失败、共享资源一次创建关闭、每 Flow 关闭不误关 Endpoint。 |
| 待外部验收 | WireGuard immutable reference 互通及 Linux/macOS/Windows 特权 TUN/设备/路由/DNS 恢复；无设备不冒充通过。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr10"></a>

## PR10 M6

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G8、G9；前置 PR09 |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 外部控制和观测投影、计量与事件、Rust facade 迁移说明、现有 FFI 及消费者真实组合回归。 |
| 明确不做 | 不改客户端 UI/面板业务，不扩大 FFI 功能、不改 ABI，不把相同 JSON 字段当语义兼容。 |
| 契约变化 | V1 配置/控制、zero.api.v1/zero.event.v1、ACK/snapshot/增量/重放/结算、Inner/Outer 事实与投递义务。 |
| 调用者迁移 / 删除时点 | CLI/HTTP/IPC/gRPC/Connector/FFI 调用适用宿主/领域服务；公开 Rust trait/构造函数/配置/feature 逐项迁移，必要 facade 设删除边界。 |
| 测试及能力保持证据 | EX01、GUI-V1/PANEL-V1/RUST-FFI 原始样本与新旧真实内核交叉；事件身份/重置/重复投递、无变化发布、ABI/分配释放/线程。 |
| 待外部验收 | 固定可检出 GUI/面板提交与外部 captures、真实设备验收；仅运行外部验证，不自动修改消费者仓库。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

<a id="pr11"></a>

## PR11 M6

| 字段 | 执行要求 |
| --- | --- |
| 目标 ID / 前置 | G0–G10；前置 PR10 |
| 状态 / 实际基线 | 未开始；前置合入后从当时最新 develop 开分支并登记实际 base/head，当前没有本单元 SHA |
| 范围 | 删除所有无职责旧桥、同步 AGENTS/README/feature 与公开迁移资料，汇总全部资格和资源性能对照。 |
| 明确不做 | 不以接口/目录数减少、TODO、Unsupported 或仅编译通过关闭任务；不自动合并/发版/部署。 |
| 契约变化 | 最终目标依赖 DAG、标准通道、唯一事务及生命周期成为实际生产路径，所有过渡层有删除结论。 |
| 调用者迁移 / 删除时点 | MIG01 检索旧 claim/operation/handler/Any/私有桥及实际调用者；只删除已完成替代与回归的路径。 |
| 测试及能力保持证据 | 全量 workspace/fmt/严格 clippy、最小/可选 feature、显式完整 CI、全部受影响互通/平台；RS01 同负载资源/吞吐/延迟/分配/构建对照。 |
| 待外部验收 | G9 消费者与真实设备证据；G0–G10 每项必要资格均满足才关闭，否则标待验收并列出精确缺项。 |
| 完成证据 | 实际 diff、任务目标、base/head、命令输出、选中且成功的 CI/互通结果、兼容及外部缺项；不能引用旧 head 结果 |
| 回退边界 / commit | 开工记录实际 base，交付填写本单元 commit 及源码回退范围；二进制降级另验持久化/outbox，不同时保留双生产路径 |

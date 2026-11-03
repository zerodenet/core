# v0.0.3 本地检查点与剩余缺口

## 2026-11-03 当前修订

本轮 A/B 的当前实现和候选资格按[云端执行账本](../cloud/execution.md)另记。下文 Config/Engine/Hysteria2 未迁的表述属于本轮之前的历史切面；已迁职责不重新列为待办，其余实现缺口仍保留。

全部旧 listener 调用者已迁入标准 factory/source，最后 WireGuard 入站 Packet 和 SS 插件接入也已完成；InboundListenerCapability、PreparedInboundListenerOperation、BoundInbound 及旧执行链整体删除。下文 10 月各批次及表格中的“接入/Packet 标准角色未迁”描述按其历史子范围理解，不重新作为 listener 待办。

当前必须完成的实现域为 Config 资源 I/O 迁出、Engine 宿主准备/持久化职责迁出、其余出站/共享载体标准角色和 Registry 总 leaf 投影收敛、System/detour/池化 DNS 的剩余原始所有权、非入站 Packet/设备/TUN 归属，以及适用资源上限生产配置。G8 投影对账、G9/G10 完整平台/互通/消费者/低资源资格另外记录。TEMP-WG-OUTER-UDP-01 是唯一明确批准的临时例外，未关闭，不扩展为其他路径的豁免。

首轮云端 A 承接 Z003-10 → Z003-09，B 承接 Hysteria2 既有出站共享池完整子域，C 集成对账。完整范围、分支/PR 依赖与验收见 [云端任务包](../cloud/README.md)。用户已授权独立基线提交推送；下文“不提交/不推送”均是历史状态，当前禁止合入 develop/main、发布和部署。

更新日期：2026-10-31；检查点日期：2026-10-25。分支 develop，承接云端 PR #124 合入点 `b963c864a473f68f8d6a8444764e3e88761e6c04`。本地检查点保存已实现和已验证的工作，ALL17 / G0–G10 整体目标仍未完成，普通 Mieru native background 生产登记保持关闭。

下列项目区分原生路径的功能缺口、架构迁移债务和未执行的资格验收。未完成原生迁移不等于现有兼容路径不可用；不能通过开启未资格工厂或删掉兼容路径来关闭这些项目。

## 本批 Packet/device 旧接口已收敛，原始所有权边界继续开放

2026-10-31：删除 `ClaimedPacketLeaf`、设备生命周期 trait/boxed prepared-state/erased completion 和 Registry 的整条设备转发链。Packet 直接移交实际执行操作，设备由同一真实 adapter/pool 的具体装配值承接 prepare/publish/recovery/close；普通可确认的设备先关闭并确认旧原件，再建立新代，丢弃候选保留实际关闭原件。未新增协议组合、替代 SPI 或 native 生产登记。

用户已明确选择：**保留 WireGuard `outer_udp_proxy`（含重连历史）的既有兼容行为，只在本批删除旧接口，完整原始所有权/关闭凭据继续列债务**。不能把当前 path、grace 到期、Drop、worker 结束算作全部历史原件已关闭，也不能为删接口而新增换代拒绝破坏现有能力。Windows host-session 仍是既有 worker completion 边界。Packet 标准角色、未资格设备/载体和完整替换条件继续属于 Z003-06 / Z003-05 的未完成范围；具体状态见[实施记录](README.md#packetdevice-职责域收敛2026-10-31本地增量)。只改内核，未提交/推送，未运行全量。

## 当前关闭的完整职责域：managed UDP 执行

2026-10-31：剩余 managed UDP 调用者已整体迁到静态构造、实际连接登记和直接执行，移除 handler 集合/trait、Any/resume 恢复、动态 start/forward 分流及对应 manager 树。具体协议的类型化 wire 配方仍由协议拥有，仅参与冷建立和必要的逻辑重建；不再是运行时恢复 SPI。冷缓存维持类型隔离和原有私有键，首包不重复发送/计量，失败/取消释放业务索引，失败建立不积累缓存配方。原始资源/任务/关闭及 native 准入边界保持，未扩协议组合。验证与严格交付边界见[实施记录](README.md)。本地 develop，只改内核，未提交/推送。

## 已关闭的前序接口批次：UDP flow claim 与 handler provider

2026-10-31：完整删除 `ManagedUdpHandlerProvider` 的六个实现、注册表 provider 字段/扫描和专用注册方法。每 dispatch 在 `register.rs` 直接选择同一套既有可执行 handler，使用实际 resume 类型，不新增替代 SPI、协议组合或登记；stream/relay 仍共享原 manager，真实池/任务/关闭顺序保持。provider 接入层已关闭，实际 managed handler/Any/resume 执行层仍待后续迁移，不能混记。只改内核，未提交/推送，验证见[实施记录](README.md)。

2026-10-31：完整删除 `ClaimedUdpFlowLeaf`、全部 claim 包装/闭包及重复 source_dir 转发。适配器直接交付已有的 owned flow/relay 执行结果，路由移交一次，relay 在前缀准备后移动末跳；不用新 trait、回调工厂或共享取件槽替代旧接口。真实资源/任务/关闭和计量执行边界保持，未扩大协议组合或生产登记。剩余 managed UDP handler/resume、Packet/device 契约及 Engine/Config 职责迁出继续单独列账；整体架构未完成。验证见[实施记录](README.md)。只改内核，未提交/推送。

## 已关闭的前序接口批次：TCP claim

2026-10-30：删除 `ClaimedTcpOutboundLeaf`、全部 helper/包装和测试专用 TCP claimer；所有 TCP 调用者直接消费原有可执行操作与冷诊断数据。两种视图共享实际 owner，保留错误阶段、Packet 选择、实际 source_dir、Mieru lazy prefix 和现有停旧替换约束。对应接口族完整收尾，不新增替代 SPI 或协议组合。剩余 UDP flow claim/handler、设备契约、未迁标准角色及 Engine/Config 职责迁出继续单独列账；Z003-06 和整体架构仍未完成。验证见[实施记录](README.md)。只改内核，未提交/推送。

## 已关闭的前序接口批次：packet-path claim

2026-10-30：按完整接口族删除 `ClaimedUdpPacketPathLeaf`，四个协议与全部 inventory/relay/device 调用者直接转交原有可执行操作，取消二次准备和延迟源目录闭包。没有新增协议组合、替代 SPI 或 native 生产登记。该接口族已从生产与测试调用链删除，剩余标准角色迁移、TCP/UDP claim 和设备契约仍单独列为 Z003-06 债务；不能记为整体重构完成。验证结果见[实施记录](README.md)。只改内核，未提交/推送。

## 本次 VLESS/Trojan MUX 首跳生产增量

2026-10-30：继续迁移 VLESS 显式同并发 XUDP/MUX、Trojan literal/default-dial 首跳，复用三种协议的公共冷准入规则和既有资源上限。生产只公布协议实际 Stream；原 UDP/packet 兼容入口复用真实原件和宿主池，gRPC 保留实际 LoanOwner/父链及停旧替换。普通 VLESS UDP、不同 XUDP/MUX 并发、非首跳/嵌套及其他载体不被自动改写/晋升。本批 66 项必要定向通过（新增 6 项，无未解决失败），范围和冻结证据见[实施记录](README.md#vlesstrojan-mux-首跳生产迁移2026-10-30本地增量)。不关闭 Z003-02 / Z003-03 / Z003-06 整体，未修改客户端/面板，未提交/推送，未重跑全量。

## 本次普通 VMess MUX 首跳生产增量

2026-10-30：普通 Proxy 已接入合格的 VMess MUX 首跳 → 已有标准中间角色 → 非 MUX VMess gRPC Datagram。生产前缀只公布真实 Stream；普通 TCP 标准角色和兼容 UDP 共用其实际原始池和宿主 driver，不生成第二套 legacy 池。按实际 tag 保留 legacy 否定准入并隔离其他节点；新增可选按需物理池上限，默认 4，省略时导出形状不变。非首跳/嵌套及不合格组合、VLESS/Trojan MUX 和 Mieru 普通登记保持兼容边界；Z003-02 / Z003-03 / Z003-06 均未整体关闭。63 项必要定向累计通过（新增 7 项，无未解决失败），范围和修正后的冻结证据见[实施记录](README.md#vmess-grpc-普通-vmess-mux-首跳前缀2026-10-30本地增量)。只改内核，未提交/推送，未重跑全量。

## 本次 VMess gRPC 混合与池化 relay 增量

2026-10-30：普通标准 Stream 前缀的非 I/O 共享依赖可保留原 owner 后直接使用标准 Datagram，补上 Shadowsocks TCP2022 replay 维护组合。受控 MUX 目录补齐实际 StreamLoan 控制、供体原始来源、gRPC H2 消费以及中间池的上游借用；配置替换仍按实际依赖先结清旧消费者/供体。普通 native MUX/Mieru 生产登记继续关闭，不能把受控组合或协议接口记为普通生产迁移完成；旧混合/网络池化前缀仍有兼容调用者。54 项必要定向回归通过（新增 4 项），同时删除重复的公开异步 prepared 载体接口；结果和具体边界见[实施记录](README.md#vmess-grpc-混合与池化-relay2026-10-30本地增量)。Z003-02 / Z003-03 / Z003-06 及整体计划仍未关闭；仅内核本地变动，未提交或推送，不重跑全量。

## 本次 VMess gRPC UDP relay 生产增量

2026-10-30：普通标准 Stream 前缀、默认拨号的非 MUX gRPC UDP relay 接通标准 Datagram、真实前缀握手及实际 Flow/Outer 派发与计量；宿主保留唯一真实 TCP 原始控制器和原 H2 driver，复用实际代次依赖传播前缀替换，旧资源确认清理后才启动新 owner。原生 Node DNS 只解析实际首跳；末跳原名经前缀传递，保持 TLS/CA 身份。合格链不再使用 claim/prepared relay operation，已选择角色撤销不得转回旧入口。混合/池化等未合格前缀继续使用已有兼容调用者，native MUX/Mieru 普通登记不开启；Z003-02 / Z003-03 / Z003-06 未整体关闭。35 项必要定向回归通过（新增 7 项 relay），构建后复用同一个二进制；验证及范围见[实施记录](README.md#vmess-grpc-非-mux-udp-relay-生产迁移2026-10-30)，不重跑全量，只改内核，未提交或推送。

## 本次 VMess gRPC 域名/DNS 生产增量

2026-10-30：在普通 Proxy 中接入非 MUX gRPC UDP 域名节点，要求候选 DNS 后端均具有真实 native provider，遵守既有 Node 策略、TaskScope 和原始关闭凭据。共享 DNS 真实 ComponentId 依赖负责传播替换；先清理旧用户/driver/native，再启动新 owner。保留原域名的 TLS 身份/日志与实际 IP 的 path 投影，新旧 DNS 缓存共享一个总容量。无配置 DNS、System/detour、外部 ECH、relay 与其余载体继续保留兼容路径，不开启 native MUX/Mieru 普通登记。25 项必要定向回归通过（新增 9 项），未重跑全量；具体生产回归及最终源码验证见[实施记录](README.md#vmess-grpc-udp-域名dns-生产装配2026-10-30)。Z003-02 / Z003-03 / Z003-06 和整体计划仍未关闭，只改内核，未提交或推送。

## 本次 VMess gRPC 外部 CA 生产增量

2026-10-30：literal endpoint、受支持 prepared TLS 的非 MUX gRPC 外部 CA 已接入普通 Proxy 注册、标准 Datagram 派发与实际 TLS/UDP 收发。候选配置传递配置目录与可配置文件预算（`runtime.file_read_max_bytes` 默认 8 MiB，省略时导出形状不变）；替换消费同一旧 owner 的关闭结果后才加载新 CA 并启动新代际。15 项定向回归通过（新增 6 项），未重跑全量，未提交或推送。hostname/DNS、外部 ECH、relay 与其他载体仍保留兼容路径，Z003-02 / Z003-03 / Z003-06 继续保持整体未关闭。详情及首轮测试修正见 [实施记录](README.md#vmess-grpc-udp-外部-ca-生产装配2026-10-30)。

## 上一批 VMess gRPC UDP 增量

普通 literal endpoint、无外部 CA/ECH 且由现有 prepared TLS 支持的 gRPC 非 MUX UDP 已接通生产标准 Datagram、实际派发和既有 Flow/Outer 计量；明文/TLS、普通和 multi_mode 共用实际 gRPC 实现。每 flow 持有真实 H2 driver、原始 native provider 及 transferred custody，网络换代和配置替换等待原始用户/driver/native 确认结清。此子集不再执行旧 UDP preparation；TCP、relay、hostname、外部 CA/ECH、其他 TLS 风格和 MUX 仍保持兼容。4 项真实 Proxy 业务回归、1 项原始授权回归与两组 217 项分层守卫通过；严格检查及最终全量状态见 [本批实施记录](README.md#vmess-grpc-非-mux-udp-生产迁移2026-10-29本地增量)。Z003-02 / Z003-03 / Z003-06 继续保持整体未关闭，其他项目未改，未提交或推送。

接续的冻结全量已经完成：3886/0/175、155 个目标、结束码 0，源码无漂移。其后补齐 gRPC Datagram 的外部 CA 冷准备接口，显式目录与字节上限参与完整身份，VMess 组件定向 6/0（新增 2 项）；未重跑全量。该增量尚未接通外部 CA 生产预算和实际 Proxy 装配，不计作 CA 生产迁移完成。域名/DNS、外部 ECH、relay 等仍保留兼容路径，详见本批实施记录。

## 上一批普通 VMess UDP 增量

普通 TCP/TLS/WS 非 MUX UDP 已接通标准 Datagram、每 flow 实际派发、原控制器保留及既有 Flow/Outer 计量；对应普通 claim/prepared 直接入口不再可执行。该批尚未迁移 gRPC；浏览器、外部 CA/ECH、relay 与 MUX 的既有兼容路径仍保留，Z003-03 / Z003-06 不整体关闭。新增 6 项真实 Proxy 回归和原 gRPC 业务回归均在冻结全量中通过（3879/0/175），另有 VMess-only 的宿主正常清理实际回归通过；全量之后仅修正一个多余借用并完成严格静态检查，快照边界及结果见 [实施记录](README.md#普通-vmess-非-mux-udp-生产路径2026-10-29本地增量)。只改内核，尚未提交或推送。

## 本次保存范围

- 独立 zero-runtime、共享 RuntimeHost、配置操作所有权、资源代际及确认停旧后再启动替代资源的事务。
- Mieru 实际共享池、完整 profile/凭据替换、TCP/UDP 角色、endpoint、prefix、跨 owner 和无独立 driver 原始转交的分批接线。
- 同一个 Proxy / DnsSystem / RuntimeHost 中的原生 UDP、TCP、DoT、DoH、DoQ 查询所有者、真实任务监督及原始关闭凭据。
- 通用 controlled metadata pool，以及通用 inventory 构造与 UDP handler 的独立功能门控。
- 对应协议、CI 路径、工程规则、兼容账本、回归与历史失败证据。协议上游版本基线保持不变。

代码验证来自最终源码快照（历史材料见检查点 `8acd1cca`）和验证清单（历史材料见检查点 `8acd1cca`）：Rust 1.99 完整 workspace/all-features 为 **3832 通过、0 失败、175 忽略，155 个目标**；Rust 1.98 严格 all-targets/all-features Clippy、DNS-only / no-default 编译通过。完整测试为未优化、关闭调试符号的本地构建，保留断言、默认测试并发及 16 MiB 测试线程栈。

GUI 与面板使用同一个新二进制，分别 12.70 秒、53.44 秒通过原契约检查；消费者未设置 RUST_MIN_STACK，原 30/120 秒限额不变。GUI 的实际运行实例退出 0，无 terminate/kill；面板在同一个 PID/core/revision 2 下跨三个发布进程保留 TCP/UDP。消费者源码未改，公开配置、API、Engine、FFI、gRPC、根应用和 Cargo.lock 对检查点起点无差异。

这些是提交前源码及产物的验收记录，二进制构建信息仍指向当时的 base；不把它标为提交后重新构建的产物或新提交的远端 CI。各旧批次中的“未提交/未推送”保留当时语境。

历史原始输出已从工作树清理，完整字节可从检查点 `8acd1cca` 恢复，详见[历史记录](README.md#历史记录)。后续原始日志写入已忽略的 docs/ 或系统临时目录。

10 月 26 日续推进补齐原生 TCP 打开失败的实际清理、类型化关闭凭据及既有后备查询准入，并将 VLESS reverse UDP 与 Packet/Flow 的逻辑会话从 Any 恢复链迁到显式类型直接分发；随后将 SOCKS5 UDP 关联迁到标准 Datagram 角色直接执行，删除专用 handler SPI 与 opaque resume 探测，重开等待保留的原控制器完整关闭回执。该 Datagram 批次完整测试为 **3842/0/175、155 个目标、结束码 0**。之后 TCP 批次于 10 月 27 日完成 **3848/0/175、155 个目标、结束码 0**，最终源码指纹一致；失败/中止记录和当前消费者缺口见[当前状态](README.md)。本轮未提交、未推送；下表 Z003-01、Z003-03 与 Z003-06 均未整体关闭。

## 剩余功能缺口和迁移债务

本批已调整候选 bind reservation 的原始关闭与恢复顺序，补齐 SS pending readiness 的真实 Child 退出监督，并删除 HY2 source-relative claim 包装；最终完整工作区 **3854/0/175、155 个目标、结束码 0**，原失败目标及原始关闭/真实 Child 回归通过，关闭这两个具体分支缺陷。Z003-04 / Z003-05 的其他原生迁移与资格仍未关闭，上一批失败保留历史记录。详见[当前增量](README.md#候选关闭优先恢复ss-启动监督与-hy2-tcp-收敛2026-10-27)。

上一 TCP 批次已推进：混合链直接选用最后一个未迁移节点之后的普通 Stream 后缀，不再要求这些标准节点提供旧 TCP claimer 或整链 claim；未迁移池化节点及其更短前缀仍由现有兼容 owner 承担，不能借出请求流作为长期载体。标准 hop 使用实际代际弱引用并在前缀 I/O 前取得宿主许可；普通 socket 与 Direct 的重复 claim 包装已收敛。该范围的最终全量测试已通过，尚不计整体完成；该旧批次 GUI 调试产物曾自校验超时，结果按对应二进制的历史语境保留。TCP 的 source-relative/池化旧接口和 UDP/device TCP 前缀仍有调用者，Z003-06 继续保持未关闭。

10 月 27 日再收敛实际 Mieru 池化 TCP：删除 adapter 专用 prepared relay 与 relay claim 闭包，接入现有 socket 执行器；socket/transport 的 connect 与 relay 分别共用一个拥有实际协议对象的操作，移除 TCP prepared transport 和执行时 borrowed 包装。实际池、prefix 身份、代际和单次 lazy carrier 保持，错误保留 typed 原因。该批验证见 [README](README.md#池化-tcp-执行与剩余包装收敛2026-10-27)，这不关闭普通 Mieru native 或 Z003-02 / Z003-06。

| 编号 | 优先级 / 原计划 | 当前缺口与影响 | 关闭条件 |
| --- | --- | --- | --- |
| Z003-01 | P1 / PR08A，关联 PR04B、PR05 | 原生 TCP 打开失败的关闭与既有后备准入已补齐；OS/system resolver、detour、其余打开失败重试与既有池化行为仍未闭合。内部原生路径在缺少授权 I/O 前拒绝，普通 Mieru native 生产登记仍关闭。 | 实际查询/载体所有者、TaskScope、原始 main/probe/driver 关闭凭据及依赖关系完整；取消、网络切换、配置替换和失败重试均通过，确认旧原始资源结束后再准入新资源。 |
| Z003-02 | P1 / PR04B、PR06B | VLESS/VMess 等 gRPC/H2/MUX 及 Hysteria2/其他 QUIC 的实际池化生产入口仍有迁移缺口。2026-10-28 已接通 VLESS/VMess/Trojan literal TCP/TLS/WS MUX 的同一实际 adapter map、roleless 原始 owner、实际 TaskScope driver 及标准 Stream，并通过 6 条真实 Proxy 定向回归；普通入口仍未启用。2026-10-29 补充同池标准 Datagram 角色、原始逻辑 control 归还及有界 FIFO/关闭位置，10 项实际 Proxy MUX 定向回归及冻结全量 3870/0/175 均通过。最新普通 VLESS（显式同并发 XUDP）/VMess/Trojan MUX 首跳 → gRPC UDP 子集已接通实际 Stream 借用、原件转交及生产上限配置；MUX 本身整条标准业务 Datagram 派发/计量、复杂 prefix、hostname DNS 和其余载体/生产预算仍未闭合，不能把受控角色子集记为整条生产路径完成。本批 VMess gRPC 非 MUX UDP（含合格标准 Stream 前缀 relay）已独立接通生产 Datagram，使用每 flow 的实际 H2/native 所有者；这不关闭 MUX 或其余 gRPC/池化路径。 | 将实际共享池、endpoint、原始 I/O、驱动及转交用户纳入同一宿主所有权；通过原固定上游基线互通和故障/替换回归后登记标准角色。 |
| Z003-03 | P1 / PR05A、PR05B | SOCKS5 UDP 关联已使用标准 Datagram 角色的显式代际引用和通用会话执行，移除 upstream handler SPI 与 Any 恢复链；普通 VMess TCP/TLS/WS 及本批 gRPC 非 MUX UDP（含合格标准 Stream 前缀 relay）亦已接通每 flow 标准 Datagram 和实际派发/计量，保留会话上游投影，并在重开前等待原会话控制器的完整关闭回执；双原始 provider 及剩余 UDP/packet-path 仍未闭合。 | 使用实际可执行 Datagram/Packet 会话，覆盖多目标、关联控制、单接收者、背压、发送投递状态、逻辑归还及原始关闭，保留正确计量。 |
| Z003-04 | P1 / PR09 | WireGuard deferred device、真实设备替换、Packet/TUN 回程和系统恢复尚未取得完整资格。现有 raw-IP/设备支持不等于完整新宿主迁移。上一 TCP 批次曾出现端口迁移失败后旧监听恢复失败；本批将失败候选原生 reservation 的确认清理移到资源恢复之前，原 Noise/重放/认证来源断言、真实关闭/Scope 回归与最终完整工作区通过，该具体恢复缺陷已修复。 | 接通实际设备和原控制器，验证密钥/peer 安全状态、回程、源地址、MTU、替换与失败恢复；确认旧设备/任务/I/O 和自有系统状态收尾后再启动新设备。 |
| Z003-05 | P1 / PR04B、PR07B | SS SIP003 已有实际 Child/readiness/monitor 所有者，但直接子进程被 reaped 仍不足以证明本次实际拥有的进程与转交 I/O 都结束。上一 TCP 批次启动失败用例曾意外返回 lease；本批补齐挂起 TCP readiness 时的真实 Child 退出监督，并让失败夹具以实际退出事实验证同一错误/Child 保留，前后对照、插件组与最终完整工作区通过。此退出监督分支已修复，不等于插件完整原生进程和 I/O 资格完成。 | 明确本次实际拥有的进程与转交 I/O 的关闭范围，不扩展为通用进程沙箱，使用原控制器和真实退出证据接入宿主事务；未资格路径保持兼容所有权。 |
| Z003-06 | P1 / PR03–PR06、PR11 | 2026-10-31 已完整删除 TCP claim、UDP flow claim、packet-path 二次 claim 与 managed UDP handler provider 四个接口族，直接提供原有执行操作；这不等于余下 carrier 已接入标准角色。本批 managed UDP 的 handler/Any/resume 动态执行链及调用者也已完整删除。剩余 Packet/device、接入契约及未迁资源的标准角色/宿主归属仍未关闭；不能把具体协议 wire 构造或真实兼容 I/O 记为已全面迁入 native 标准角色。逻辑 UDP 会话已改为显式类型登记、发送与响应读取，移除了该路径的 Any 包装和两个 downcast；已覆盖 VLESS reverse UDP 与 Packet/Flow 转换；本批另移除 SOCKS5 upstream handler/provider、构造与扫描恢复链，标准关联改为直接分发。HY2 source-relative TCP 已删除 claim wrapper/准备闭包，保留原认证池；本轮 VLESS/VMess/Trojan 再删除专用 transport TCP claim wrapper 和构造闭包，由已有通用操作直接持有协议冷准备结果/leaf，并隔离同一实际 MUX map 中的 owner 与完整 transport 配置。上一批范围仍是兼容执行收敛；2026-10-28 的受控 MUX TCP 子集另接入标准角色，实际宿主配置替换在旧用户及原始关闭确认前保持等待，历史角色不能绑定新池。2026-10-29 普通 SOCKS5 UDP Flow 另直接选择标准角色，删除剩余 claim/prepared operation 与启动请求包装，单节点、组和 fallback 共用该生产入口；该路径的业务、两组边界守卫及最终严格检查已通过；全量与移位后的定向复验分别记录于 README。普通 VMess TCP/TLS/WS 和本批 gRPC 非 MUX UDP（含合格标准 Stream 前缀 relay）的生产派发已迁到标准 Datagram，对应直接旧 preparation 不再执行；TCP、未合格 relay 前缀和未迁载体仍保留真实兼容调用者。其余普通池化入口、载体、TCP 前缀和 UDP 兼容路径仍有调用者；本批最终验证状态见 README。 | 调用者迁到已构造的标准角色和协议拥有的可执行会话，回归通过后删除对应旧桥；同类协议接入只修改配置转换、协议实现、注册和必要测试，不要求通用运行时识别协议私有状态。 |
| Z003-09 | P1 / PR07B，M4 / G5 | 宿主已有单一配置操作 owner、串行锁和最终确认，但 `crates/engine/src/runtime/configuration.rs` 仍发起配置文件持久化及路由/计划编译，宿主准备/提交职责未全部迁出。Engine 领域快照的安装本身应继续保留，不列作需删除的功能。 | 宿主构建/事务接管资源准备、持久化与跨组件更新；Engine 保留领域快照和 revision，以及中立计划的领域构造。所有现有控制入口保持一次最终确认，持久/临时配置、准备失败、恢复失败和重试语义回归通过。不能为搬职责新增第二个内核或破坏公开 facade。 |
| Z003-10 | P1 / PR08B，M5 / G6 | `crates/config/src/compile.rs` 仍直接读取 GeoIP，并调用规则资源编译；config 的结构/引用验证与宿主运行资源准备尚未彻底分离。加载在构建/重载发生，不是逐业务会话读取。 | 资源 I/O 与运行构建进入宿主准备阶段，config 保留 ADT 与结构/引用/协议委托验证；向 Engine 提供已准备规则和中立计划。路由、bypass、DNS dispatch、资源路径与错误语义保持，配置事务失败不提前发布或启动替代资源。 |

Z003-09 / Z003-10 是对原方案的漏项补记，不是新增重构范围。此前 Z003-01 至 Z003-06 集中列协议资源，不能据此声称其他层已经完成。G8 内部控制/观测投影和 G9/G10 完整组合仍需按原任务卡逐项对账；公开 schema 未改和消费者子集通过并不自动关闭这些目标。

P1 表示阻断对应原生生产启用或原计划阶段关闭；不是对当前兼容执行路径已发生线上故障的断言。证据和协议覆盖明细见[迁移状态表](runtime-extraction.md#剩余协议迁移顺序与阻断条件)、[原生 DNS](native-dns.md)及[DoQ](native-doq.md)。仍在使用的旧接口可在[capability](../../../crates/proxy/src/protocol_registry/capability.rs)和[实际 UDP 连接登记](../../../crates/proxy/src/runtime/udp_flow/registered/state/model.rs)核对。

## 尚未完成的资源配置、验收与交付

| 编号 | 优先级 / 原计划 | 缺少的证据 | 完成标准 |
| --- | --- | --- | --- |
| Z003-07 | P1 / PR01、PR07B、PR08A、PR11 | 生产可配置资源预算与当前最终实现的 1 CPU/1 GiB 完整压力/性能资格。内部 DNS 的 16 查询/16 原始额度、DoQ 最多 8 producer 组属于已测内部上限；本轮 native MUX UDP 的 8 数据帧/32 关闭位置及字节上限亦为内部保守预算，尚未接入生产配置管理。 | 将适用上限纳入既定配置管理；实测稳态、突发、耗尽、空闲回收、网络/配置切换、失败恢复和退出。记录内存、FD、任务、吞吐和延迟；有效安全历史与活跃业务不能被通用淘汰规则清掉。 |
| Z003-08 | P1 / PR06B、PR09–PR11，G0–G10 | 当前检查点的完整官方工具互通、跨平台/特权 TUN、Packet 模式及原生业务/fallback/MUX/故障/计量组合矩阵。175 个忽略项不计已运行通过。 | 在相应环境执行并记录精确源码、工具/协议基线及原始结果；补齐真实 GUI/TUN 与面板所需场景，完成远端 CI、整体任务卡对账及最终旧桥清理。 |

Z003-07 包含生产配置实现与后续压力资格两部分。本批已为生产 CA 文件快照接通可配置字节上限，并为合格普通 VMess MUX 首跳接通按需物理池/任务上限（默认 4，可选配置 1–4096），其余原生资源、任务与连接上限的生产配置及 1 CPU/1 GiB 资格仍未关闭。当前 `crates/proxy/src/component/native_dns.rs` 的 `LIMIT = 16` 是内部限制，不能视为已经完成适用资源上限的生产可配置化；沿用既定配置管理实现，不新增通用资源调度框架。

Z003-08 的历史 GUI 超时不得移作当前失败：较早未优化调试镜像在原 30 秒 `--version` 期限失败，诊断的 40.64 秒 SHA-256 计算不算验收；其后 2026-10-27 的同一新镜像已通过冻结 GUI（19.21 秒）和面板（60.36 秒）子集，见 README。上述结果都属于各自源码批次。2026-10-28 新增 MUX 实现已取得本批最终全量 3864/0/175（155 个顶层目标、结束码 0）及同镜像冻结 GUI 20.04 秒、面板 63.29 秒通过，具体源码/二进制和范围见 README；2026-10-29 的标准 Datagram 共池增量另通过冻结全量 3870/0/175，以及同一新镜像 GUI 20.40 秒、面板 73.77 秒子集；前一轮零预算监督测试时序失败和修正过程保留在 README，不沿用旧源码结果。这仍不等同真实 GUI/TUN、完整上游互通或 1 CPU/1 GiB 资格。

本地 commit、推送、远端 CI、整体架构完成、发布和部署分别记录。`8acd1cca` 为已有本地检查点，2026-10-28 的 MUX 迁移已分组保存至本地 `91b3a2a7`，工程规则与记录随文档提交保存，未推送；普通生产登记不因提交或本地测试通过而开启。原计划与验收标准仍见[17 张任务卡](../task-cards.md)和[交付规则](../delivery-policy.md)。

## 后续顺序

以架构债务为交付单位，优先关闭 Z003-06 的完整旧接口族：先列出全部实现与生产/测试调用者，逐一迁入已有执行契约，删除接口及包装，最后用源码守卫确认不可恢复。每批必须明确删除边界与保留执行边界，不再以逐个增加协议/载体/前缀组合作为里程碑。已关闭 TCP claim、UDP flow claim、packet-path 二次 claim、handler provider，以及完整 managed UDP handler/Any/resume 执行职责域、本批 Packet claim / Registry 设备生命周期 SPI。接下来应处理接入契约、Packet 标准角色与明确保留的设备/载体原始所有权债务、其余未迁原生资源归属，以及 Engine/Config 职责迁出；不得继续把已删除的 UDP 恢复链列为未完成。保留的 TCP connect/relay 执行操作是现有执行边界，不把它们改名成新的协议准备接口。

后续继续收敛上述接口族，并交付 Z003-09（Engine 的宿主准备/持久化职责迁出）和 Z003-10（Config 的资源 I/O 迁出）。针对每类债务覆盖现有调用者的必要业务、失败与关闭回归；不为验收扩大能力或引入替代 SPI。确实涉及资源替换时仍须确认旧原始资源清理后才能构造/启动替代资源。Z003-07 的适用预算随实际所有者补齐，Z003-08 的完整资格矩阵保留为最终交付条件，不用局部通过数折算整体完成率。

只改内核；客户端、面板兼容是约束，涉及其他项目的修改仍由用户决定。本地 develop/v0.0.3，除非另获授权，不提交、不推送。

# 当前实施状态

2026-11-03 云端接续：C 在单环境协调 A/B 独立 worktree，推进 Config 资源准备与 Engine/宿主事务、Hysteria2 既有出站共池及实际原件收尾。[执行账本](../cloud/execution.md)记录职责和删除边界；精确候选及最终检查以 [PR #127](https://github.com/zerodenet/core/pull/127) 的 C 记录为准。下文是各自精确源码的历史批次，不跨 head 沿用结果，ALL17 / G0–G10 仍未整体完成。

2026-11-03：用户授权将当前本地增量分组保存并推送独立 [云端基线和任务包](../cloud/README.md)，准备两个实现 agent 与一个集成 agent。源码分组与历史验证边界见 [基线清单](../cloud/baseline.json)。本地 develop 不移动；云端不得合入 develop/main，最终候选拉回本地验收。本页各批次的“未提交/未推送”保留当时状态。

版本 v0.0.3，分支 develop。云端 PR #124 的合入点是 `b963c864`；后续本地检查点为 `8acd1cca`。ALL17 / G0–G10 整体尚未完成，普通 Mieru native background 生产登记保持关闭。

截至 `f1bf1b6f` 的上一批本地变动已按职责分组提交到 develop。2026-10-28 的 MUX 原始所有权迁移亦已按基础设施 `b1fd79a8`、协议实现 `12133be4`、Proxy 装配与回归 `91b3a2a7` 分组提交；工程规则与本记录随文档提交保存。全部仅为本地检查点，未推送。下文各历史批次的“未提交”仅记录其验证时状态，不代表当前 Git 状态。分组提交没有改变已验证的内核源码或扩大生产准入范围。

## 当前实现

- [独立 zero-runtime 与宿主事务](runtime-extraction.md)：同一个宿主拥有目录、任务监督、配置操作和资源代际；旧资源确认清理后才能构造/启动替代资源。
- Mieru 的[共享池](mieru-pool.md)、[完整 profile 与角色](mieru-profiles.md)、[DNS/UDP underlay](mieru-carriers.md)、[prefix](mieru-prefix.md)和[跨 owner 原始转交](cross-owner.md)已分批接线。
- [原生 DNS](native-dns.md)已覆盖 UDP/TCP/DoT/DoH，[DoQ](native-doq.md)已接通实际 endpoint、驱动、用户/视图及原始 main/probe 关闭凭据。
- 剩余功能和验收条件见[清单](outstanding-20261025.md)。迁移仅覆盖既有能力；池化保留现有语义，插件清理覆盖实际拥有的进程/I/O，不扩成通用资源调度或进程沙箱。

## WireGuard 最后旧 listener 与标准 Packet 接入（2026-11-03，本地 develop）

WireGuard 的独立 listener、linked endpoint 双向复用与同一 crypto owner 的恢复接入实际 ComponentFactory/InboundSource/AcceptedRequest::Packet。adapter 只保留配置投影、协议 device 动作映射以及有限 concrete device assembly；握手、认证、AllowedIPs/peer/roaming/replay 仍由 WireGuard 协议实现。原始 UDP 在同一宿主安装后才 bind，标准源保存原始 listener 的弱身份；同一 Scope 的真实 Critical wire task 负责 wire/timer/共享 endpoint 回程，标准 Packet Tx/Rx 把完整 IP 报文交给宿主路由、网络栈与计量，不创建占位业务 SessionId。

原有物理 MTU、TCP MSS、IPv4/IPv6 分片和 peer/Inner/Outer 计量语义保留；设备换代的接收事实触发对应重组状态更新。两条新增 Packet 交接通道均为单包、按需分配；宿主保留同一条待完成写续接并继续排空接收，避免双向背压死锁。接收等待和已排队缓冲跨取消保留，不新增任务或资源调度框架。Ready 分别确认已初始化的 Packet 路由循环和实际原始 UDP receive 的 poll（Pending 或成功 I/O）；单独的消息通道等待或任务登记不构成 Ready。停旧要求真实 wire/子任务 joins、转交视图退休、原始 UDP/子 I/O 的独立凭据；同端口恢复只能在旧 terminal 确认并发布后取得新原件。

linked endpoint 解除关联暴露的响应任务取消误判同步修复：逻辑 flow 消失后，原 JoinSet 尚未消费的真实任务仍保留轻量取消事实，直到原始 join 才回收；不再把预期取消误判为整个 listener 的致命错误。真实 UDP pending-read 回归同时验证取消后逻辑 owner 释放、后续登记和未请求取消的异常；原始 JoinError 身份及失败保留，任务 join 不替代 native close。

删除 WireGuard 的旧 listener capability/ColdListener operation、全局 InboundListenerCapability、PreparedInboundListenerOperation、InboundDefinition 单变体包装、BoundInbound 和 prepare/bind/execute 及 legacy fallback 全链路。PreparedInboundBind 只保留真实标准 recipe 与原始资源声明；same-crypto recovery capsule 在 bind 前安装到实际 owner，具体 Devices 的热更新和依赖重启决策保留，不建立新 listener SPI。

TEMP-WG-OUTER-UDP-01 的既有 outer_udp_proxy/reconnect 兼容行为单独保留，未扩大范围，尚不满足最终无重叠和完整原始 carrier 关闭证明；接口归零不关闭该债务，也不开放普通 native catalog 准入。其他路径继续确认停旧后启新。公开配置/API/events/FFI、客户端和面板对接不变，仍为 v0.0.3 / develop，未提交或推送。最终冻结源码的定向回归为 **96 项通过、0 失败，2 项真实 ICMP socket 用例明确跳过**，另有 **187 项架构守卫通过**；依赖边界、测试布局和格式检查通过。不运行 workspace 全量，不将 listener 职责迁移记作整体重构完成。测试构建仅启用 wireguard,socks5,zero-dns/udp；既有 TUN 路由会使自动夹具地址探测选择与隧道冲突的地址，因此测试显式使用当前物理接口 IPv4，不调整运行中的网络或内核。

## Shadowsocks SIP003/SIP003U 标准接入迁移（2026-11-03，本地 develop）

Shadowsocks 的 TCP-only、UDP-only、TCP+UDP 插件模式整体使用协议自有 ComponentFactory/InboundSource 与 Stream/Datagram。保留 SIP003 环境/参数、既有 cipher/身份/计量/半关闭语义和 SIP003U 无 UDP readiness 握手的行为；没有扩大协议组合。薄 adapter 只投影配置，生产登记不再回退到旧 listener。

标准资源声明 ListenerHandoff 将公网端口保留原件、同地址族回环 TCP/UDP 和原始 readiness TCP 控制器归到同一个宿主。公网 TCP 原始 close report 确认后才启动插件；真实 Child 通过有限 start-only grant 同步安装到持久 owner，在同一 Scope 注册 Critical 监控。取消或失败启动保留实际 Child、原始 I/O 与未完成状态，真实 reaping、I/O 返回/原生关闭和 Scope join 全部完成后才允许替换。readiness 不分配业务 SessionId，不通过未拥有的 UDP route probe 开额外 I/O。此项证明直接 Child 和宿主实际拥有的 I/O，不宣称进程后代容器，也没有开放普通 native Process catalog。

TCP2022 的实际 acceptor replay pool 复用现有 replay Owner，维护 worker 进入同一真实 Scope；taskless 工厂仍获得 without_tasks 视图。完整冷凭据快照可重复 Build，热更新不会污染回滚配方。删除 Shadowsocks InboundListenerCapability、adapter 旧 inbound 桥、InboundCarrierPlan/InboundCarrier、ShadowsocksInboundPluginPlan、TcpAndDatagramInboundListenerOperation、旧 TCP listener executor 和 InboundConnectionContext 全树。

最终必要定向 **10 项行为回归通过、0 项失败**（含三种模式的真实 TCP/UDP 生产路径，以及 pending readiness 取消、Child 异常退出、原始清理和同端口替换），**186 项架构守卫通过**。依赖边界和测试布局检查通过；未运行全量。公开配置/API/events/FFI 及客户端面板工作流不变，版本/分支仍为 v0.0.3 / develop，未提交、未推送。此为 Shadowsocks 批次的历史状态，最后 WireGuard 的后续迁移见本页最新记录；TEMP-WG-OUTER-UDP-01 和其他未完成架构域仍单列，不把本批算成整个重构完成。

## Hysteria2 标准接入迁移（2026-11-03，本地 develop）

Hysteria2 既有 QUIC TCP/UDP、H3 与 legacy ALPN 认证、Salamander、带宽协商及可选 HTTP/HTTPS 伪装站点整体接入协议自有 ComponentFactory/InboundSource。TCP/UDP 通过现有 Multiplex/Stream/Datagram 交付标准派发和计量；H3 分类/请求与 HTTP/H2 worker 使用实际 accepted Scope，文件和 TCP/Unix 回源使用宿主现有 FileTree/InboundTargets 原始 provider。主 QUIC 保留原字面地址要求，可选站点保留域名解析和 HTTP/TLS/H2/跳转语义。冷准备不发布凭据；真实 Build 才更新同一 profile authority，冷凭据快照与可热更新的实际 authority 分离，支持按原配方恢复。同步拒绝的 QUIC Initial/ALPN 只终止该连接，保持原 endpoint 驱动以送达拒绝和接纳后续合法客户端。

删除 Hysteria2 的 InboundListenerCapability、adapter listener.rs、独占 authenticated-QUIC / group / multiplex operation，以及已无生产调用者的 proxy-local QUIC 旧 accept loop 和 BoundInbound::Quic/Group。有限额外 TCP listener 的每个原始控制器、真实解析任务和 accepted receiver 均归同一组件宿主；部分绑定失败也必须确认全部旧原件关闭才能恢复或替换，不以 Drop/端口可用代替。没有新增替代 SPI、协议组合或普通 native 出站登记。该批次后剩余 WireGuard，后续迁移见本页最新记录；PR06A 全部验收/整体架构尚未完成；WireGuard 临时债务单列不变。

公开配置/API/events/FFI 与客户端面板工作流未调整；本批仅修改内核，未提交或推送。最终必要定向回归 **16 项通过、0 项失败**：真实宿主/原始关闭与同端口替换 5 项、既有 Hysteria2 生产集成 9 项、IPv6 QUIC 1 项、冷凭据恢复 1 项。186 项架构守卫通过；依赖边界、测试布局及格式检查通过。未运行全量测试。首轮发现的同步 ALPN 拒绝导致 listener 停止已修复；新增 fixture 的共享组件启动和合法站点绑定/HTTPS 配置已纠正。最终行为二进制对应冻结的内核源码；旧的失败日志仅保留为修复过程证据。

## 对照原方案的整体进度（截至 2026-11-01，按批次更新）

原 Page revision 7 的 M0–M6 / G0–G10 是完成标准；17 张任务卡是逻辑交付边界，旧的“未开始”和检查点状态不代表当前代码进度。现在是基础骨架已落地、多条生产路径已迁移，但协议与其他层的职责迁移仍在进行，不能记为“架构完成、仅剩验收”。

当前接入迁移：HTTP、SOCKS5/mixed 与内置 Direct 的实际标准 source 已接通。Direct TCP-only 以及既有同端口 TCP/UDP 已整体迁到 ComponentFactory / InboundSource / Stream / Datagram，覆盖 literal、hostname、空包和独立客户端；删除 Direct 的 InboundListenerCapability、旧 binder/preparer、inbound/direct 及 tcp_and_peer_datagram 执行链。UDP 使用 TCP 实际选择的端点和预先保留的原始 owner，peer 以原始 listener 弱身份和真实 facts 交付同一个宿主 Scope，不伪造 TCP caller，不新增替代 SPI 或协议组合。每 peer 在握手前保留实际 Datagram cleanup owner，确认任务 joins、Direct 原始 fd、真实控制器归还后才释放再接入。Trojan 的 TCP/UDP/MUX 已接入协议自有 ComponentFactory / InboundSource / AcceptedRequest，删除该 adapter 的旧 listener 桥和 InboundListenerCapability 实现；最终生产回归状态见下节。VMess 本批已接通同一标准接入契约的 TCP/UDP/MUX，实际原件与 gRPC/MUX Scope 回归见下文。VLESS 的合法既有输入现均走标准 source，旧 listener 和对应执行链已删除；最后 WireGuard 已在后续批次迁入标准 Packet，InboundListenerCapability 与旧执行链整体删除；临时 outer-UDP 债务及其他架构域仍未关闭。

Trojan 本批迁移覆盖既有 TCP、UDP 和 MUX 全部 listener 分支，不把旧 operation 包进新工厂。协议拥有真实 TLS/WS/gRPC 握手、认证、UDP 编解码及 MUX reader/writer；宿主消费标准请求，负责身份、路由、计量、认证连接取消与原始 Scope。配置目录仅在冷准备时借用，保持相对 TLS 证书路径；profile 冷准备不发布，实际 host Build 才更新同一原 profile。旧 `adapters/trojan/listener{,/profile}.rs` 已删除。

MUX 的逻辑 UDP 原件保留在原 accepted 条目：有限 ordinal 只在真实 driver Scope join 和原始 provider 清理完成后复用；等待方取消后由同一个宿主 observer 接续清理，不增加任务 owner、实例或每会话历史表。连接认证和载体取消通过既有 RequestMultiplexer 交付，达到并发上限时仍可观察原连接关闭。成功凭据按有限 ordinal 聚合；逻辑 TCP 的实际 caller join 同样触发同一 observer 对其已登记 carrier 原槽的确认清理，不必等整个 MUX 连接退出。该清理与最终 shutdown 串行消费真实凭据，不关闭仍活动的其他 caller。显式 wire END 保留旧双向结束语义，普通发送方 EOF 保留反向半关闭；标准 Stream 的 write 返回有效载荷长度，不能返回整帧线长度。逻辑 EOF、Drop、任务 body 退出或端口可重绑都不作为 native close。

本批最终 **82 项必要行为回归 + 184 项源码架构守卫通过，0 项未解决失败**：平台原始所有权/取消 13 项，实际宿主/原件关闭/停旧替换/恢复与冷 profile 22 项，共享 HTTP/SOCKS5/mixed/Direct 及 Trojan WS/gRPC 38 项，Trojan 入站 TCP/UDP/MUX 6 项，逻辑 Stream 3 项。真实 TLS/MUX 连接在 TCP 并发下连续完成 300 条 UDP 逻辑流（超过有限资源槽数），并验证最后 TCP END 的真实上游 EOF；该 EOF 是行为回归，native closure 仍由实际原槽/controller/Scope 的独立凭据确认。保留公开 `trojan` 协议名、TCP payload/UDP 原计量边界、256 条完成历史上限、热凭据更新和相对 TLS 证书路径。没有重跑 workspace 全量。

截至 Trojan 批次是原 7 个 adapter 中 **1 个完整职责域的迁移**；本次 VMess 批次后为 **2 个，仍余 5 个**。其余 Hysteria2、Mieru、Shadowsocks、VLESS、WireGuard adapter 尚未迁移，G5/G7 和整体 M1–M6 仍未完成。WireGuard `TEMP-WG-OUTER-UDP-01` 不变；普通 Mieru/native MUX 出站登记不扩大。只改本内核仓库，客户端/面板及公开配置/API/events/FFI 不变；本批未暂存、提交或推送。

VMess 本批覆盖既有 TLS/WS/gRPC、TCP cipher、UDP packet/raw 和 MUX。协议自有 Factory 一次消费已冷准备的用户/transport material，实际 Build 才发布到原 adapter runtime 的同一 profile/replay authority；相对证书目录和热凭据更新保留。UDP TX/RX 共享各自实际通道的同一 packet/raw 状态，MUX 各逻辑流独立持有；不会因响应侧复制旧状态而改变线格式。原 adapter listener、InboundListenerCapability、`VmessInboundAcceptedRoute` / `accept_standard_route` 中转已删除；库的显式公开 Session API 与生产 source 分离。

核对真实任务归属后补齐通用 gRPC carrier：VMess/Trojan 标准 source 的实际 H2 连接、keepalive、hunk read/write futures 由原 accepted Scope 中的一个真实任务驱动，保持原队列、服务名匹配和半关闭语义；不再经过旧 detached spawn 入口。取消只是停止请求，原件关闭仍等待该 Scope join 与实际原始 TCP provider。任务/流有有限上限并按需创建，不建立第二个宿主、pool 或任务登记表。独立 VMess/Trojan feature 明确包含既有 Datagram 宿主运行时，避免默认功能集合掩盖所有权缺失。

VMess 批次最终 **28 项必要定向行为回归 + 186 项源码架构守卫通过，0 项未解决失败**：VMess 生产路径 13 项、Trojan 共享 carrier 回归 6 项、仅启用 `vmess` 的真实宿主/冷 profile/停旧替换/恢复 9 项。实际 TLS 和 gRPC 分别从活动 UDP 换代到新 source 的 TCP/UDP MUX，旧 owner 的原始 provider 关闭与实际 joins 在替换 ACK 前确认；新 MUX 的 Scope 分别消费真实 7/8 个任务 join（gRPC 多一个承载 H2/keepalive/hunk I/O 的原始任务），原始 listener/accepted TCP/出站 TCP/Direct UDP 由真实 provider 关闭。这里没有把端口可重绑、Drop 或任务 body 返回当关闭凭据。另覆盖 packet/raw 模式固定、多目标/空 UDP、相对 TLS 路径、公开 `vmess` 协议名与 payload 计量，以及并发 TCP 下连续 300 条 MUX UDP 逻辑流的有限原件槽复用。未重跑 workspace 全量。本批不扩大普通 Mieru/native MUX 出站登记或协议组合，公开配置/API/events/FFI、客户端和面板不变，WireGuard 既有临时债务单列保留。

实际换代回归发现并修正了受控 TCP carrier 的旧缺口：用户视图归还后，原始出站 socket 原先可能留在 owner 内等待下次拨号回收。真实 business issuer 现在把已授权原槽及其弱 owner 身份交给同一 accepted 图；记录数量受原始物理 owner 的真实有限容量约束，不另建 socket/task 池。旧入口撤销后，宿主在实际 caller/driver join、用户归还和同槽原始 native 关闭凭据齐全前不确认清理。失败/中断保留原槽和原始来源，成功凭据聚合为有限类别；不把 Drop、客户端 EOF 或端口重绑当 native close。

共享出站资源的 caller 失败归类同步纠正：实际 Auxiliary 业务任务的原错误仍留在 caller 诊断与接入 Scope，不提升为共享 native 故障；实际 Critical worker 和 native/Unknown 错误不受此豁免。

这批只推进既有接入和宿主职责，不增加协议组合、第二实例或替代 SPI。实际冷选择先检查工厂对完整输入的资格；已选标准 source 的准备、绑定或启动错误不得回退旧 binder。旧资源清理/Scope join 未确认时继续阻止启新。客户端、面板及公开配置/API/events/FFI 不变，WireGuard 临时债务仍单独保留。后续实现缺口仍以本节 M1–M6 和 outstanding 清单为准，不把已有定向测试数量换算为整体完成百分比。

2026-11-01 Direct 整体迁移最终 **93 项必要定向回归与 184 项架构边界检查通过，0 项未解决失败**。其中 Direct 生产路径 11 项、标准通道/接入契约 20 项、HTTP/mixed 与 SOCKS5 UDP 27 项、实际宿主原始资源/peer Scope/登记/监听地址/停旧/恢复 35 项。真实同一 owner 的 source 与 peer 共两个原始任务完成 join，TCP listener、UDP listener 与 peer Direct egress 的三份原始关闭凭据分别确认；不以端口探测替代关闭证据。两项恢复夹具由旧的先 bind 后入配置改为实际标准装配、真实 readiness 后收发，原故障注入、取消、原始 native close 和回滚断言保留并通过。`zero-proxy` 的 all-features 与 no-default-features 库检查、格式和测试布局检查通过；未重跑全量、未开启其他协议登记或修改客户端/面板。本地 develop，未暂存、提交或推送。

上一批最终 93 项必要行为/契约回归与 182 项架构边界检查通过：Direct 真实收发/换代、HTTP/mixed、SOCKS5 UDP、原始资源与 Scope 监督、停旧/回滚、完整输入工厂选择、IPv6 原始监听及真实 caller 错误归类。关闭所有可选协议的构建通过。没有重跑全量、开启新的 native 登记或进行客户端/面板实机与 1 CPU/1 GiB 平台验收。验证产物仅在仓库外保存；本地 develop，未暂存、提交或推送。这些结果只关闭本批具体迁移/缺陷，不关闭下表剩余职责。

| 原阶段 | 当前进展 | 未关闭的条件 |
| --- | --- | --- |
| M0 / G0 基线 | 版本化完整方案、任务卡、固定消费者与部分行为基线 | 完整资源/性能与平台基线仍有缺项；旧数字不能替代最终源码的比较 |
| M1 / G1 中立契约 | 公共组件、角色、通道、网络、错误与生命周期契约已有实际调用；traits/core 保持 no_std；旧 listener 接口已整体删除 | 出站兼容角色/资源仍有生产调用，契约存在不等于所有协议已迁完 |
| M2 / G2–G3 实例与出站 | 实际工厂、代际、依赖和普通 Stream 路径；Engine 目标解析返回中立身份；混合 TCP 已部分收敛，受控 MUX TCP/TLS/WS 子集已接入实际宿主，标准 Stream / Datagram 共用实际池及驱动 | 剩余池化、复杂载体与部分 TCP/UDP 前缀仍走兼容路径；VLESS/VMess/Trojan MUX 首跳仅在下述合格 gRPC relay 子集开启；其余 MUX/Mieru 普通 native 登记未开启 |
| M3 / G3–G4 UDP 与接入 | 逻辑 UDP 显式会话；普通 SOCKS5 UDP 与合格 VMess 非 MUX UDP 生产路径已有标准 Datagram；所有旧 listener 调用者已迁移，WireGuard 入站使用标准 Packet；managed UDP handler/Any/resume、Packet claim 与 Registry 设备生命周期 SPI 已删除 | 剩余出站/载体标准角色、未资格设备的原始所有权及其余资源的宿主归属继续开放；不能重新把 listener 旧链列作待迁移 |
| M4 / G5 宿主与事务 | 独立 RuntimeHost、配置操作所有权、任务监督、资源代际，以及已接管资源的确认停旧替换事务 | Engine 仍发起路由/计划编译与文件持久化，宿主准备/提交职责未全部迁出；领域快照安装与 revision 仍应由 Engine 保留 |
| M5 / G6–G7 基础服务与 Packet | DNS 原始查询 owner、Network 契约、公共 Flow/Packet 转换与设备接线已有覆盖 | system/detour/池化 DNS、config 资源加载、完整设备替换和系统恢复仍有实际实现缺口 |
| M6 / G8–G10 投影与收尾 | 公开配置/API/事件兼容边界保留；已有多批内核全量及冻结消费者检查，结果按下方对应源码批次记录 | 内部观测/控制投影整体对账、旧桥删除、其余生产资源上限配置、完整互通/特权平台、真实 GUI/TUN、1 CPU/1 GiB 和最终交付未齐 |

需要纠正先前剩余清单的范围：协议资源之外，`crates/config/src/compile.rs` 仍读取 GeoIP 并准备规则资源，`crates/engine/src/runtime/configuration.rs` 仍编译与持久化配置。这对应原 PR08B 和 PR07B，是实际职责迁移债务；宿主已有串行事务不能代替这些迁出条件。规则加载发生在构建/重载，不能误写为逐连接加载。详见清单 Z003-09 / Z003-10。

Z003-07 也包含实现工作：例如 `component/native_dns.rs` 的查询/原始资源上限仍为内部常量 16，适用上限尚需接入既定生产配置管理，再做 1 CPU/1 GiB 资格；不能全部归为“只待验收”。按用户决策保持保守默认和按需分配，不为配置上限再增加通用资源管理框架。

当前传输层没有 config/engine/proxy/具体协议的生产依赖，runtime 对 proxy 的依赖只在测试侧；不能把已解除的依赖重新列作缺陷。原 Page 的 `runtime → proxy` 已按用户批准调整为 `proxy → runtime`，见[修订](runtime-extraction.md)，不是未批准的目标偏移。上述表格不折算任意完成百分比，也不将部分协议迁移或本地测试通过算作 G0–G10 整体关闭。

## VLESS 旧 listener 职责域关闭（2026-11-03，本地增量）

核对合法配置后纠正前批剩余项：`config::validate::protocol` 早已禁止 mKCP 与 REALITY 同时启用，Hysteria 也必须有显式 QUIC TLS 配置。普通 mKCP、mKCP+TLS 与 REALITY 各自已走实际标准 source。前批将无法被合法配置选中的 mKCP/REALITY 旧分支记作待迁生产能力不准确；本批删除该冗余执行链，不实现或开放这个禁止组合。

VLESS 现在仅注册实际 ComponentFactory / InboundSource，不再实现 InboundListenerCapability，也不保留旧资格筛选回退。删除 adapters/vless/listener.rs、accept_mkcp_peer、accept_recorded_mkcp_route，以及仅由它们消费的 PeerRouteInboundListenerOperation、context/transport.rs、context/recorded.rs 和整个 inbound_route/recorded 执行树。标准路径准备/启动失败继续报错，不能回退旧 binder；不新增替代协议 SPI、角色或第二个资源/任务 owner。

直接调用旧 binder 的 IPv6、MUX 前缀和载体回归夹具也改为实际宿主标准 source；原 TCP/UDP 收发、TLS、地址形式和关闭断言保留。新增普通 mKCP、mKCP+TLS、REALITY 各自 TCP/UDP 实际派发、计量与两代同端点替换回归，消费原始 host/controller/Scope 凭据后才启新；客户端 EOF 仅作行为观察。禁止组合、失败准备和旧入口不可达单独回归。最终冻结生产源码通过 **14 项必要定向回归、186 项架构守卫**，格式、diff 与测试布局检查通过，未重跑全量。实际夹具先调用宿主 request_stop，由完整 observer 消费 Scope joins，并驱动 confirm_shutdown 的原始 provider 清理，之后才观察 EOF；通知或 EOF 都不替代原始凭据。首轮夹具遗漏宿主撤销/显式清理顺序，以及缺少 VMess/Trojan TLS 材料的失败均保留，未放宽协议校验或超时。精确源码快照、当前批次差异和原始结果在仓库外 zero-vless-mkcp-reality-20261103/validation-final.json。

当前仍有 WireGuard 共 1 个旧 listener adapter；PR06A 和整体 G0–G10 未关闭。VLESS XICMP RAW wire 资格仍待有权限平台验证，不能由本批旧接口删除代替。WireGuard TEMP-WG-OUTER-UDP-01、普通 Mieru/native MUX 出站准入、Engine/Config 职责和资源配置债务不变。下文历史批次关于 VLESS 旧 listener 的剩余描述由本节取代。

只改内核，本地 develop/v0.0.3；公开配置/API/events/FFI、客户端与面板项目未改。只做必要定向回归，未运行全量，未暂存、提交或推送。

## VLESS Hysteria 文件/反代标准接入（2026-11-03，本地增量）

既有 Hysteria 载体的文件服务、HTTP/HTTPS 反向代理已接入实际 VLESS ComponentFactory / InboundSource 生产装配。协议声明原文件根目录及实际 origin 端点，宿主不解析协议私有配置。继续使用现有资源访问契约、同一个 accepted 图和 TaskScope，没有新 listener SPI、协议组合、宿主实例或连接池。标准 source 一旦选中，失败不得退回旧文件/网络执行路径。

文件服务的真实 std::fs::File 与原生目录控制器由宿主持有，打开前安装实际 cleanup owner；阻塞文件任务的原始 JoinHandle 始终保留在该 owner，取消等待方不会分离线程。必须分别确认实际阻塞任务 join、actor 的原 Scope join、唯一视图归还和匹配原控制器的 opaque native close receipt，才能归还额度并允许替换。任务故障原样留在宿主诊断，随后仍可完成实际原件清理，不把错误变成永久资源泄漏。每 listener 最多 16 个按需文件 session；这只是保守内部上限，不关闭 Z003-07 统一配置债务。普通 catalog 的 FileTree 仍未取得原生资源资格。

反向代理复用已声明的 InboundTargets、真实 DNS/出站 TCP provider；TLS、HTTP/1、H2 和流式 body 工作归同一实际 caller/Scope。不再通过独立 HttpConnector、TokioExecutor 或私有连接池绕过宿主所有权。请求路径、query、Host 策略、转发头、流式收发、文件条件请求/Range/目录行为沿用原实现。文件根目录越界继续返回 404；TLS wire EOF 只用于行为观察，原 TCP 关闭仍由实际 native provider 凭据证明。

已删除 VLESS 旧 H3/Hysteria listener dispatcher、accept_http3_streams、accept_recorded_transport_stream 及其全部调用者，并移除旧 Incoming::Hysteria、QUIC metadata/bind 包装。**VLESS 的 InboundListenerCapability / listener.rs 仍未整体删除**，现仅保留既有 mKCP/REALITY 兼容执行分支。没有显式 QUIC 凭据的 Hysteria 输入原本无法绑定，本批保留清晰错误，不将其算作新增能力或兼容例外。剩余旧 listener adapter 仍为 4；PR06A / ALL17 / G0–G10 未整体完成。下文历史批次所列文件/反代旧路径已由本节取代，其他剩余范围不变。

本批最终验证记录保存在仓库外 zero-hysteria-masquerade-20261103：实际生产装配/两代换代、文件阻塞与真实任务失败、原始文件/目录关闭、HTTP/H2 语义共 28 项必要行为回归，另有 186 项架构守卫；最终状态及测试夹具修订见 validation-final.json。XICMP RAW wire 资格、WireGuard TEMP-WG-OUTER-UDP-01、普通 Mieru/native MUX 出站登记和其余职责域均不因此关闭。Windows 原生目录代码未在本机执行，不计作跨平台验收；格式、diff 与测试布局检查独立记录。只改内核，本地 develop/v0.0.3；公开配置/API/events/FFI、客户端和面板未改，未暂存、提交或推送，未运行全量。

## VLESS Noise/XDNS/XICMP 原始资源链（2026-11-03，本地增量）

在既有标准载体范围内，三类 mask 已接入实际 VLESS ComponentFactory / InboundSource，不再为其走旧 listener。transport 只准备惰性的真实工作 future，原宿主 Scope 与 Metadata 用户/驱动账本登记实际任务；mask 使用独立原始 origin，不冒充 Quinn endpoint/connection，也不建立第二个任务监督器。mKCP XDNS/XICMP 通过真实 Metadata 载体交付 Carrier peer，物理 mask 只执行一次，conversation future 留在原 accepted Scope。历史逻辑 sender 停旧后不能再发包，但保留别名仍阻止原件托管责任结束。

XICMP 原始 socket 由同一实例的宿主 ControlledIcmpOwner 持有，只允许在有限 lifecycle start 阶段取得一个唯一视图。父取消先阻止新分配，实际载体消费者排空后，由这个原始 provider 显式撤销并关闭原件。Quinn 用户/任务先退出，再从内到外停止并 join mask，最后确认 Metadata 视图、用户图和 native provider 凭据；中断或超时保留原 owner。定向回归曾发现提前停 Noise 导致仍在退出的 Quinn driver 得到 BrokenPipe，已按依赖顺序修正，并在最终源码上通过实际两代替换回归；没有吞掉原始 I/O 故障。普通 catalog 的 NativeIcmp 资格仍未开放，不因此扩大其他原生登记或协议组合。

旧 raw QUIC recorded listener 分支及 accept_recorded_quic_route 的全部调用者已删除。VLESS 的 InboundListenerCapability / listener.rs **仍未整体删除**：Hysteria 文件/代理伪装、既有 mKCP/REALITY 兼容分支仍有实际调用。它们不因本批 mask 所有权完成而获得标准资格。剩余旧 listener adapter 仍为 4；PR06A / ALL17 / G0–G10 未整体关闭，WireGuard 临时债务单独保留。下文较早批次关于 mask 的剩余描述以本节为准。

最终冻结源码通过 **31 项必要行为回归、186 项架构守卫**：实际 Proxy 的 Noise / XDNS TCP/UDP 派发、计量和两代资源串行替换，持有原 sender 时阻止清理完成、停旧后禁止发包、实际任务登记失败后的原件回收，以及 XICMP 同 provider 的原始关闭和任务 join。格式、diff 与测试布局检查通过；未重跑全量。**XICMP RAW 收发仍未验收**：本机 RAW socket 返回 EPERM，原有 loopback 互通探测在 ping fallback 下超时；真实 ping-mode 原件/Scope/关闭回归通过，不能替代 RAW 互通。原始日志、源码快照和本批增量保存在仓库外 zero-vless-owned-masks-20261102。只改内核；本批未改公开配置/API/events/FFI，未调整客户端/面板，本地 develop/v0.0.3，未暂存、提交或推送。

## VLESS TCP 载体与 reverse 标准接入（2026-11-02，本地增量）

TCP XHTTP、WS/HTTPUpgrade 的受信 PROXY prelude，以及 reverse 用户已接入原 ComponentFactory / InboundSource / AcceptedRequest 生产选择；本批不新增协议 SPI、配置字段或协议组合。XHTTP 的 HTTP/1 packet-up/stream-one 和 H2 stream-up/stream-one 沿用同一 listener session registry；连接、Hyper H2 执行器、body 和完整包接纳任务登记到原 accepted Scope。HTTP/1 保留先确认完整接收、继续解析后续请求的语义；H2/H3 继续确认排队。已经 poll 的 FIFO reservation 与有限请求/字节 permit 在原 Scope 中转交，不因 HTTP waiter 结束丢失包，也不建立第二个任务所有者或载体池。 HTTP/1 半关闭后，已接收包的任务还持有同一个 source publisher，直到实际队列接纳结束；避免 source primary 先退出并取消其原 Scope 内仍待排队的包。该 publisher 只保留交付寿命，不是 native-close 或 join 凭据。

受信 PROXY v1/v2 使用中立 transport 解析器，在 TLS/载体/认证前执行。既有 HandshakeContext 增加可选的一次性连接事实数据交接，宿主检查物理 original_source 未改变后更新有效来源/本地地址，再执行 Stream/Datagram/MUX/Control 策略、计量和设备限制；原 accepted native entry 与关闭权不变。内部 Rust 构造方补齐该可选字段，公开配置/API/events/FFI 不变。

reverse 继续使用原 PortalRegistry 和协议 MUX；实际控制原任务、物理 driver、UDP 上传和响应泵共用原 accepted Scope。已退役 caller 不能通过历史 Portal 再开放逻辑通道；保留的旧逻辑句柄不等于持有新 native 权限，原始资源仍由宿主独立确认关闭。普通 Mieru/native MUX 出站登记范围不变。

已删除旧 listener 的全部 TCP 执行分支、dispatch_transport、TcpOrQuicInboundListenerOperation、runtime prelude 桥及旧 QUIC first-stream 专用监听/日志包装。未资格 raw QUIC 的首个双向 stream 仍按既有语义，由同一 transport helper 在已有 QuicConnectionInboundListenerOperation 中接收；不另建 listener SPI。VLESS accept_recorded_tcp_route、旧 accept_tcp_route/accept_transport_streams 包装，以及可选 TCP recorded 调度/握手目标兼容拨号的全部调用者和执行链一并删除。标准 TCP 载体必须取得实际 AcceptedCaller，不再保留到 detached gRPC/XHTTP/H2 构造器的隐式分支。

本批最终 **48 项必要定向行为回归、186 项源码架构守卫通过，0 项未解决失败**。实际 Proxy 5 项覆盖 XHTTP 四种既有 HTTP 用法的 TCP/UDP 派发及串行原始关闭、TLS 前受信 prelude、reverse 控制、半关闭后完整上传包跨连接补序与既有 TCP 前缀续接；其余为协议标准 source/旧逻辑句柄 12 项、XHTTP 队列/接收确认 7 项、标准接入原件/取消契约 14 项，以及 QUIC 5 项、共享 XUDP/UDP 原件/目标关闭等 4 项和首 stream 连接存活 1 项。流水线用例先保留缺失序号、确认原上传连接的全部 HTTP 接收响应并半关闭，再由另一个连接补齐，验证 66 个完整包有序到达实际目标；原始 Native provider receipts 与 Scope joins 仍独立确认。早期无效 fixture、类型检查失败及中断构建不计入通过数；完整源码边界、原始日志与增量保存在仓库外 zero-vless-listener-final-20261102。格式和 diff 检查通过。只改内核，本地 develop/v0.0.3，未暂存、提交或推送；未运行全量，不调整客户端/面板项目。

**尚不能删除 VLESS 的 InboundListenerCapability 与 listener.rs。** 本段原列出的 Noise/XDNS/XICMP 原始后台/原生资源迁移见上节；当前剩余包括 Hysteria 文件/反向代理伪装的文件/网络原件和既有 mKCP/REALITY 兼容分支；未资格组合仍使用原有兼容入口，不能通过拒绝既有配置、隐藏回退或虚构关闭凭据来删除接口。剩余旧 listener adapter 数仍为 4。本批不是 PR06A / ALL17 / G0–G10 整体完成，WireGuard 临时债务独立保留。

## VLESS 原生 QUIC 标准接入（2026-11-02，本地增量）

普通 literal QUIC、XHTTP HTTP/3 和 Hysteria 载体已接入原 ComponentFactory / InboundSource / AcceptedRequest 生产链。协议准备原 TLS/ALPN 和传输参数，真实宿主保留原 MetadataDatagram 的 main/probe native owner，沿用同一个 listener TaskScope。传输库的 sealed incoming 在 source.next 中只接收真实连接尝试；TLS 与 VLESS 握手在宿主登记实际 Carrier origin 后执行，不伪造 TCP accepted entry 或业务 SessionId。原 UDP grant 的实际监听端点通过同一宿主保留，接入 facts 必须匹配该原件。物理 caller 与 XUDP 关联复用同一个有限 child group，不另建任务所有者或连接池。

Quinn endpoint/connection driver 使用实际 TaskRegistrar、原 TaskJoinWitness 与唯一 DriverExitReceiver。H3/Hysteria 的认证、前缀分类、逐请求执行和 XHTTP body worker 进入同一实际 caller；结构化请求 future 共享原 driver，不再使用旧 detached JoinSet / tokio::spawn 执行链。XHTTP H3 三种既有 mode 仍归同一协议实现。返回 sealed 连接、stream、H3 请求和原 Metadata views，与实际 driver join/显式接收分开确认；原 provider 验证原始 opaque users receipt 后才能完成 native owner 清理。冷声明和普通目录 metadata 资格不替代实际 Proxy 装配，普通 Mieru/native MUX 出站登记不扩大。

普通配置及无额外 worker/native 资源的 UDP mask 可选择新入口。Noise/XDNS/XICMP、Hysteria 文件/反向代理伪装及其他未资格组合保留原有兼容入口（TCP XHTTP、受信 prelude 和 reverse 的后续迁移见上节）；它们需要各自真实资源或消费者的迁移，不算作已完成，也不是新增停旧例外。VLESS 旧 listener 的共享 QUIC 分支仍有这些调用，本批不能删除整个分支或宣称 VLESS/PR06A 完成。标准 source 一旦选中，失败不得退回旧 binder。公开配置/API/events/FFI 不变，客户端和面板仓库未改。

本批 5 项实际 Proxy 标准装配回归通过：raw QUIC、H3 stream-up/packet-up/stream-one、Hysteria 各自执行真实 TLS/TCP/UDP 收发和两代串行替换，raw QUIC 额外保留 9 条已连接、未提交业务的真实连接，验证共享 poller 额度和空闲握手清理。Quinn 的实际连接各持有自己的写 poller；poller 上限覆盖实际 driver 与 sealed user 的独立保留额度，仍按需分配，不建立常驻池。物理 caller 满额只拒绝本次未启动请求，driver 账本满额则等待原始 join 接收/回收，不继续创建无额度 driver。载体认证没有业务 Session，真实 TCP/UDP 才进入原派发、计量和会话结束链。另有 2 项原始 provider 的启动取消/未使用 grant 退回回归、4 项受公共装配影响的旧路径回归、11 项协议标准接入回归、1 项满 driver 配额时的可选 accept 期限/原件关闭回归与 186 项架构边界检查通过。未使用的原始 grant 必须消费唯一非 Clone RX 和匹配的 TX/claim；仍持有的 TX/poller 会阻断完成，已进入 endpoint 的资源只能走完整 sealed users/driver 原凭据，不能降级到未使用路径。依赖尚未接入的原生资源或消费者的组合仍保留旧登记，不以普通路径通过代替那些组合的资格。本批未跑全量或官方外部互操作，未提交或推送。源码阶段、基线、差异与原始日志保存在仓库外 zero-vless-quic-inbound-20261102/validation-final.json。

## VLESS REALITY/fallback 标准目标接入（2026-11-02，本地增量）

既有合格 REALITY raw TCP 和普通 fallback 输入已接到原 ComponentFactory / InboundSource 生产装配。冷准备由协议声明完整的目标端点清单；宿主只提供这些端点的 InboundTargets 资源访问，不解析协议配置、不新增协议 SPI，也不扩展原配置校验允许的载体组合。普通目录对该声明仍标记为未资格；只有实际 Proxy listener 的原始 accepted provider/Scope 能执行它。目标资源上限为每 listener 按需最多 4096 个原件，统一生产资源上限配置仍单独开放。

每次实际目标操作在 connect/DNS 等待前保留 accepted cleanup owner，并把真实目标 driver 登记在同一个原 TaskScope。TCP 复用原节点 DNS、地址候选与 egress 策略；Unix 在原始 native fd 的持久控制器安装后才执行非阻塞连接和注册。无原件克隆、第二个宿主、连接池或独立任务登记表。失败/取消仍由原 owner 保留；驱动 join、唯一 view 归还和原始控制器关闭结果分别确认，未确认时不释放原件容量或允许替换。重入/中断关闭保留已消费的原始凭据与失败来源，不能用 Drop、EOF、端口可用性或任务 body 返回当 native close。

标准 AcceptedRequest::Fallback 现在执行实际选路、PROXY v1/v2 前缀和一次性捕获回放，保留 TCP/Unix 网站转发，不创建业务 SessionId。REALITY 的目标选择、认证、探测、TLS 镜像和限速仍归协议/transport；目标读写由其真实握手或 Control future 执行。标准 Control 同时观察 source 的取消，以便取消真实限速等待，再等待原件和 Scope 清理。

合格输入不再进入旧 binder/listener/recorded-fallback 执行链，选中标准 source 后的准备或启动失败不得回退旧链。VLESS 的 InboundListenerCapability 和 adapter listener.rs 尚有未资格 QUIC/H3/Hysteria、TCP XHTTP、reverse、受信 prelude、状态型 mKCP mask 等实际调用，包含这些未资格载体的 REALITY/fallback 组合也不记作完成。没有为了删除文件扩大资格或拒绝原有兼容配置；剩余旧 listener adapter 数仍为 4，PR06A / ALL17 / G0–G10 未关闭。普通 Mieru/native MUX 出站和 WireGuard TEMP-WG-OUTER-UDP-01 边界不变。

本批只改内核，本地 develop/v0.0.3，未暂存、提交或推送；客户端/面板项目及公开配置/API/events/FFI 未改。本批 20 项定向行为回归与 186 项架构边界检查通过，覆盖真实 TCP/Unix 原件的两次串行换代、实际限速等待取消、REALITY 未认证转发、认证 TCP/UDP/ML-DSA/Vision、TLS fallback 的 SNI/ALPN/path 与 PROXY v1/v2、原有 TCP/gRPC XUDP 续接及协议标准接入。未运行全量；缺少 XRAY_BIN，未执行官方 Xray 互操作方向。原始凭据单测位于目标 driver 最后一次收紧之前，最终生产 REALITY/fallback 二进制已包含“等待唯一 view 实际归还才完成 driver”的收紧；最后仅改中立测试名与格式。各验证对应源码阶段、原始基线、变动差异及失败记录保存于仓库外 zero-vless-reality-fallback-20261102/validation-final.json，不作为构建/CI 依赖。

## VLESS 标准接入与 XUDP 原始所有权（2026-11-02，本地增量）

VLESS 的普通 TCP/TLS、WS、gRPC、H2、HTTP-Upgrade 和普通 mKCP 输入已选择实际 ComponentFactory / InboundSource / AcceptedRequest 生产装配，覆盖既有 Stream、Datagram 和 MUX；不是把旧 listener operation 包进工厂。adapter 仅投影配置与冷准备完整 profile，真正的宿主 Build 才发布到原 adapter runtime 的同一认证状态。被选择的标准 source 准备或启动失败不得退回旧 binder；新 owner 仍须等待旧 owner 的原始关闭结果和 Scope joins。没有新增协议组合。

现有标准 Datagram 增加可选续接事实：协议提供认证身份限定的 opaque key 和原始结束原因观察值，宿主不解析 XUDP 帧。VLESS key 由已认证 UUID 与原 wire global ID 组成，并由真实 inbound / protocol / principal 隔离；wire 格式和业务 SessionId 分配规则不变。物理载体退出与显式 END、业务取消、空闲到期分别处理；重连复用原有业务 dispatcher 和原始 UDP 资源，不新建第二份关联 socket/池。

逻辑关联通过显式有限声明按需建立，复用同一个 listener 的原 TaskScope、child owner 和 accepted 资源账本；它有真实 Association origin，不伪造 TCP accepted entry 或原生 UDP peer admission。原始宿主任务/witness 和 cleanup owner 在首次派发/native 准备前安装。物理载体仅持有自己的协议控制器、视图和原始 native/Scope；关联持有真正的 Direct/upstream I/O 与返回债务。断载体后必须消费旧逻辑通道的完整关闭结果才能交接；结束关联后必须确认 dispatcher 原件、原始 provider、返回视图和实际 joins 才删除 key。取消尚未提交的续接只释放逻辑预约，不宣称 native close。任务/资源上限按需使用，未新增常驻实例或第二个资源管理框架；统一生产资源上限配置工作仍未关闭。 通用 source 启动器从已校验的主 TCP/UDP listener 读取并发预算，允许其附加有限逻辑关联声明，不将该声明误当成第二个 native listener；原关闭准入不放宽。

已删除本批对应的临时 TCP 中转链：VlessStandardTcpAccept、accept_standard_tcp_route、AuthenticatedTcp / AuthenticatedClient 和 accept_standard_client_owned 及全部调用者，移除 transport/inbound/standard.rs。未资格载体直接保留原有明确兼容入口，不经新的标准请求再拆回旧枚举。共享 MUX 帧头仍归物理载体，逻辑 UDP 计量保持旧 payload 口径，避免改变限额以及客户端/面板看到的业务用量。

**VLESS listener 整体尚未迁完，InboundListenerCapability 及 adapter listener.rs 仍有真实调用。** 普通 REALITY/fallback 与普通 QUIC/H3/Hysteria 的目标/载体原件迁移见上节；Noise/XDNS/XICMP 等未资格 QUIC、Hysteria 文件/代理伪装、mKCP XDNS/XICMP 及其未资格组合继续走原兼容路径；TCP XHTTP、reverse 和受信 PROXY prelude 的后续标准迁移见本页顶部新批次；不把其既有配置改成拒绝，也不将这些路径计作标准迁移完成。剩余旧 listener adapter 数仍为 4，PR06A / ALL17 / G0–G10 不因此关闭。普通 Mieru/native MUX 出站登记不扩大，WireGuard TEMP-WG-OUTER-UDP-01 原例外不变。

2026-11-01 的候选阶段已补齐受原接入 Scope 监督的 gRPC 单/多流、raw H2 实际流控和普通 mKCP conversation futures；原证据位于仓库外 zero-vless-standard-inbound-20261101 和 zero-vless-carrier-scope-20261101。这些历史候选证据不替代本批实际 Proxy 装配、派发/计量与 StopOld-before-BuildNew 回归。本批最终 **30 项必要定向行为回归、186 项源码架构守卫通过，0 项未解决失败**：真实 TCP/gRPC XUDP 原始关闭与停旧换代 1 项（分别覆盖两种载体）、平台真实 peer/原件 Scope 1 项、共享续接兼容 10 项、协议标准 source/载体/凭据 10 项、生产 TCP 3 项，以及 WS、热凭据、mKCP TCP/UDP、REALITY/REALITY Vision 共 5 项。mKCP 生产启动曾因通用启动器仅接受单项 UDP 声明失败，已修正并只重跑该受影响用例；未把协议候选测试当成生产证据。zero-proxy 全特性类型检查通过（保留未使用项警告），格式、diff 空白与测试布局检查通过。首轮诊断、计量和启动失败记录保留；最终增量、用例二进制及日志对应关系见仓库外 zero-vless-xudp-continuity-20261102 / validation-final.json。只改本内核仓库，客户端/面板项目及公开配置/API/events/FFI 未改；本地 develop/v0.0.3，未暂存、提交或推送，未运行全量。

## Packet/device 职责域收敛（2026-10-31，本地增量）

本批删除 `ClaimedPacketLeaf`、Direct/WireGuard 的 Packet claim 包装、`OutboundDeviceLifecycleCapability`、`PreparedOutboundDeviceState`、`OutboundDeviceCompletion` 以及 Registry 中相应的登记表、查询/准备/关闭转发。Packet 在实际适配器装配边界直接生成已有运行时执行操作，通过 `Arc` 共享；WireGuard 已发布 profile 保存同一组实际操作，重复路由投影不重新分配这三组执行对象；native/translate/datagram 与 Flow 转换保持原有可用性和返回关联事实，运行时不再调用第二个协议准备接口。

实际设备所有者由编译时的 Direct/WireGuard 装配保存，与现有 inbound/outbound 使用同一个 adapter/pool；inventory 克隆共享它，弱 dispatch 仍不保留设备或宿主。具体准备值直接承接配置、网络换代、发布、失败恢复和关闭，不引入新 lifecycle trait、回调工厂、协议组合或第二个宿主。Registry 回到 lookup/dispatch 责任，设备准备上下文也已迁出 Registry。该具体装配只接管既有 Direct/WireGuard 兼容设备，不是新增协议的标准接入契约；目标仍是既有 ComponentFactory / Packet 角色与真实原始 provider，未资格的 native 工厂不启用。

普通 Direct descriptor 和无 outer proxy 的 WireGuard 设备在 BuildNew 前回收受影响的原件；新设备的原件先进入持久所有者，再等待 readiness。取消/丢弃候选会保留清理所需的实际原件，关闭等待可重入；Raw-IP 原始 UDP socket 的显式关闭结果与原始 worker joins 分开消费，不能只看 `is_closed`、Drop 或端口可用性。失败恢复先重建上一配置的 DNS 依赖，再准备实际设备和 listener；DNS/设备恢复失败时不继续启动依赖 listener，取消候选须先丢弃其准备占位。设备关闭错误会进入原有 orchestration 错误结果，取消/超时只留下未完成原件，不确认关闭。

**用户已明确选择保留既有兼容行为的边界：WireGuard `outer_udp_proxy` 及其重连历史尚缺完整原始所有权/关闭凭据，本批只迁接口和具体调用方，保留旧兼容换代行为，不通过新增拒绝改变既有能力。** 其 grace、worker joins 和当前 path owner 都不算完整 native-close 证明；这项债务继续开放，普通 native device/复杂载体登记不因本批放行。Windows host-session backend 也只保留既有 worker completion，不伪造 native receipt。此批不能记为 Packet 标准角色及全部设备 native 所有权完成。

仅内核、本地 develop/v0.0.3；公开配置/API/events/FFI 未修改，客户端和面板项目未修改；不提交、不推送。**54 项定向行为回归、182 项架构守卫通过**；fmt、diff 空白和测试布局检查通过（314 source files / 95 targets / 0 errors），未运行 workspace 全量。行为来自 source-7 的同一个 lib test 二进制；source-8 仅给 raw-IP socket 的两个 Datagram listener 分支补充 `datagram-route-runtime` 条件，在本次 all-features 执行中展开代码一致，两个裁剪构建（no-default / raw-ip-runtime only）均通过；仍有 optional-feature / unused 警告，不记为 Clippy 零警告或全量资格。另用裁剪构建验证条件缺失时的行为。源码冻结、原始关闭/握手记录和过程失败证据留在仓库外任务产物 `zero-packet-device-domain-20261031`，不作为构建或 CI 依赖。

## UDP 执行职责域迁移：移除 handler / Any / resume 旧链（2026-10-31，本地增量）

本批以完整执行职责域收尾：VLESS、VMess、Trojan、Mieru、HY2 和 Shadowsocks 的剩余既有 UDP 调用者，在建立边界直接使用 Rust 静态类型构造连接；登记后保存真实 Logical/Tuple/Datagram/Upstream I/O。删除三个 managed handler trait、全局 handler 集合与装配函数、opaque `ManagedUdpFlowResume`、Any/downcast 恢复、按 kind 分流的请求/发送模型，以及 stream/datagram manager 和对应包装目录。后续业务包直接调用已登记连接，不再重建 cache identity、查找 handler 或克隆/恢复协议状态；没有新增协议枚举、回调工厂或替代协议 SPI。

保留的具体协议 `*Resume` 是协议自己拥有的类型化 wire 配方，仍在冷建立及必要的逻辑重建中使用；既有编译时 connector 负责接到真实协议 I/O。它们不再以任意类型 payload 进入运行时登记表。Rust `TypeId` 只用于冷缓存的类型命名空间，维持原先各具体 manager 的隔离；键值仍由协议构造，缓存只存可执行连接，不按诊断阶段、地址或猜测类型拼装新身份。

流式、SS socket 和预发送 datagram 三种原有语义保持：stream/relay 继续使用同一实际池/连接与原错误阶段；SS 保留目标响应 waiter；HY2/Mieru datagram 的建立已发送首包，后续不重发、不重复计量，实际逻辑连接结束后由具体协议配方重建。失败冷建立不积累缓存配方，挂起操作不作为空闲项淘汰。业务失败/取消会撤销对应执行连接索引；标准持续响应读取仍只遍历自己的原索引，不为兼容流新增逐包全表扫描。

协议池、原始 controller/provider、TaskScope 和宿主确认停旧事务未改；逻辑连接结束、归还视图、缓存回收或 Drop 都没有被当作 native 关闭凭据。普通 Mieru native 和未资格 MUX/载体登记继续关闭；没有扩展协议组合。只改内核，本地 develop/v0.0.3，公开配置/API/events/FFI 及客户端/面板项目未改，未提交/推送。该执行职责域的旧恢复链已删除，不代表其余 Packet/device、接入契约、原生资源迁移或 Engine/Config 职责已完成。

最终 **48 项必要行为回归 + 180 项源码架构守卫通过**。新增六项覆盖 SS/HY2 的真实 UDP 收发、同流复用、实际 Flow TX/RX、HY2 在真实逻辑连接结束后的重建且不重发首包、合并缓存的 Rust 类型隔离、登记后无配方直接执行/撤销原引用，以及真实 OS bind 失败后的配方所有者不滞留。其余覆盖已有冷投影、标准关联与逻辑响应重读、Mieru 两种 underlay 和三跳/网络换代、已有池化 UDP 业务、10 项 MUX 原始 controller/driver/双视图停旧替换、HY2 实际认证池、SS 原始 packet carrier 及 Packet/Flow 转换。六个协议的 wire 实现和生产准入未改，没有为通过回归新增协议组合。

裸 `--no-default-features`、基础 `udp-runtime`、仅 `managed-stream-runtime`、仅 `managed-datagram-runtime` 四组生产构建通过；裁剪组合仍有未使用项警告，不声称严格 Clippy 通过。48 项行为对应 source-5 冻结实现及同一二进制；source-6 仅在 managed datagram 子模块上补齐原有 feature 门控，修复基础 UDP 组合的编译错误，全特性构建中的执行代码逐字保持相同。补验失败组合及尚未执行的两个裁剪组合，未重复已通过业务组。首轮类型检查的导出/导入问题和旧测试的导入、`FlowFailure` Debug 断言已修正；已报错且源码过时的首次 test build 仅终止本任务自己的 cargo/rustc，保留原日志，不计作通过。

删除旧子系统后，14 项强制旧目录/接口存在的布局守卫同步撤销，并用新执行边界守卫覆盖无旧 token/目录、无 payload downcast/handler 搜索、实际 I/O 登记、冷缓存类型隔离和业务索引撤销；未保留空壳凑测试数量。格式、diff 空白及测试布局（314 source files / 95 targets / 0 errors）通过。未重跑 workspace 全量，未执行本批外部官方互通或客户端/面板实机资格。快照、完整本批差异和原始结果保存在仓库外 `zero-udp-execution-domain-20261031`；本地 develop，未提交/推送。

## 架构债务收尾：删除 managed UDP handler provider 接口族（2026-10-31，本地增量）

本批完整移除 `ManagedUdpHandlerProvider`、VLESS/VMess/Trojan/Hysteria2/Shadowsocks/Mieru 的全部六个实现、注册表的 provider 字段和遍历入口，以及仅为该 trait 存在的 `register_managed_capability` 分支。六个适配器统一使用现有基础能力注册；删除 inventory 的同名纯转发及 dispatcher 构造中仅为查询 provider 而传入的 registry/inventory 参数。现有 `registered_udp_handlers()` 不再接收注册表，在编译装配处直接选择原有的 datagram/stream/relay executor。没有新 trait、回调、工厂 SPI、协议组合或 native 生产登记。

原实现均不读取 adapter 状态；本批直接创建相同的既有 manager，并保持原有选择顺序、每 dispatch 的独立连接状态、stream/relay 两个执行视图对同一 manager 的共享。协议 resume 和实际连接继续携带原有池/资源视图；没有复制或提前启动第二份协议池。通用运行时没有新增协议匹配。原派发、计量、错误和确认停旧再启新事务未改，公开配置/API/events/FFI 和客户端/面板项目未改；只改内核，本地 develop/v0.0.3，未提交/推送。

这关闭 provider 接口族；`ManagedDatagramFlowHandler`、stream/relay handler、Any/resume 兼容执行、Packet/device 与未迁标准角色仍有实际职责和调用者，继续单独列账。整体 ALL17 / G0–G10 未完成，不能将这一层删除写成全部 UDP 已迁标准 Datagram。

最终 **19 项必要行为回归 + 191 项源码架构守卫通过**。定向用例覆盖：无运行时的 handler 冷装配、真实 SS/HY2/Mieru resume 的唯一选择、注册能力、opaque resume 恢复、同流缓存复用、既有 UDP operation 按值转交、普通 Mieru 两种 underlay 的真实派发/复用/计量、Mieru 三跳原始转交、VMess/VLESS/Trojan 原有同池 UDP 兼容入口，以及等待原始 control/driver/native 关闭后的替换。没有为验证新增协议组合，也没有重跑 workspace 全量。

生产源码在 `source-3` 冻结；最终增量编译与上述 19 项结果对应同一源码且无后续实现漂移。首轮构建期间进一步删除了 inventory 转发参数，因此首轮单项冷测试不重复计入最终数量。`cargo check -p zero-proxy --no-default-features` 及额外启用 `managed-stream-runtime,managed-datagram-runtime`、但不选择具体协议的生产构建均通过；裁剪组合仍有未使用项警告，不声称严格 Clippy 通过。格式、diff 空白和测试布局（314 source files / 95 targets / 0 errors）通过。未进行客户端/面板实机或外部互通验收。源码快照、完整本批差异及原始结果保存在仓库外 `zero-managed-udp-provider-removal-20261031`；未提交/推送。

## 架构债务收尾：删除整个 UDP flow claim 接口族（2026-10-31，本地增量）

本批删除 `ClaimedUdpFlowLeaf` 的全部实现/调用、`ClaimedUdpHooks`、Direct/SS/HY2/Mieru/portal claim 包装、三种 transport 的延迟准备闭包，以及 `protocol_registry/transport_leaf` 目录和 Direct/reverse UDP 的纯包装文件。适配器在实际配置目录一次冷物化协议 leaf，并以普通 `Result` 数据交付既有 UDP flow operation / relay binder；没有新增替代 trait、回调工厂或协议组合。VLESS/VMess/Trojan 的 TCP、UDP 与 relay 复用实际协议 leaf 及其真实池视图，原池、profile 和 native 登记边界不变。

现有 UDP operation 的 `Box<Self>` 一次执行语义保持。claim envelope/relay chain 不再克隆；普通 route 直接移交选中的操作，relay 先准备真实 TCP/packet 前缀，再按值移出末跳。所有权用 Rust move 表达，不新增 Arc/Mutex 取件槽或二次准备 SPI。TCP/packet 的既有可复用句柄仍独立共享。冷错误按原阶段保存在 owned `FlowFailure` 中，未选中的 UDP 错误不影响 TCP/Packet；Direct 在选中时继续独立验证当前平台接口。迁到标准 Datagram 的 VMess 路径仍拒绝旧 direct 执行，选中标准角色后的错误不能重回兼容入口。

原真实载体、任务监督、关闭凭据和停旧再建新事务未改。Mieru 的 stream/datagram/lazy binder 保留，VLESS 的双流/关联 connector 分支仍由 runtime 执行；普通 native Mieru 登记继续关闭。只改内核，配置/API/events/FFI 和客户端/面板项目未改；版本 develop/v0.0.3，未提交/推送。这关闭第三个完整准备接口族，不代表剩余 handler/resume、Packet/device 和未迁标准角色已完成。

最终 **71 项必要行为回归 + 190 项源码架构守卫通过，0 项未解决失败**。新增两项真实适配器冷 UDP/relay 结果检查、一项 TCP/Packet 不受 dormant UDP 错误影响的选择检查，以及一项普通 Mieru TCP/UDP underlay 的真实 UDP 派发、同流复用与逐包累计 Flow TX/RX；Mieru 普通 native 登记仍明确断言关闭。其余覆盖 VMess 生产 UDP 37 项、Mieru 原始前缀/网络换代 3 项、MUX 原件替换 10 项、HY2 同一认证池、SS 原始 UDP、Direct/TCP、packet/registry 投影和逻辑会话。旧测试专用 UDP claimer已移除。

生产实现在 source-3 冻结；随后 source-4 仅修正新增测试对普通 VMess 的预期：metadata 中仍有 relay 投影，但旧 direct UDP 应返回 `udp_standard_role`。保留该生产拒绝，没有放开兼容入口；仅补验两项冷投影，其余已通过业务结果复用，未再重跑。前两轮编译涉及旧测试消费 UDP 后继续使用 claim、以及新增 response join 测试元组形状的修正，失败/中止不计通过；最终测试沿用真实 provider/任务/关闭凭据。

`cargo check -p zero-proxy --no-default-features` 与增加 `udp-runtime` 的最小组合均通过；这些裁剪组合仍有可选功能/未使用项警告，不声称严格 Clippy 通过。格式、diff 空白和测试布局（314 source files / 95 targets / 0 errors）通过。未重跑 workspace 全量，也未执行客户端/面板实机验收。过程、源码冻结和原始结果保存在仓库外 `zero-udp-flow-claim-removal-20261030`；未提交、未推送。

## 架构债务收尾：删除整个 TCP claim 接口族（2026-10-30，本地增量）

本批删除 `ClaimedTcpOutboundLeaf` 的全部生产/测试实现、`ClaimedTcpHooks`、socket/session/transport claim helper，以及 `protocol_registry/claim.rs`、`protocol_registry/transport_leaf/tcp.rs`、`adapters/direct/tcp.rs` 三个纯包装文件。Direct、SOCKS5、Shadowsocks、Mieru、Hysteria2、VLESS、VMess、Trojan 以及 VLESS reverse 直接交付原有 connect/relay 可执行操作。socket 和 transport 的两种执行视图共享同一实际 `Arc` 对象，不再在 TCP 选中后克隆 leaf、分配另一层执行包装。没有新增替代 trait、工厂契约或协议组合。

`Result` 类型别名仅表达已有执行操作或冷诊断数据。三种 transport 的错误阶段、connect endpoint、relay 错误文本仍在相应 TCP 分支选中时返回，不影响 UDP/Packet；真实 I/O 的错误来源沿用原执行器。Direct 的源接口检查仍在实际选择时验证当前平台状态，严格 Packet 转换跳过该 Stream 分支。Hysteria2 在适配边界固定实际 source_dir 并复用真实认证池；Mieru 的既有 lazy relay 执行未变，池命中不能为了取得前缀而提前拨号。VLESS reverse 直接交付既有逻辑 TCP operation，剩余 portal UDP claim 单独列账。

本批未改宿主原始关闭 provider、TaskScope、依赖换代或确认停旧事务，未扩大 native 生产登记。公开配置/API/events/FFI 与客户端/面板项目不改；本地 develop/v0.0.3，未提交或推送。已有 TCP 执行操作和标准角色未迁路径仍有职责，不能把删除这一层记录为整体重构完成。

最终 **47 项必要行为回归 + 190 项源码架构守卫通过，0 项未解决失败**。新增真实适配器冷投影/克隆共享和 Direct 选中时策略检查两项；删除旧测试专用 TCP claimer，并保留实际 Packet converter 的选择回归。既有用例覆盖三种 transport 的延迟错误/相对目录/实际 MUX 所有者、HY2 同一认证连接、Mieru 命中池时不打开 lazy 前缀及原错误来源、15 项 TCP 错误/健康归属、10 项 MUX 原件与配置/网络替换、已有前缀的真实 TLS/UDP/计量，以及 packet-path/设备投影。

最终 lib test 可执行文件对应 source-3 冻结实现；其后只更新文档。首轮新增 Direct 用例错误假设无效接口可通过 UDP 准备，修正为保留 UDP 自己的既有校验，未修改生产 UDP 行为。190 项守卫首轮的四项旧文本断言已用本批开始前对应源码复核为既有失败，现按实际协议模块声明、测试专用属性和配置解包事实检查，继续禁止运行时执行、Engine leaf 恢复和 runtime facts 构造。守卫按原 std-only 测试源独立编译，不为读取源码再构建普通库。

`cargo check -p zero-proxy --no-default-features` 通过，覆盖没有 UDP 生命周期闭包时的生产构建；该最小功能组合仍有可选功能/未使用项警告，不声称严格 Clippy 通过。格式、diff 空白及测试布局（314 source files / 95 targets / 0 errors）通过。未重跑 workspace 全量，也未做客户端/面板实机验收。过程、前后快照和原始结果保存在仓库外 `zero-tcp-claim-removal-20261030`；未提交、未推送。

## 架构债务收尾：删除 packet-path 二次 claim 接口（2026-10-30，本地增量）

本批以完整接口族为边界：删除 `ClaimedUdpPacketPathLeaf`、三个仅复制计划的协议包装、VLESS 的延迟源目录构造闭包及所有 `prepare_udp_packet_path` 转发。SOCKS5、Shadowsocks、Hysteria2、VLESS 直接提供已有的 `PreparedUdpPacketPathOperation`；统一声明、inventory、TCP/UDP relay、设备前缀与运行时请求均转交同一个 `Arc` 操作，不新增替代 SPI。VLESS 复用已由实际配置目录准备的同一协议 leaf；Hysteria2 在适配边界固定 source_dir，防止运行时再次指定另一目录。

操作仍是冷配方，载体创建、协议认证、计量、缓存与关闭继续由原执行边界负责。未新增协议组合或启用未资格登记，未改变实际原始 provider、TaskScope 或停旧再建新事务。这里删除的是一整个冗余 claim 层，既有执行操作仍承担剩余 packet-carrier 路径，不能据此宣布标准 Datagram/Packet 全部迁移或 Z003-06 整体关闭。公开配置、API/events/FFI 及客户端/面板项目不改；仅本地 develop/v0.0.3，未提交、未推送。

最终必要定向 **37 项行为回归 + 10 项架构守卫通过，0 项未解决失败**。新增两项真实适配器检查：无需 Tokio 运行时的冷投影/克隆共享，以及 VLESS/Hysteria2 配置目录身份隔离；删除旧测试专用 claim 适配器，既有 Shadowsocks 真实 UDP/原始关闭用例改为从实际 Proxy/inventory 取得操作。其余覆盖出站投影、tuple/codec、设备前缀、原始 packet owner、raw-IP 接收监督、MUX 替换及已有 VLESS/Trojan 前缀的实际派发/计量。

最终行为结果来自 source-3 冻结源码的同一个 lib test 可执行文件；10 项源码架构守卫由原 std-only 测试源单独编译，扫描整个 Proxy 源码树禁止恢复旧接口和准备方法。初次 no-run 同时触发的额外普通库编译已停止、不计通过；追加真实用例时调用了 inventory 私有便捷方法，现改用已有 crate 可见的正常入口，没有扩大生产可见性。最终 `cargo fmt --all -- --check`、diff 空白及测试布局检查通过（314 source files / 95 targets / 0 errors）。未重跑全量或消费者实机验收；原始过程与冻结记录保存在仓库外任务产物 `zero-packet-path-claim-removal-20261030`。

## VLESS/Trojan MUX 首跳生产迁移（2026-10-30，本地增量）

三种协议复用同一条冷准入规则：literal/default-dial 的实际 MUX 首跳、已有普通可执行中间角色、合格 VMess gRPC 非 MUX UDP 末跳；非首跳、嵌套或其他未合格组合仍保留原登记方式。各协议工厂自行声明并构造 Stream-only 角色，gRPC 消费标准 StreamLoan 与原始父链，不走该链的旧 TCP claim/prepared。没有第二个 Host/Proxy 或私有物理池。

VLESS 只晋升显式启用 XUDP 且 XUDP/MUX 并发相同的子集，不改写原普通 UDP。实际 PacketIo 在同一个 adapter map、原始 native owner 和 TaskScope driver 上发送；保留既有 XUDP global ID、空包、多目标编解码及 packet 兼容入口。原逻辑兼容 pump 直接消费实际 PacketIo，并持有同一 PoolUse，IO 先于 custody 归还；不再为 native UDP 建立旧 upload relay/物理 driver。Trojan 的原固定目标 raw UDP 编解码使用同一个原始池的 UserStream，保持原包长及空包限制；未宣称普通标准 Datagram 整体迁移完成。

所有权 fence 仍按节点 tag 跨 transport 保留，完整 profile 同时包含实际 transport scope，防止同一节点的 WS 等变更错误复用旧池。已暴露的 legacy owner 在冷候选中也不能通过 cache/view 驱逐晋升。配置替换继续先结清真实消费者、driver 和原件，再构造/启动新资源；新物理连接按需建立，复用既有可选上限（默认 4）。公开配置/API/events/FFI 形状不变，没有修改客户端或面板。

Trojan native 兼容 pump 将读写两半收敛到同一个逻辑任务：原始物理读取结束即归还两边的实际 IO/PoolUse，即使调用者仍持有缓存发送端也不能钉住退休资源。VLESS 同样在实际 PacketIo 结束后结清 custody。没有用 logical return 代替父池的 native/task 凭据。

本批必要定向 **66 项通过、0 项未解决失败（新增 6 项）**：新增生产 3 项（各覆盖 VLESS/Trojan）、既有 gRPC/CA/DNS/relay 33 项、既有原始 MUX 10 项、VLESS/Trojan/VMess adapter 分别 7/4/9 项。生产检查覆盖两条真实 TLS/H2/UDP 流共用一个原件、legacy/native 两节点实际 UDP/空包及 8 KiB 收发和逐包累计计量、真实 legacy 暴露后的冷候选拒绝晋升，以及持有实际 UDP dispatch/发送端跨原始关闭后不能恢复发送。挂起 CONNECT 替换验证旧消费视图、供体 driver 和原始 native 关闭匹配；Trojan 使用同 endpoint 的 WS header 变更，替代池在确认停旧后才安装，物理载体仍按需创建。

首轮只修正新测试模块的显式路径；测试中强制 `RouteMode::Packet` 被原有 UDP admission 拒绝，保留该既有拒绝，并对比 legacy/native 的严格 Packet 行为。没有新增原生 Packet 能力，也没有把 packet-carrier 兼容入口等同于原生 Packet 角色。最终实现/测试按 source-4 冻结，期间只更新文档；格式、diff 空白及测试布局（314 source files / 95 targets / 0 errors）通过。复用同一个已构建二进制完成其余分组，不重跑全量，不修改客户端/面板，未做消费者运行或 1 CPU/1 GiB 整体验收。仅本地 develop/v0.0.3，未提交、未推送。

## VMess gRPC 普通 VMess MUX 首跳前缀（2026-10-30，本地增量）

本批将合格的 VMess MUX 首跳接入普通 Proxy 目录：literal endpoint、现有受支持 TCP/TLS/WS，默认 dial，直接 relay 最后为已合格的非 MUX VMess gRPC，中间仅限已有普通可执行角色的 SOCKS5/非 MUX VMess。配置中的同一 MUX 节点若还用于非首跳、嵌套 relay 或不合格组合，整体保持原兼容所有权。受控目录的其余组合不因此自动晋升；VLESS/Trojan MUX、Mieru 普通 native 登记仍关闭。不是全部 MUX 迁移完成。

生产前缀仅登记实际标准 Stream，保留原 roleless 池、TaskScope、StreamLoan、消费者依赖和原始关闭链。gRPC relay 不再创建该首跳的 claim/prepared TCP 操作；后续 TLS/H2/UDP 派发和 Flow/Outer 计量使用原标准 Datagram 执行。兼容 UDP 沿用协议自有 byte-stream framing/响应语义，但通过同一个 native map/PoolUse 和原 driver 获取逻辑 UDP 流；不另开 legacy 物理池。普通 TCP 仍由实际标准 Stream 角色执行；旧 TCP 准备不得绕开已选择角色。其余仍有真实调用者的旧接口保留，不能空删后声称全部迁完。

PoolControl 按实际 owner 隔离 native/legacy 准入，不再因一个已登记节点而关闭另一个 legacy 出站。VMess 的所有权 fence 按配置 tag 跨 transport 保留，实际缓存键仍包含完整 transport/profile；任何同名节点的 legacy 暴露记录在 adapter 存活期间保持否定准入，驱逐缓存、丢弃视图或切换 TLS/WS 不能替代原始关闭凭据。历史 native 视图关闭后仍失败，不得绑到新代。兼容用户选择实际当前根 provider；网络/DNS 换代继续由原 epoch 事务消费旧用户、驱动及原始关闭结果后才取得新载体。

`runtime.native_mux_max_carriers` 是可选正整数，范围 1–4096，默认 4；只限制按需创建的物理池和其任务，不预分配连接。省略时不导出该字段，现有公开 API/events/FFI 和协议配置字段保持。只修改内核，本地 develop/v0.0.3，不提交或推送；没有改客户端、面板，也未做消费者运行验收。

必要定向最终累计 **63 项通过、0 项未解决失败（新增 7 项）**：gRPC 生产/CA/DNS/relay 33 项、已有受控 MUX 10 项、真实业务原始授权 4 项、VMess adapter 准入 9 项、实际池控制/队列 6 项，以及配置上限的默认/显式/非法边界 1 项。普通 MUX 首跳的两条真实 TLS/H2/UDP 流共池、实际挂起 CONNECT 的配置替换、legacy/native 两节点独立运行及同池 UDP、Scope owner 丢失/退休拒绝重建、跨 transport 的 legacy 否定准入均有覆盖；默认 4 与配置 2 的额度已在实际原始 registration 上核对。

验证中修正测试配置的未定义 inbound 与错误 gRPC 字段名；实际 Darwin UDP socket 的 SO_SNDBUF 为 9216，16 KiB 直接回环发送亦报 EMSGSIZE，因此真实 UDP 对比采用既有 MUX 的 8 KiB 基线，并验证空包后继续收发及逐包累计 Flow 计量。没有为测试更改系统/内核 UDP 缓冲策略，没有降低协议接口的包长上限。首轮编译错误及中途中止不计通过；最终实现自 source-3 冻结后未改，随后仅修正两处测试文件，复用已构建二进制与定向失败项补验，未重跑全量。格式、diff 空白和测试布局（314 source files / 95 targets / 0 errors）通过；未做 GUI/面板运行或 1 CPU/1 GiB 整体资格。

## VMess gRPC 混合与池化 relay（2026-10-30，本地增量）

本批继续迁移既有路径。普通 Proxy 中，拥有实际共享 replay 状态/维护依赖的 Shadowsocks TCP2022 等标准 Stream 前缀不再仅因存在非 I/O 依赖而整体回到旧 UDP relay executor；它们在同一个受控 TCP 载体上执行协议握手，仍使用原 adapter 的共享状态、真实依赖和 TaskScope。标准 Datagram 执行、响应读取和 Flow/Outer 计量沿用既有通路。进程、设备及其他不合格原生依赖仍保留兼容执行。

已有受控目录中的 VLESS/VMess/Trojan MUX 标准 Stream 角色现在通过实际 `connect_carrier` 交付逻辑 StreamLoan。控制器保留同一个真实逻辑流和 PoolUse，消费实际流的 shutdown/归还；原始池的物理 I/O、驱动及关闭凭据仍由供体持有。逻辑归还不能冒充共享池 native close。gRPC 的载体取得契约兼容原生准备和真实借用：消费者在后续握手前安装 LoanOwner，自己的唯一 H2 driver 仍注册在原 TaskScope。它没有另建池或绕过原 Scope 启动 worker。

当池的上游具有实际借用执行器时，中间池的实际宿主 owner 会保留上游借用及原始父链；现有有限池的 epoch 切换继续等待旧原件和用户结清。宿主按实际角色、依赖和供体原始 registration 校验来源，弱目录引用不保留旧配置/宿主。消费者的配置替换依赖仍在申请载体前登记；挂起的后续握手也不能丢弃已经取得的逻辑控制器。借用失败不重放，直接根载体的 DNS 地址重试仍保持原始关闭后再尝试的规则。

这批没有开启普通 native MUX/Mieru 登记。受控 MUX 组合使用真实 Proxy 装配和监听器验证，但不能计作普通 MUX 生产准入已完成；普通目录中的网络池化/未合格混合前缀仍有实际兼容调用者，因此共用 claim/prepared 接口尚不能全部删除。迁移后的标准组合绕过这些旧操作；版本保持 develop/v0.0.3，仅改内核，公开配置/API/events/FFI、客户端与面板项目不变，未提交或推送。

异步载体契约统一为 `acquire_controlled_stream`，删除重复的公开 `prepare_controlled_stream_async` 及其转发。同步原始控制器准备仍用于必须先安装原件的原生调用；DNS 准备只保留为宿主私有实现。取得阶段的错误保留实际业务身份；只有带原始 TCP 连接错误的失败可以进入地址候选重试，DNS/前缀失败不能重放。中间池分别保留规范化 socket endpoint 和原配置的 CONNECT wire 名称，完整冷身份包含该 wire 名称，避免改变既有 relay 地址编码。

首轮业务 28/3：修复载体取得重构遗漏错误业务 ID、将 DNS 准备错误带入连接重试，以及中间池把原有 named-target 改成 IP 编码这三个实际问题，保留原断言。普通 Shadowsocks replay 组合、三种 MUX 真实共池及挂起替换在首轮已经通过；中间池还需消费真正根 TCP 控制器的关闭凭据，不能只用借用槽的完成状态作证。最终必要定向 **54 通过、0 失败（新增 4 项）**：gRPC 取得/CA/DNS/生产派发 31 项、既有受控 MUX 10 项、业务原始授权 4 项、adapter 装配/准入 7 项、既有 TCP/TLS/WS/WSS Datagram 2 项。新增 4 项分别覆盖普通 Proxy 的经典 SS/TCP2022 replay 组合、三种 MUX 前缀的两条真实 TLS/H2/UDP 流共用一个物理池、Socks → MUX → 普通 VMess → gRPC 的中间池原始父链，以及 CONNECT 回复实际挂起时的真实 Proxy 配置替换。源池驱动和原始 TCP 控制器必须关闭匹配；中间借用槽完成本身不作原生关闭证据。原普通 MUX 关闭准入守卫也继续通过。

最终构建 11 分 27 秒，31 项 gRPC 实际运行 10.18 秒；其余 23 项直接复用同一个二进制，不再构建。冻结源码验证到结束无可执行源码漂移，期间只更新既有文档；`cargo +1.99.0 fmt --all -- --check`、`git diff --check` 与测试目录检查（314 source files / 95 targets / 0 errors）通过。未重跑全量，旧的 35 项和历史全量结果不替代本批验证。客户端/面板实机未重跑；本批未改消费者、公开配置/API/events/FFI，内核 Rust 取得接口变更已记入 release ledger。此前失败检查/夹具修正不作通过证据。

## VMess gRPC 非 MUX UDP relay 生产迁移（2026-10-30）

普通 Proxy 的 relay 末跳若为已登记的 `ControlledNodeStream` / Datagram，且所有前缀均为实际标准 Stream、默认拨号、无后台任务/池及额外依赖的角色，现在直接选择 `PreparedUdpOutbound::DatagramRelay`。它进入同一逻辑 UDP 会话执行器、响应派发和 Flow/Outer 计量，不再构造该路径的 claim/prepared relay operation。前缀仍由各协议的 Stream 实现负责握手，通用 runtime 不匹配协议配置或解析协议字段。

宿主签发绑定真实业务准入、末跳 endpoint 和完整前缀代次的 relay ticket，在同一原始受控 TCP socket 上内联执行前缀握手，然后交给原 VMess TLS/gRPC 实现；不增加后台实例、载体池或 driver。前缀不能替换宿主已安装的准备操作，也不能另行直连。普通 root issuer 仍拒绝非空前缀，外部只持有数字身份不能取得 relay 原始资源授权。首跳实际 socket 地址、逐跳 Outer 观察、首跳健康与末跳 Flow 投影沿用已有语义。

沿用 RuntimeHost 的实际代次依赖图，在 DNS/载体申请前登记末跳到每个前缀的真实 owner 依赖；前缀变化会使消费者一同换代。关闭消费同一个原始 controller、实际 H2 TaskScope driver、转交用户和前缀角色租约的结果，旧资源未确认清理不能启动新代际。挂起的前缀 CONNECT 也走这条事务；取消或视图归还本身不是关闭凭据。网络变化仍等待旧用户/driver/native 结清。

首跳域名只由同一宿主的原生 Node DNS provider 解析，查询使用原 TaskScope 和原始 socket 凭据；末跳域名交给前缀，保持 TLS/CA 身份及日志中的原名，不在本机绕过前缀解析。配置了合格原生 DNS 的普通 gRPC owner 复用同一个共享 DNS 依赖，使 literal 末跳也能使用域名首跳；未发生需求时没有额外查询或 socket。多地址只重试尚未进入前缀握手的实际 TCP 连接错误，原始控制器关闭确认后才能尝试下一地址；前缀、TLS 和协议失败不重放，也不回退同一节点的旧 UDP executor。

无标准 Stream 的混合前缀、池化/有任务或依赖的前缀、非默认拨号、System/detour 首跳 DNS、外部 ECH 等仍保留现有兼容路径及其真实调用者；不能删除这些共用的旧接口。已选择的标准角色若撤销或出错直接失败，禁止通过兼容入口重新拨号。native MUX/Mieru 普通登记保持关闭。本批不改公开配置、API/events、FFI 或客户端/面板项目，仍为 develop/v0.0.3，未提交或推送，不代表整体架构迁移完成。

必要定向回归共 **35 通过、0 失败（新增 7 项 relay）**：gRPC 实际生产/资源回归 22 项、真实业务授权 4 项、adapter 冷选择/装配 7 项、既有 TCP/TLS/WS/WSS Datagram 2 项。7 项新增用例覆盖单/多前缀、实际 VMess/SOCKS5 握手、明文/可信 CA TLS、普通/multi_mode、空包/大包/多 cipher、Flow TX/RX、首跳域名的真实 DNS 原件、末跳域名委托、网络换代，以及实际 CONNECT 回复仍未送出时的真实 Proxy 配置替换；分别检查实际 H2 driver join、原始 native 关闭匹配及旧角色撤销。前缀拒绝、撤销、UDP 禁用均验证未绕过到末跳或旧执行器。

首轮 15/4 的 4 项新增用例停在测试夹具将 relay 成员误写为 `outbounds` 的解析错误；修正为既有 `proxies`，selector 仍保留其既有 `outbounds`。补齐用例后最终 22/0；其余 13 项复用同一个二进制运行。最终定向构建耗时 43 分 58 秒，业务组实际运行 10.33 秒；未重跑全量。构建至验证结束可执行源码无漂移，期间只更新这四份既有文档。`cargo +1.99.0 fmt --all -- --check`、`git diff --check` 和测试目录检查通过（314 source files / 95 targets / 0 errors）。本批未重新运行 GUI/面板实机验收；兼容说明依据本批配置/API/事件/FFI 及消费者源码均未改、既有帧和计量定向回归保持。

## VMess gRPC UDP 域名/DNS 生产装配（2026-10-30）

普通 Proxy 的非 MUX gRPC UDP 域名节点接入标准 Datagram；冷选择同时检查实际候选 DNS 配置。所有配置后端必须具备已接通的原生查询 provider（literal/bootstrap peer、无 System/detour，所需加密 DNS 功能已编译）。不满足条件的配置保留原有兼容入口；选中后发生 DNS、拨号或 TLS 错误，不再转回同一节点的旧 UDP preparation。TCP、relay、外部 ECH 与 native MUX/Mieru 的既定准入边界不扩大。

共享 DNS 查询工厂是同一个 RuntimeHost 中的真实无角色 owner，gRPC 工厂声明其实际 ComponentId 依赖，冷装配验证后再启动。复用原 DnsSystem 的配置、Node 解析策略及 DNS 原始 provider；没有第二个 resolver、Proxy 或协议池。原生 Node 查询用独立 flight/答案命名空间，实际任务仍注册到原查询 TaskScope；新旧缓存共用配置的总 LRU/TTL 上限，不新增一份缓存容量。内部 DNS 上限仍为 16，本批不冒充 Z003-07 的可配置资源预算已完成。

中立异步 controlled-stream 准备契约允许宿主在原始 reservation 和用户记录安装后执行真实 Node DNS。域名保持为 business ticket、TLS/SNI 和日志中的原 endpoint；拨号 IP 是宿主从真实 DNS 答案选出的地址，准备和连接完成时均检查 DNS/网络代次。多个地址串行尝试，前一次原始 native 控制器关闭确认后才能申请下一 socket；TLS/协议握手失败不重放。实际连接地址仍进入既有 path 投影，Flow 计量和 VMess/gRPC wire 不变。

网络变化使挂起的 Node DNS 等待返回过期，新的查询申请还需等待旧查询任务 join、逻辑 custody 归还与原生关闭。gRPC 换网仍先检查原用户、H2 driver 和 native 原件，未清理完不能获取新载体。DNS 配置变化通过真实共享依赖传播到消费者，先关闭旧 gRPC，再清理旧 DNS，最后构建/启动新 owner。单纯路由重载会刷新 resolver publication，但不据此强制清理未变化的活跃 UDP owner；原连接可继续保留并允许新的 flow。

本批不增加公开配置字段，不改 V1 API/events、FFI、客户端或面板项目。版本仍为 develop/v0.0.3，未提交或推送。此节仅记录该生产子集，ALL17 / G0–G10 及其余协议迁移仍未整体关闭。

最终必要定向回归共 **25 通过、0 失败（新增 9 项）**：实际 gRPC 生产/所有权 15 项（原 8 项及新增 7 项域名/DNS 用例）、adapter 冷选择/装配 7 项、既有 TCP/TLS/WS/WSS 非 MUX Datagram 2 项、DNS 缓存预算 1 项。普通和 multi_mode 使用只有域名 SAN 的真实证书且不设置 server_name override，验证原域名的实际 CA/TLS/UDP 收发、DNS UDP→TCP 回退、Flow TX/RX、实际 IP 与配置域名的不同投影、多地址原始关闭、DNS 失败拒绝旧路径、挂起查询换网、DNS 配置的传递替换及路由单独重载。保留真实 query TaskScope、H2 driver 和原始控制器的匹配断言。

首轮实际业务为 11/4：修复生产路径已取得真实 IP 却仍投影配置域名的问题；两项换代用例误用终止整个 Datagram 准入的 wait_closed，改用既有 progress_retired 并保留原始关闭断言；多地址操作可能已返回连接拒绝，改为保留真实已申请控制器及其原始关闭凭据，不再将它假定为仍在拨号。调整后本组 15/0，再复用同一个二进制执行 7 项装配和 2 项既有载体检查，均通过。开发中的编译失败输出单独保留，不计作实际回归通过。

使用 Rust 1.99.0 的定向测试编译，未重跑 workspace 全量。最终 gRPC/装配/载体验证前后 4156 项源码/构建输入无漂移；缓存实现此后亦未修改。cargo fmt --all -- --check、git diff --check 和测试布局检查通过。原始输出只存于仓库外任务产物 zero-vmess-dns-production-20261030；历史 3886/0/175 不作为本批最终源码证据。


## VMess gRPC UDP 外部 CA 生产装配（2026-10-30）

本批将前一阶段的外部 CA 准备接口接入普通 Proxy 的实际注册器。literal endpoint、非 MUX gRPC、受支持 prepared TLS 的外部 `tls.ca_cert_path` 已选择标准 Datagram 生产角色，不再执行该子集的旧 UDP preparation。普通与私有装配都从同一个候选配置传递 source_dir 和文件预算；不新建协议 runtime、池或清理任务。hostname/DNS、外部 ECH、relay、未支持的 TLS 后端与 MUX 的兼容边界继续保留。

新增可选 `runtime.file_read_max_bytes`，默认每文件 8 MiB，支持配置调大；零值和溢出探测无法表示的上限在 config 验证中拒绝。未配置时该字段不出现在导出 JSON，既有 `tls.ca_cert_path`、V1 控制接口/事件、FFI 及客户端/面板项目保持兼容。CA 在真实宿主的 File grant 中按组件代际读取一次；相对路径以配置目录为基准，后续 flow 使用该授权快照。改变 CA 路径或预算触发原有确认停旧事务，禁止在旧用户、原 H2 driver 和 native carrier 未完成前启动替代资源。

必要定向回归共 **15 通过、0 失败（新增 6 项）**：gRPC 业务/所有权组 8 项（含新增 CA 双模式 TLS/UDP 收发、真实配置替换、三种文件失败和不可信 TLS 证书拒绝），adapter 冷装配 6 项，配置预算/旧导出形状 1 项。实际 CA 收发覆盖空包、大包、Flow 计量与 endpoint 展示；启动后移除 CA 文件验证不在新 flow 中重新打开。替换在旧逻辑读取明确挂起后开始，保留原控制器和 driver 凭据，直到旧用户归还才读取新 CA、启动新代际并与不同证书完成 TLS/UDP 收发。未重跑全量，旧的 3886/0/175 结果不冒充本批最终源码验证。

首轮定向为 7/1：启动失败用例重新查询 inventory，取得了新的冷代际，因而无法取得旧 provider。测试现于 start 前保留实际 slot，直接驱动和检查同一原始 File/Scope/native owner；保留原关闭断言，没有放宽生产条件。复验 8/0 后，6 项装配与 1 项配置回归亦通过。格式、测试布局和 diff 检查通过；原始输出在仓库外任务产物 `zero-vmess-ca-production-20261030`。

只改内核，未提交或推送。开发期间另一路已提交 TUN/路由改动 `bab00b69`，本批保留该提交；本记录仅说明 CA 增量的验证范围。整体架构迁移及 gRPC 的其余分支尚未全部完成。

## VMess gRPC 非 MUX UDP 生产迁移（2026-10-29，本地增量）

本批只改内核，承接本地 develop 的既有未提交工作。普通目录中的 literal endpoint、无外部 CA/ECH 且由现有 prepared TLS 支持的 gRPC 非 MUX UDP 已接线至标准 Datagram；明文/TLS、普通与 multi_mode gRPC 使用同一真实载体实现。工厂只向该生产子集提供 Datagram，TCP、探测及 relay 保留原有执行器；hostname、外部 CA/ECH、未支持的 TLS 风格和 MUX 保留原兼容能力，不开启 MUX/Mieru native 登记。新增 4 项真实 Proxy/socket 业务回归和 1 项实际宿主权限边界回归已通过，前后源码无漂移。Rust 1.98 的格式、严格 workspace/all-targets/all-features Clippy（`-D warnings`）、默认 workspace check 和 VMess-only check 均已通过；常规与最小组合仍有既有可选功能警告。接续的同一轮 workspace/all-features 全量已于 2026-10-30 01:57:52（Asia/Shanghai）结束：**3886 通过、0 失败、175 忽略，155 个 Cargo 顶层目标，结束码 0**；嵌套子进程报告不重复计数，4529 项源码/构建输入前后无漂移，清单 SHA-256 为 `5908de73fa1097777a2ee4857003dad79d131e83cacf583c078c989dea4c74f3`。未重新启动全量；该结果属于下面 CA 准备接口增量之前的冻结源码。

协议复用上一批的 VMess 逐报文编码/重组、TX 兼容字节口径和标准 payload receipt。gRPC 的原 H2/read/write/keepalive futures 仍组成同一个实际 TaskScope driver；没有第二个客户端池、后台资源管理任务或 Proxy 实例。每 flow 自己持有一条新鲜 gRPC client，同 flow 复用其连接。实际 adapter runtime 只用不含秘密的静态身份和弱状态索引绑定完整协议 recipe；完整描述冲突拒绝，历史角色不得接管新 owner，关闭后的 recipe 也必须经过原始 owner 确认和宿主事务才能再次登记。

既有 Engine 密封准入凭据以 `AdmittedBusinessDemand` 同时服务 TCP/UDP，UDP 必须携带实际成功准入返回值；SessionId 或公开请求事实不授予 native 创建权限。中立 `ControlledNodeStream` 声明固定 endpoint/dial 和有界原始载体数，实际宿主签发当前网络代际的 business reservation；service、foreign owner/fence、其他 endpoint/policy/prefix 都不能借用它。描述声明、实际原 provider、物理与用户覆盖证据和宿主替换事务保持同一所有权链。原始计数和 TaskScope 上限按需消费，未预创建空闲连接或任务；当前生产上限与既有 Datagram 宿主的 4096 上限一致，尚未宣称生产可配置预算或 1 CPU/1 GiB 资格完成。

Datagram 控制器依次归还逻辑视图、消费真实 H2 driver exit 和原 native 控制器关闭凭据。FIN/EOF、cancel、Drop、视图计数或端口可用均不能代替原始关闭；失败或超时仍保留同一原控制器。宿主撤销已使 H2 退出时，归还不可用的逻辑包装只结束 custody，完成报告仍取自原 provider。网络换代在旧用户、原 driver 与 native 未结清前拒绝新创建；配置替换沿用确认停旧再构建/启动新资源的事务。

开发期两次关闭失败按原样保留：首次等待逻辑 EOF 导致清理未完成；改为归还视图并消费实际 driver/native 凭据后，原始 socket 已关闭，但宿主正确拒绝未追踪 caller 的用户确认。现仅对新声明的 Node Datagram 在原始申请前授予既有 transferred-view 模式，未清除 Unknown 或放宽旧路径的判定。修正后的 4 项业务回归覆盖 TLS/明文、普通/multi_mode、三种 cipher、空包/大包/分裂帧、single/selector/fallback、同流复用和原计量/上游展示、真实业务取消、网络换代，以及实际配置替换后再切回旧 profile；原 driver 和 native/users 凭据必须匹配同一实际 owner。另通过固定 endpoint/dial/prefix、foreign owner/fence 与 Service 拒绝等权限边界，以及原两组 217 项分层守卫。

普通 gRPC UDP 直接由 inventory 的实际 Datagram 声明选中，通过既有每 flow 派发/响应监督和 Flow/Outer 计量；通用运行时不匹配 VMess 配置。该子集的旧直接 UDP preparation 不再可执行，仍有实际调用者的 TCP/relay 和其余 carrier 桥保留。公开配置、HTTP/IPC/gRPC 控制 API、FFI、Cargo.lock 及客户端/面板项目未改。本批未提交、未推送。

### gRPC Datagram 外部 CA 准备契约（2026-10-30）

上述冻结全量结束后，新增协议拥有的 `Options::from_node_options_refs_at`：显式传入配置目录和有限 CA 文件读取上限，沿用实际 File owner、gRPC 原始 carrier owner 和相同 adapter 状态索引；旧入口仍以无外部文件授权调用同一准备逻辑。冷准备不读文件，目录与字节上限进入完整身份；相同 recipe 复用同一实际状态，改变目录或上限不得混用 owner。未新增后台任务、连接池或公开配置字段，也未放开外部 CA 的生产选择条件。

本增量仅修改 VMess 的准备入口、测试模块登记及新增测试文件。Rust 1.99 的 VMess 组件定向回归 **6 通过、0 失败（新增 2 项）**，格式和测试布局检查通过；未重跑全量。先前 3886/0/175 的全量对应本增量之前的冻结源码，不冒充新增接口后的全量结果。下一步仍需生产文件预算及目录装配、实际 CA TLS/UDP 收发与替换回归；hostname/DNS、外部 ECH 和 relay 迁移同样未关闭。

全量结束后另检测到并行修改 `crates/proxy/src/runtime/packet_route/session.rs`，该改动不属于本次 CA 增量，保留原样且不纳入上述验证结论。原始日志与输入清单在仓库外任务产物 `zero-vmess-grpc-datagram-20261029`；只改内核，本批未提交、未推送。

## 普通 VMess 非 MUX UDP 生产路径（2026-10-29，本地增量）

本批在本地 develop 承接现有未提交改动，只改内核。普通 VMess 工厂同时提供 Stream / Datagram，两个角色共享相同的协议配置、admission 和 TLS 准备状态；Datagram 自己执行 CMD_UDP 握手、加密流与逐报文编解码，不复用旧的 detached UDP worker。现有 TCP 载体准备抽为协议私有公共函数。普通 TCP、TLS、WebSocket（含 early data）和 WSS 的 UDP Flow 已接通实际生产派发；单节点、selector/fallback 共用该入口。

inventory 根据已编译工厂的真实 Datagram 声明选择每 flow 的标准角色，未增加 VMess 分支。运行时保留原有按业务 session 隔离和复用的行为，通过现有 logical response owner / 原 JoinSet 监督读取，不再经 VMess Any/resume/handler 扫描发送。原控制器在角色打开后、数据视图暴露前交给预留的 RetiredDatagrams owner；宿主已有主循环亦推进同一 owner，因此最后一个 UDP caller 退出后仍继续正常清理，不新起协议或资源管理任务；数据视图归还只证明 custody 结束，关闭超时仍保留原控制器。协议明确执行 stream shutdown 并读取实际 EOF 后才报告逻辑 I/O 关闭；不把此逻辑报告当作其他物理池、driver 或 provider 的关闭凭据，也不改变宿主确认停旧再启新的事务。

该普通路径不再构造 claim/prepared operation，兼容 claim 的直接 UDP preparation 明确拒绝已迁移配置，保留其真实 relay 投影。gRPC、浏览器拨号、外部 CA/ECH 和 relay 仍使用原兼容执行器；普通 native MUX/Mieru 登记继续关闭。本批完成普通 TCP/TLS/WS 的整条业务路径，不代表 VMess 所有非 MUX 载体和 relay 已完成标准迁移。

会话及完成日志继续使用原有带 endpoint 的 StreamPacket 中立流模型，实际执行连接仍是标准 Datagram，不经过旧 handler。无 endpoint 的 reverse/Packet 转换保留原逻辑模型；不改变客户端、面板已可见的上游地址。

Flow TX 沿用既有 VMess packet（含报文头）字节口径，标准 SendReceipt 仍只计业务 payload；可选中立观察回调提供兼容计量，未报告的标准会话沿用 payload receipt，不要求每个协议实现额外的专用计量接口。底层 TCP/TLS/WS 的真实 Outer 观察独立，握手不会再次计入此 Flow TX。RX 进入现有实际 response accounting，支持空报文、跨加密块和同流连续报文，不按一次 stream read 猜测报文边界。

开发期新增 5 项实际 Proxy / 真实 socket 回归通过：五种 cipher、空包与 37 KB payload、分裂帧、selector/fallback、独立 flow 与自身连接复用、Flow TX/RX、TLS/WS early data/WSS、UDP policy / 严格 Packet / 撤销拒绝以及原始关闭超时恢复。真实对端延迟 EOF 时，旧控制器报告未完成；实际 EOF 后消费原关闭报告。两组分层守卫共 217 项通过。首次冻结全量在编译阶段中止，用于补齐会话上游投影，未记为通过；原日志保留在仓库外任务产物。随后全量发现一处 gRPC 专用工厂继承普通 Datagram 声明但没有相应 executor 的不匹配；宿主描述校验正确拒绝，未弱化校验，专用工厂现明确仅声明 Stream。新增第 6 项宿主正常清理回归在 VMess-only（未启用 SOCKS5 或关联运行时 feature）实际运行通过：所有 UDP dispatcher 已退出，原控制器在实际 EOF 前仍占用真实预算，宿主消费其关闭结果后才释放预算且正常准入保持开放。本批冻结快照的完整 workspace/all-features 为 **3879 通过、0 失败、175 忽略，155 个 Cargo 顶层目标，结束码 0**（`TEST_END 2026-10-29T10:51:54Z`）；嵌套子进程报告不重复计数。4520 项源码/构建输入（含 Cargo.lock）前后无漂移，输入清单 SHA-256 为 `e8941e78a228261c2df5b5db6c16b919f3a1fc35bf5b03143e62aa53c0ccca92`。新增 6 项实际 Proxy 回归、原 gRPC 业务回归、原有 UDP/relay/计量和分层守卫均在此快照上通过。外部工具及特权忽略项不计已执行通过。完整测试使用 Rust 1.99、16 MiB 测试线程栈、关闭调试符号和默认测试并发；通过既有夹具参数提供实际 UDP 自收发确认的当前物理地址，不改变产品配置。

全量结束后的严格 Clippy 指出 `Outbound::open_transport` 中一处多余借用；该函数参数已是 `&OpenContext`，仅将 `check(&context)` 改为 `check(context)`，不改变执行语义，也不通过 allow 抑制诊断。完整运行的输入清单保留，上述全量结果不标作这一行修正后重新执行；修正后源码另行冻结为 4520 项输入，清单 SHA-256 为 `cc8f9ed9449af021e73d6a87102ad25cda223961e9df7d667bf10693c8aa7da8`。 其余 4519 项输入一致；将该行的单个 `&` 复原后文件摘要与完整运行快照一致。修正后 Rust 1.98 严格 workspace/all-targets/all-features Clippy（`-D warnings`）、格式、默认 workspace check 和 VMess-only check 均通过，最终输入前后无漂移。默认及最小功能组合仍有警告，不表述为全组合无告警；没有为单个等价借用修正重跑整套全量。原始输出保存于仓库外任务产物 `zero-vmess-datagram-20261029`，不是构建或 CI 依赖。

公开配置、HTTP/IPC/gRPC 控制契约与 FFI 未改，ConnectObserver 的新可选默认方法保留 Rust 实现方的源码兼容。客户端、面板及其他项目没有修改；本批未提交、未推送，不沿用前批二进制的 GUI/面板验证冒充当前验证。

## 普通 SOCKS5 UDP 生产入口收敛（2026-10-29，本地增量）

本批以一条完整的既有业务路径为交付单位：普通 SOCKS5 出站的单节点、组选择与 fallback UDP Flow。生产 inventory 直接选定实际标准 Datagram 代际，再由现有关联执行器收发；不再先构造协议 claim、克隆 prepared operation，再回到同一个标准角色。UDP policy、严格 Packet admission、真实线级 TX 计量、关联复用和关闭所有者沿用原行为。

删除 `RegisteredAssociationUdpOperation` 文件、SOCKS5 对应 `ClaimedUdpFlowLeaf` 实现、adapter 的 UDP flow claim 构造、`UpstreamTrackedStart` 请求包装与转出符号。标准候选只传递既有 `DatagramAssociationTarget`，包含不带秘密的 endpoint 和宿主实际代际；通用运行时不匹配协议配置。inventory 的迁移选择目前只覆盖已具生产条件的普通 SOCKS5 关联，未宣称所有 Datagram 工厂已完成业务迁移。Packet-carrier 和 relay 链保留各自原执行路径，不为这批引入新契约、工厂、任务或资源框架，也不启用带试验限制的 MUX/Mieru 入口。

新增实际 socket 回归核对旧 UDP operation 已不可用而业务仍能执行：两个目标与空报文复用同一关联、实际线级 TX 计量、原控制连接与关闭报告；代际撤销在宿主 fence 处拒绝且不回退拨号；UDP 禁用和严格 Packet 拒绝保持。原组/fallback、鉴权、relay 和计量集成回归在最终全量中执行。完整 workspace/all-features 在同一冻结快照上执行 **3871 通过、2 失败、175 忽略，155 个顶层目标，结束码 101**（`TEST_END 2026-10-29T04:55:41Z`），4510 项输入前后无漂移。两处失败分别为准备文件白名单尚未包含新的 UDP 准备位置，以及候选辅助函数放在 outbound facade。99 项代理派发集成（含 SOCKS5 组/fallback、复用、空闲重建及跨协议 relay）通过，没有业务测试失败。外部工具/特权环境忽略项不计已执行通过。

随后保留 facade 守卫，将候选辅助函数移到现有 `leaf.rs`，outbound 仍只做总分发；函数体逐字相同，SHA-256 为 `cde59a0ab8c4c54c023b53ccab34f93bf39fe4a93b7e3186d1d73b7f02060318`。准备边界名单仅补充 `inventory/udp/{leaf,association}.rs`，同时对这两处增加不得 await、execute、spawn 或持有 Proxy 的约束。原两个只依赖标准库的守卫目标，以各自真实 CARGO_MANIFEST_DIR 和 `rustc --test --edition=2021 -D warnings` 单独编译执行，分别 **27/0、190/0**。没有取消或弱化 outbound 的原检查。

最终源码再次冻结，Rust 1.98 严格 workspace/all-targets/all-features Clippy、格式及 **4 项定向回归（新增 3 项）** 均通过，4510 项输入前后无漂移。全量快照输入清单 SHA-256 为 `bd7b55b65e1779d9698c32b9d4d975b51f2003b10de84508f73e3862a3f180ce`；最终清单为 `e3ee2d0367cfccc8398da1d5368cf57bc70eccd02765f01fb026295b4e04a3df`。全量后仅改动候选函数的两个所在文件和两组守卫文件，函数体未改。此处记录完整运行与移位后的目标复验，不把修正前的 101 改记为最终源码的单次全量 0，也不将其表述为远端 CI。

默认 workspace、SOCKS5-only、VLESS-only 检查在移位前的相同逻辑上通过；可选组合仍有告警，不表述为全组合无告警。另一次 Rust 1.99 严格检查被未改传输代码的 `fetch_update` 弃用和 async-trait 宏 `double_must_use` 诊断阻断；没有借本批改动这些无关代码，正式静态检查使用 CI 选择的 stable（本机 1.98）。完整运行与业务定向使用 Rust 1.99、未优化且关闭调试符号、16 MiB 测试线程栈和默认测试并发。所有开发期失败原样保留，原始输出在仓库外任务产物 `zero-datagram-cutover-20261029`。

本次增量只改内核及架构记录，公开配置、控制 API、FFI、Cargo.lock 和其他项目未改；尚未提交或推送。此批完成普通 SOCKS5 UDP Flow 的旧包装收敛，不能据此关闭其他协议或 Packet/relay 迁移。

实现前的模块路径/生命周期编译诊断、未发布夹具的宿主准入拒绝均保留。实际夹具需完成宿主 start/commit；撤销在协议调用前由宿主返回 `StalePlan`，测试核对实际 stage、component、session、NotSubmitted 和未拨号，不更改生产错误语义。

## MUX native Datagram 共池迁移（2026-10-29，本地增量）

本批承接 `01968e85`，尚未提交或推送。VLESS / VMess / Trojan 的 MUX 工厂同时返回标准 Stream / Datagram，原 `StreamFactory` / `StreamOptions` 收敛为单一 `Factory` / `Options`；两种角色使用同一个协议自有 map、池身份、原始物理槽和 TaskScope driver，不新增 UDP 上传/接收任务。Datagram 直接消费协议已有逐报文响应队列，避免经 AsyncRead 将不同报文拼接或截断。通用泛型辅助只管理逻辑视图和真实 PoolUse；协议继续拥有地址、framing 和响应解码。

同一物理连接的 native UDP FIFO 限制为 8 帧数据和最多 32 个逻辑关闭位置，数据及关闭帧均有有限字节上限。逻辑创建先预留真实 END 位置，只有接受过数据才发送 END，并且始终排在先前数据之后；队列满时拒绝本次未提交报文，不清掉已接受帧，也不另起清理任务。TX/RX 及原始逻辑 control 仍持有实际物理 custody；`wait_closed` 在两个数据视图归还后移除协议逻辑登记，再归还 PoolUse，只报告逻辑资源释放。取消、Drop、逻辑计数或逻辑回执不能代替物理 owner 的 TaskScope join 和原始 native/users 关闭凭据。宿主原有转交规则保持：TX/RX 和实际 control 视图全部归还后才结束用户租约；完整逻辑回执不等于仍持有的控制能力已经归还。执行者先核对原始回执，再归还 control，只保留关闭报告等轻量事实。未完成、超时、异常或中断继续保留同一控制所有者，原始物理和任务凭据仍独立检查。

VLESS/VMess 角色提供逐报文地址；Trojan 现有 raw-MUX UDP 只提供固定目标，并显式拒绝多目标及不能表达的空报文。Proxy 的既有 Datagram 入口将真实 carrier origin 转交已有 NodeOpener 校验，没有新增第二套载体来源接口。该范围仍是私有实际 Proxy 装配的 literal-root 角色接线，完整业务 UDP 派发/计量、carrier 借用、复杂 prefix、hostname DNS、其余载体和生产可配置预算尚未关闭。普通 native MUX / Mieru 生产登记保持关闭，旧生产兼容路径继续保留。

最终源码完整 workspace/all-features：**3870 通过、0 失败、175 忽略，155 个 Cargo 顶层目标，结束码 0**（`TEST_END 2026-10-29T02:04:12Z`）。嵌套子进程的单独报告不重复计数。4510 项源码/构建输入（含 Cargo.lock）前后无漂移，清单 SHA-256 为 `9ebfd7f06d44692b959dfb4fc59e58505f28031ca9adc2349340d4aedfbb1af5`。实际 Proxy MUX 的 10 项定向回归及最终全量均通过，其中新增 4 项覆盖三个协议的 Datagram 共池、报文/多目标/过载、逻辑控制归还、清理中断和真实配置事务。另新增 2 项有界 FIFO/关闭位置回归，保留并通过原 transferred-users 约束测试。

同一生产源码已通过 Rust 1.98 的严格 workspace/all-targets/all-features Clippy、常规 workspace check、Trojan tokio-only、core/traits no-default/no_std 及格式检查。后续仅补齐下面所述 5 行测试准备逻辑，并通过 Rust 1.99 的严格 runtime/all-targets/all-features Clippy、6 项监督组、零预算用例连续 12 次和最终完整全量。常规/单包依赖检查的既有警告不等于整个仓库无警告。完整测试使用 Rust 1.99、未优化且关闭调试符号、默认测试并发和 16 MiB 测试线程栈；经实际 UDP 绑定/自收发验证的当前物理地址通过原夹具参数提供，不改变产品配置。

保留失败记录：首轮构建期间修改接口造成新旧输入混用，以 101 失败；随后固定检查发现拆分夹具关闭函数可见性遗漏，修正后重跑。曾错误地将完整逻辑回执视为转交 control 已归还，原 transferred-users 测试阻止了该约束放松；当轮全量为 **3869/1/175、155 个目标、结束码 101**（`TEST_END 2026-10-28T16:36:47Z`）。宿主改动已撤回，保留原测试；实际 MUX 回归改为核对原始回执后显式归还 control，并在回执完整但 control 仍持有时驱动真实宿主清理，确认其继续挂起。旧物理地址在机器网络切换后失效的一次运行显式中断为 130，不计通过；重新验证当前物理地址后运行。

纠正 control 归还后的下一轮全量为 **3869/1/175、155 个目标、结束码 101**（`TEST_END 2026-10-29T01:22:21Z`），4510 项输入无漂移。唯一失败为原有零协作预算 join 自唤醒测试：任务体的 oneshot 发送不能证明原任务已经返回 scheduler。原二进制单独重复 12 次均通过，不能据此抹去全量失败。夹具新增单 worker 上的后续真实任务屏障，确保原任务在无后续挂起点的情况下先返回，原始 join 仍未消费；不加 sleep、不伪造回执、不改生产代码或放宽断言。上述最终 3870/0/175 属于补齐该前提后的冻结源码，不沿用旧批次结果。

同一只读内核二进制 SHA-256 `5f7c0b1f32a46a790e132deaaf882214b3984b897fb23246a582ce2bf6fcf547` 通过冻结消费者：GUI **20.40 秒、结束码 0**，运行实例在父 stdin 关闭后自然退出 0、无 terminate/kill；面板 **73.77 秒、结束码 0**（Go 目标 72.40 秒），同一 PID/core/revision 2 的既有 TCP/UDP 跨三个发布进程保持。GUI HEAD `d39ad2787fb05052fa15065853ab71526441ccc3` 的既有 Rust 修改保留，面板 HEAD `658671e9f3978df1c79f4030fdb072a877f10bf5` 且工作区干净；工作区状态、冻结脚本/夹具及镜像摘要前后未变。消费者未设置 RUST_MIN_STACK 或测试物理地址，原 30/120 秒期限保持。这是冻结契约子集，不是实际 GUI Rust 改动、真实 GUI/TUN 或完整面板场景验收。

本批只改内核及架构记录，公开配置、控制 API、FFI 和 Cargo.lock 未修改；尚未提交或推送。原始输出在仓库外任务产物目录 `zero-mux-datagram-20261028`，不是构建/CI 依赖。普通 native MUX/Mieru 准入、真实 GUI/TUN、完整上游工具矩阵、1 CPU/1 GiB 和整体架构仍未完成。

## MUX 原始宿主所有权迁移（2026-10-28，本地提交检查点）

VLESS / VMess / Trojan 增加协议自有 MUX `ComponentFactory` 和标准 Stream 角色，依赖同一个实际 adapter map 的 roleless 池 owner。物理连接的读、写、空闲监视先组成不启动任务的 driver，再由该 owner 的实际 `TaskRegistrar` 登记；VLESS TCP 不再启动每条逻辑流的上传任务。公共载体生命周期复用 `CarrierOwner`、原始 native controller、唯一 driver view 和实际 `TaskJoinWitness`；协议继续拥有 framing、握手、缓存 key 和逻辑流。原始槽在第一次等待拨号前持有，取消/Drop、任务退出或缓存删除均不能签发原始关闭凭据。只读 observation 保留实际原始控制器的完成元数据，不持有旧 I/O、池或配置。

真实 Proxy 的既有私有 background 装配入口已接入这个 TCP 子集，标准角色通过既有 `CarrierOrigin` 把实际物理来源交回选定 upstream 校验，没有新增 TCP claimer 或第二套载体信息契约。缓存重载保留宿主拥有的 entry，仅在实际池关闭后回收该代；网络/DNS 代际变化先关闭旧原始载体并等待旧逻辑使用归还，再申请新载体。配置替换继续使用宿主确认停旧后才能构造/启动新代的事务。

**普通生产登记继续关闭。** 当前接线范围为 literal root、已支持的 TCP/TLS/WS MUX 及明确有限的内部载体上限/打开预算。原生 Datagram/TCP-UDP 共池、复杂 relay prefix、hostname DNS、其余载体/资源和生产可配置上限尚未完成，不能删除这些路径的 claim/prepared 或把它们伪装成标准角色。生产 `Proxy::new` 保留既有入口和配置/API/FFI；本批仅修改内核。完整迁移和客户端/面板兼容性仍以本批源码的实际回归为准，上一批数字不自动沿用。

本批最终源码完整 workspace/all-features：**3864 通过、0 失败、175 忽略，155 个 Cargo 顶层目标，结束码 0**（`TEST_END 2026-10-28T13:04:52Z`）。嵌套子进程的单独报告不重复计入目标/通过数。4501 项源码与构建输入（含 Cargo.lock）前后无漂移，输入清单 SHA-256 为 `eccff2db50ad254713e9edf1360d1707e9052267caf5c4a626b65ef7d3d8fb98`。新增 6 条实际 Proxy 回归覆盖三个协议的物理复用/饱和、真实任务与原始控制器关闭、网络换代、实际配置事务停旧、TLS 准备取消及清理中断重试；另有 2 条冷身份/越权与 legacy 暴露拒绝检查。原协议 TCP/UDP/MUX、WireGuard 恢复和进程回归保持通过。

首次实际接线的 5 条失败均在拨号前被拒绝：所选 `ResolvingCarrierOpener` 只提供受控候选，不能调用其未授予的同步打开入口。现消费同一实际 provider 的候选与 `prepare_stream(0)` 原始授权，并在准备和逻辑交付前复核实际 DNS 代际；没有直接拨号后备。修正后定向及最终全量均通过。早期夹具借用/Copy 检查错误与该失败保存在仓库外日志中，不计通过。

Rust 1.98 严格 workspace/all-targets/all-features Clippy、常规 workspace check、Trojan tokio-only、core/traits no-default/no_std、格式和集成测试布局检查通过。完整测试使用 Rust 1.99、未优化且关闭调试符号、默认测试并发与 16 MiB 测试线程栈；通过既有夹具参数提供已验证 UDP 自收发的物理地址 `192.168.50.138`（不是产品默认配置）。

同一只读二进制 SHA-256 `9997470eeb872e69295aac74c22506e98e40d38c68fff563609440a1b33bfe32` 通过冻结消费者入口：GUI **20.04 秒、结束码 0**，实际运行实例在父 stdin 关闭后自然退出 0、无 terminate/kill；面板 **63.29 秒、结束码 0**（Go 目标 62.42 秒），同一 PID/core/revision 2 的 TCP/UDP 跨三个发布进程保持。GUI HEAD `d39ad2787fb05052fa15065853ab71526441ccc3` 的既有 Rust 修改保留，仅运行只依赖标准库的冻结契约脚本，不验收这些外部工作区改动；面板 HEAD `658671e9f3978df1c79f4030fdb072a877f10bf5` 且工作区干净。两个工作区状态、冻结入口摘要及二进制摘要前后未变；消费者没有设置 RUST_MIN_STACK 或物理 host 覆盖，原 30/120 秒时限保持。未修改消费者源码，公开 config/API/FFI 与 Cargo.lock 相对 `f1bf1b6f` 无变动。

本批原始输出保存在仓库外任务产物目录，标识为 `zero-mux-owner-20261028`；该目录不是仓库依赖。这些通过不代表普通 native MUX/Mieru 准入、真实 GUI/TUN、完整官方工具矩阵、1 CPU/1 GiB 或整体架构完成。内核代码已固定在上述三个本地提交，未推送。验证发生在提交前的相同源码上；提交没有修改已验证实现，不将这些镜像标成按新提交重新构建的产物或远端 CI。

## VLESS / VMess / Trojan TCP 与 MUX 收敛（2026-10-27）

删除专用 `ClaimedTransportTcpLeaf` 和三个 adapter 的 TCP 构造闭包，使用已有通用 `TransportLeafTcpOperation` 持有冷准备结果和选中的实际协议 leaf。source dir 在 inventory/adapter 边界从实际配置快照交付，协议仍负责解析、相对文件路径和池身份；不再先捕获整个 adapter runtime、等第二次 prepare 才重建 leaf。TCP 的冷准备错误仍只在选中 TCP 时归一化报告，未选择的 TCP 错误不提前使 UDP/Packet claim 失败。既有 connect / relay 错误阶段、endpoint 和非 MUX relay 行为保留。

三个协议的实际 MUX 池增加 owner/完整静态 transport 配置隔离。原 key 只覆盖部分 transport 提示，可能令相同服务器和凭据的不同 outbound 共用被另一 owner 计量的连接，或复用连接绕过新的 TLS/WS 配置。新的轻量 view 仍指向同一个实际 adapter map；同一 owner 的 TCP/UDP 使用同一描述，配置未变时继续复用。不增加第二个池、worker、任务框架、公开字段或 API。VLESS 复用既有完整 carrier identity，VMess/Trojan 的描述由协议拥有的静态 transport material 生成，不包含活动池状态；未启用 MUX 的 leaf 不额外构造 scope。

新增实际 Proxy registry、真实 TLS/WS/MUX 及 echo 回归：相对 CA；同 owner 顺序重复请求复用一个物理连接；同 tag 更换不可信 CA（VLESS/VMess）或 WS path（Trojan）必须触发真实失败握手；另一 outbound 不借旧 owner 的连接；原 owner 仍可复用。这里没有宣称任意并发下只能建立一个物理连接。保留真实监听直到 worker 接管，fixture 的实际任务收到取消后全部 join；这些 fixture 结束不是产品原始 native close 凭据。

本轮最终源码完整 workspace/all-features：**3856 通过、0 失败、175 忽略，155 个目标，结束码 0**（`TEST_END 2026-10-27T12:25:49Z`）。严格 all-targets/all-features Clippy、原进程测试 22 项、新增真实 TCP/MUX 2 项、运行时边界 190 项及架构守卫 27 项通过。4468 项源码/构建输入（含 Cargo.lock）在运行前后没有漂移，清单 SHA-256 为 `241a7a0ad0ab4d62280598de881d2ec753429cf9f41650e357ac34ffdc600ffe`。完整测试使用 Rust 1.99.0、未优化且关闭调试符号、默认测试并发和 16 MiB 测试线程栈；当前实际物理地址经 UDP 自收发确认后通过原 host 参数提供。

此前一次完整运行因状态端口启动超时失败（101），其原始临时日志因机器重启丢失，仅保留会话观察；后续一次完整运行捕获到实际子进程在原 6 秒内未退出、无输出且端口未开放，夹具显式终止并回收，随后停止该轮（130）。单独原进程测试可通过，定向采样也曾干扰另一业务测试；这些失败不计通过。另确认 gRPC 构建脚本向 Cargo 声明了不存在的相对 proto 路径，现改为实际文件路径；生成代码摘要不变，最终完整 Cargo 运行直接复用产物、不再重复编译链接。该构建修复和最终通过不证明偶发冷启动超时的根因已经解决，也不代替冷启动或低资源资格。原测试期限与产品行为未放宽。重启后的原始记录保存在仓库外任务产物目录，标识为 `zero-v003-transport-20261027`，不增加仓库临时归档或构建依赖。

同一只读二进制（SHA-256 `5bf0a15bdf8555b63abd7f9cd820d1cf2900d16fab969dfe0902f0b9853c8ee0`）通过冻结消费者入口：GUI **19.21 秒、结束码 0**，实际运行实例在父 stdin 关闭后自然退出 0、无 terminate/kill；面板 **60.36 秒、结束码 0**，同一 PID/core/revision 2 的既有 TCP/UDP 跨三个发布进程保持。GUI HEAD `d39ad2787fb05052fa15065853ab71526441ccc3` 的其他既有 Rust 修改保持原状；面板 HEAD `658671e9f3978df1c79f4030fdb072a877f10bf5` 且工作区干净。两个工作区状态、冻结入口和二进制摘要前后未变，消费者未设置 RUST_MIN_STACK 或测试物理 host 参数，原 30/120 秒期限保持。这是冻结契约子集验证，不包含真实 GUI/TUN、完整外部互通和 1 CPU/1 GiB 资格。

仍未迁完生产池的完整原始 owner / TaskScope / 转交用户关闭链；旧 TCP claim 和 UDP handler 大接口也仍有其他调用者。原 host 确认停旧后再构造/启动替代资源的事务与 native 工厂门槛未改，普通 Mieru 生产登记保持关闭。只改内核，已分组提交到本地 develop，未推送。

## 候选关闭优先恢复、SS 启动监督与 HY2 TCP 收敛（2026-10-27）

失败配置的回滚先清理该候选留下的真实 bind reservation，取得原生关闭、TaskScope 完成和原故障发布结果，再恢复 last-good 组件、TUN、监听和 DNS。清理未确认时保留 owner 并阻断资源恢复；已关闭故障只能按有限的原实例身份消费，批量清理优先返回仍未完成的原 owner，不能让较早的已关闭故障掩盖它。WireGuard 的原 crypto owner 和 Noise 历史仍由既有恢复胶囊保留，没有通过重新握手或清空历史恢复。

SS SIP003 启动监督保持同一个 pending TCP probe，并在等待期间读取真实 Child 退出。已退出子进程不再等到 TCP 探针期限才报错，原失败、Child 和 ExitStatus 仍留在同一个 plan，禁止原地重启。启动失败夹具明确挂起真实探针，再以实际子进程退出作为失败事实，不依赖并发端口探测的时序；没有伪造成功探针。独立前后对照使用相同真实 Child/挂起探针回归：旧生产函数 3 秒超时失败，修复后 0.01 秒通过并确认 ExitStatus 7 和实际回收。这只证明退出监督分支，不代替完整 SIP003 进程/转交 I/O 原生资格。

HY2 source-relative TCP claim 直接复用持有实际协议 leaf 的 `SessionTcpConnectOperation`，删除 `ClaimedSessionTcpLeaf`、准备闭包与 `claim_session_tcp_leaf_with_source`；路径由协议 leaf 的既有构造方法物化，同一 adapter 的认证连接池保留。新增实际 Proxy/registry、相对 CA 文件和 QUIC 服务端回归，要求两次独立 TCP 准备在同一个认证连接上完成。其余 transport claim、MUX/QUIC 与 UDP/device 前缀仍有迁移债务，普通 Mieru native 生产登记继续关闭。

本批仅改内核，公开配置、API、协议帧与上游版本基线不变，未提交或推送。最终源码的完整 workspace/all-features：**3854 通过、0 失败、175 忽略，155 个目标，结束码 0**（`TEST_END 2026-10-27T07:12:54Z`）。361 项定向回归、Rust 1.98.0 严格 all-targets/all-features Clippy 与 9 组独立 feature 构建通过；包含原 WireGuard 完整目标、SS 插件组、原始关闭/真实 Scope 等待、HY2 相对 CA 共池和生产边界守卫。

检查到完整运行结束的 4466 项源码/构建输入（含 Cargo.lock）指纹一致，清单 SHA-256 为 `ab42ed6ca6bb61ffec052e4304880e910f60b21834ca2a58bfd17617e18fa1aa`。完整测试使用 Rust 1.99.0、未优化且关闭调试符号、默认测试并发与 16 MiB 测试线程栈；实际物理 host 参数经原夹具入口自收发确认，不修改产品默认地址。中间检查的两个测试接线编译错误及一次将 test-only 服务端误计入 adapter 生产扫描的失败均保留；修正测试接线并使用既有非测试源码扫描器，生产禁止项和原测试期限未放宽。SS 子进程先于探针退出也被保留为合法早退失败；专门挂起探针的真实 Child 回归单独证明等待期间的退出监督。原始输出、前后对照和源码指纹保存在系统临时目录。

下方两个全量失败属于上一批源码，历史记录保留；本批通过原失败目标、直接资源回归及完整工作区后，关闭这两个具体分支的缺陷，不把 Z003-04 / Z003-05 或整体协议迁移标为完成。同一本批只读二进制（SHA-256 `253772d15eef83d54b2d8482771e1ff5e8dae0dad76ea71018356edb6211e392`）独立运行冻结入口：GUI **27.18 秒、结束码 0**，实际运行实例在父 stdin 关闭后自然退出 0，无 terminate/kill；面板 **77.05 秒、结束码 0**，同一 PID/core/revision 2 的既有 TCP/UDP 跨三个发布进程保持。消费者不设置 RUST_MIN_STACK，原 30/120 秒限额不变，两个冻结检查入口的摘要前后未变。GUI HEAD 为 `d39ad2787fb05052fa15065853ab71526441ccc3`，该工作区另有本对话以外的 Rust 修改；本批仅运行未变且只依赖标准库的契约脚本，不修改或验收这些工作区变更。面板 HEAD `658671e9f3978df1c79f4030fdb072a877f10bf5` 且工作区干净。此结果不代替真实 GUI/TUN、冷启动/1 CPU/1 GiB 或完整外部互通资格。

## 池化 TCP 执行与剩余包装收敛（2026-10-27）

Mieru 的实际 TCP 池化 relay 改用已有通用 socket 执行入口，删除 `ClaimedSocketTcpLeaf`、relay claim 闭包及 adapter 专属 `PreparedMieruTcpRelay`。协议继续通过同一个实际 `ClientPool` 做 lazy lookup，复用原 prefix identity / network generation；命中时不打开额外前缀。通用执行器只附加一次 I/O 观察，Mieru 适配器保留协议调用桥，不再实现 prepared operation 执行器。

socket 与 transport TCP 分别由一个拥有实际 handshake / leaf 的操作实现 connect 和 relay 两个契约，删除重复的 connect/relay 操作类型。TCP 直接持有协议 leaf，移除 `PreparedTransportLeaf` TCP 包装及两个只构造操作的转发函数；该包装仅留给尚未迁移的 managed UDP。另删除 socket/session 执行时只借用同一个 handshake 的 `PreparedSocketTcpOperation` / `PreparedSessionTcpOperation`。没有增加第二套协议 SPI、连接池或后台任务。

池化前缀打开失败不再经过字符串重建而丢失原始错误。新增回归使用实际 registry/adapter 池及真实 OS 连接失败，检查原生错误来源保留且 carrier 只尝试一次；既有真实 SOCKS5→Mieru 池化回归加强为同时检查累计建连数，防止命中后打开并丢弃多余前缀。同时修正纯 TCP feature 下健康状态字段错误依赖 UDP 的条件编译遗漏；独立检查覆盖 tunnel/session/transport-session 三条路径。该范围定向回归通过，完整测试尚未通过；下节 3848 项通过记录属于前一批源码，不覆盖本批。

本轮只修改内核，公开配置/API/协议帧未变，未提交或推送。通用宿主替换事务不变，普通 Mieru native 生产登记保持关闭；这次旧执行壳收敛不替代 system/detour、原生载体及资源预算等剩余资格。本批消费者独立检查结果见下文，原 30/120 秒限额及检查入口未改。

上一池化 TCP 批次当时未全绿：严格 Clippy 与 8 组精简 feature 构建、323 项定向回归均通过；首轮完整测试为 **3848/1/175、155 个目标、结束码 101**，WireGuard 端口迁移失败后旧监听恢复断言失败。同一冻结源码的单项与 10 轮默认并发 WireGuard 全目标复核均通过，原因尚未确定，Z003-04 仍保留此缺陷。第二轮同为 **3848/1/175、155 个目标、结束码 101**（`TEST_END 2026-10-27T05:10:10Z`），WireGuard 通过，但未改动的 Shadowsocks 插件启动失败用例意外获得 lease，Z003-05 同样待收敛；不通过改断言、延长期限或反复重跑来宣布缺陷关闭。两轮全量及严格检查间，4464 项源码/构建输入（含 Cargo.lock）指纹未变，清单 SHA-256 为 `d0fdfb688a88aa4ffb047ce34c627396385c7219fd160e0ff633ab2535ae77d2`。完整测试仍使用 Rust 1.99.0、未优化且关闭调试符号、默认并发及 16 MiB 测试线程栈；严格检查使用 Rust 1.98.0。原始日志在系统临时目录。

同一本批只读二进制（SHA-256 `eb5687e5dca9a8e283840fcfbb9700f4588fa81a3b4aec558fa75619d21796c1`）独立运行冻结消费者入口：GUI **15.16 秒、结束码 0**，实际 GUI-owned 运行实例在父 stdin 关闭后自然退出 0，无 terminate/kill；面板 **50.17 秒、结束码 0**，同一个 PID/core/revision 2 的既有 TCP/UDP 跨三个发布进程保持。GUI `d39ad2787fb05052fa15065853ab71526441ccc3`、面板 `658671e9f3978df1c79f4030fdb072a877f10bf5` 的源码和检查入口摘要前后未变，不设置消费者 RUST_MIN_STACK，也未调整原 30/120 秒期限。消费者子集通过不覆盖上述两个全量失败，不关闭冷启动/1 CPU/1 GiB/真实 GUI-TUN 资格；下节旧二进制的 GUI 超时记录仍保留历史语境。

## TCP 标准角色与兼容接口收敛（2026-10-26）

混合 TCP relay 链直接从实际目录选用最后一个未迁移节点之后的普通 Stream 角色；标准节点不再依赖整链 `ClaimedRelayChain` 或旧 TCP claimer。未迁移池化节点及其前面的整段前缀保留原有执行器和长期资源所有者，不能将业务请求的标准流当作共享池载体；这部分仍等待真实 carrier 借用及原始关闭契约。标准中间/最终 hop 保存真实组件代际的弱引用，缓存身份包含该代际；保留连接的重开同时检查静态依赖和当前实际 owner。先取得宿主角色许可，再执行选定的兼容前缀；该一次性桥不提供 root 拨号、Datagram、受控借用或原始关闭凭据，也不伪造 prefix RoleId。真实请求身份与错误来源保留，可能已提交前缀的错误不得重放，未知地址元数据保持不可用。

普通 socket TCP 直接复用现有 `SocketTcpConnectOperation`，Direct 复用 `DirectTcpConnectOperation`，删除各自重复 claim 包装；Mieru 自有 relay、source-relative/池化接口以及 UDP/device 借用的 TCP 前缀继续保留兼容路径。没有新增资源池、后台任务、协议 SPI 或生产工厂。原宿主确认停旧后才构造/启动新资源的事务不变，普通 Mieru native 登记仍关闭。

明确请求的组件目录准备失败直接返回保留的原错误，不再静默选用旧 TCP 执行器。新增回归使用同一真实装配的 socket 执行器和标准角色、保留绑定的真实监听器，覆盖三跳合法兼容前缀/标准后缀组合、半关闭/逐跳计量、退休角色在拨号前拒绝、实际握手拒绝不重放、弱引用不延长旧 owner 寿命，以及目录失败不能降级的负向准入检查。目录回归只移除同一真实装配的 SOCKS5 兼容投影，验证 factory-only Stream 后缀不需要旧 TCP 接口，并保留 Mieru 实际兼容池前缀，未开启 Mieru native。仅修改内核，未提交、未推送。

2026-10-27 最终 TCP 源码的完整 workspace/all-features：**3848 通过、0 失败、175 忽略，155 个目标，结束码 0**，`TEST_END 2026-10-27T01:53:28Z`。新增 6 项回归、进程/控制/派发/运行时及工作区边界 373 项、实际主机地址复核 110 项通过。Rust 1.98.0 严格 all-targets/all-features Clippy、SOCKS5-only、Shadowsocks-only、managed-stream-runtime-only 和 no-default 编译通过；完整测试使用 Rust 1.99.0、未优化且关闭调试符号、默认并发及 16 MiB 测试线程栈。检查与最终测试间 4463 项源码/构建输入（含被忽略的 Cargo.lock）指纹一致；清单 SHA-256 为 `26b1b04038ed0e753b96eebb4a3954c28ef108f49d6c2690139f5e4770d8e043`。这些结果不关闭其余协议或 ALL17。

失败和中止记录保留：早期 fixture 错把请求流当作未迁移池的长期前缀，实际原生所有权栅栏拒绝后，fixture 改回合法的兼容前缀/标准后缀组合；未放宽借用栅栏。首轮完整执行 **3836/12/175**：首个进程在原 6 秒内未就绪，测试按既有逻辑停止并回收该进程，串行锁中毒再引出 10 项连带失败；另 1 项是工作区旧白名单漏列新的 inventory 准备模块。守卫只放行该精确模块，同时禁止 await、Proxy、协议私有配置和任务创建；原启动期限未改，最终完整进程 22 项与守卫 27 项均通过。跨日主机地址从 `.103` 变为 `.101`，另一轮出现 8 项 `AddrNotAvailable`；等当前 fixture 自然结束后停止 Cargo，该轮以 143 结束，未计通过。真实 UDP 自收发及 110 项地址相关复核通过后，使用有效地址重新完成上述全量。原始输出保存在系统临时目录，不增加仓库临时归档。

消费者使用同一只读调试产物，SHA-256 `82c1b59d9c29ffd60722d3e27684473f64df618228e493ea01f7ddb4818fde93`。**当前 GUI 检查未通过**：冻结入口两次在 `--version` 的原 30 秒期限超时（30.42 / 30.61 秒），未进入实际 GUI 管理的 run 实例。独立诊断中构建信息在 0.07 秒输出，实际二进制 SHA-256 在 40.64 秒才输出、进程 40.66 秒自然退出；采样指向未优化 `sha2` 软件压缩实现。诊断不是 GUI 验收，未修改源码、哈希输出、检查入口或期限。

同镜像面板按原入口独立验证，**103.89 秒、结束码 0**，同一 PID/core/revision 2 的 TCP/UDP 跨三个发布进程保持。GUI `d39ad2787fb05052fa15065853ab71526441ccc3`、面板 `658671e9f3978df1c79f4030fdb072a877f10bf5` 及冻结检查入口的源码和摘要前后未变；消费者未设置 RUST_MIN_STACK，原 30/120 秒限额保持。GUI 失败未因面板通过而关闭，下面旧批次的消费者通过结果不能覆盖本批；面板连续性也不代替原生资源关闭凭据。当前不宣称生产构建、冷启预算、真实 GUI/TUN 或 1 CPU/1 GiB 资格完成。

## 前一批标准 Datagram 关联迁移（2026-10-26）

SOCKS5 UDP 关联改为由通用运行时直接执行实际标准 `DatagramOutbound` / `DatagramSession`，移除 `UpstreamUdpHandlerProvider`、handler 向量、`UpstreamAssociationHandler`、不透明 upstream resume 构造与类型探测，以及 adapter 内的 native 会话执行壳；`ClaimedSocks5UdpLeaf` 同步删除，直接准备 endpoint 操作。协议继续拥有认证、UDP ASSOCIATE 握手和帧处理；适配器只投影 endpoint 事实，配置到实际组件的映射留在 inventory。通用 upstream 特性显式依赖 UDP 响应支持，移除过去由 SOCKS5 顺带启用的隐藏组合要求。

关联引用使用实际 `ComponentInstanceId`，不再保留凭据或旧配置，也不强持有旧角色/池。原有关联管理器使用 `Empty / Active / Retiring` 枚举保存同一个实际所有者，活动与退休状态不能同时存在；切换、空闲关闭或发送失败后保留原控制器，取消和清理失败不移除它。代际失效保留标准 `ChannelError`，清理超时/错误保留原来源并归为本地失败，避免将尚未尝试的新上游计为不健康。收到原控制器的完整关闭回执后才能打开替代会话。现有 wire-byte 计量和单接收者语义保留，没有新增资源池或后台任务。

这是 Z003-03 / Z003-06 的一条生产路径收敛，不代表 SOCKS5 双原始 provider、TCP claim/prepared、其余 UDP/packet-path、Mieru 普通 native 登记或 ALL17 已完成。

本批最终源码的完整 workspace/all-features：**3842 通过、0 失败、175 忽略，155 个目标，结束码 0**；测试前后 4455 个源文件指纹一致。新增真实 EOF/取消重试、实际 owner 代际拒绝及清理错误来源回归通过。Rust 1.98.0 严格 all-targets/all-features Clippy、SOCKS5-only、upstream-association-runtime-only、managed-stream-runtime-only 编译通过。完整测试使用 Rust 1.99.0、未优化且关闭调试符号，保留断言、默认并发与 16 MiB 测试线程栈，继续使用夹具既有的实际物理 host 参数。

较早一轮为补齐本地清理错误归因而中止（130）；随后完整运行暴露 3 条源码边界断言失败（101）。已将具体协议特性开关移入测试文件，使用既有生产源码扫描器区分错误来源测试，并将 SOCKS5 断言更新为直接接入标准操作且禁止恢复旧 helper。修正后架构目标单独 190 项通过，最终完整重跑也通过；未放开生产协议分支、resume 探测或 handler SPI。失败和中止不计通过，原始产物保留在系统临时目录。

同一只读二进制（SHA-256 `a1a004ab13446e44737a7d4959b9fff1d9f27c9db9d6b6fb7a13c13e5f37efe2`）：GUI 契约检查 **16.84 秒**，实际运行实例自然退出 0、无 terminate/kill；面板发布回归 **64.70 秒**，同一 PID/core/revision 2 的 TCP/UDP 跨三个发布进程保持。消费者源码与既有检查入口未改，不设置 RUST_MIN_STACK，原 30/120 秒限额保持。面板连续性检查不代替原生关闭资格；本批不包含真实 GUI/TUN、完整外部互通或 1 CPU/1 GiB 资格。

仅调整内核，尚未提交或推送；普通 Mieru native 生产登记保持关闭。下述 3838 项及 GUI/面板结果属于前一批源码。

## 前一批续推进结果（2026-10-26）

补齐原生 TCP DNS 打开失败后的既有后备查询：实际原始控制器和转交用户确认关闭后，类型化错误才允许下一次申请；保留底层 OS 错误，清理未完成继续阻断。实现与回归见[原生 DNS](native-dns.md)。system resolver、detour、其余 failed-open 和既有池化迁移仍未闭合。

继续收敛[逻辑 UDP 会话执行链](../../../crates/proxy/src/runtime/udp_flow/registered/logical.rs)：使用显式类型保存和分发会话，删除一个 Any 包装及两个 downcast，覆盖 VLESS reverse UDP 与 Packet/Flow 转换入口。旧 opaque resume 仅用于未迁移路径；本轮未改 SOCKS5 UDP ASSOCIATE 等剩余桥，也未新增管理器、后台任务或资源池。[新增回归](../../../crates/proxy/src/runtime/udp_flow/registered/logical/tests.rs)覆盖直接发送、多目标与空报文、旧引用失效，以及原始响应任务实际 join 后才重新读取。

前一批源码的完整 workspace/all-features：**3838 通过、0 失败、175 忽略，155 个目标，结束码 0**。新增定向 3 项、原兼容路径 2 项通过；实际 VLESS reverse TCP/UDP 突发、主动返回与重连，以及 WireGuard TCP/UDP 网络切换恢复通过。Rust 1.98.0 严格 all-targets/all-features Clippy、SOCKS5-only、Shadowsocks-only、managed-stream-runtime-only 编译通过。完整测试使用 Rust 1.99.0、未优化且关闭调试符号，保留断言、默认并发与 16 MiB 测试线程栈。

首轮全量的自动 host 探测选中本机 TUN 地址，平台 egress 5 项及 endpoint 3 项超时；已保留失败并中止该轮（130）。使用夹具既有 `ZERO_TEST_HOST_IPV4=192.168.0.103` 指定实际物理地址后，相关目标原断言分别 5/4/25 项通过，最终完整重跑也使用同一参数并通过。该地址是本机验证输入，不是产品默认值；在其他环境选择实际已分配的非回环物理地址。未修改夹具源码、超时、客户端服务或系统路由。

同一只读新二进制（SHA-256 `42f06cfec8e0174a6b5194ed7e6043ad8d3be4055d7711d3e18f7a87dd8d57e7`）：GUI 契约检查 **19.45 秒**，实际运行实例自然退出 0、无 terminate/kill；面板发布回归 **56.06 秒**，同一 PID/core/revision 2 的 TCP/UDP 跨三个发布进程保持。消费者源码未改、不设置 RUST_MIN_STACK，原 30/120 秒限额保持。本对话只调整内核，消费者仅运行冻结验收入口。这不包含真实 GUI/TUN、完整外部互通或 1 CPU/1 GiB 资格。

本轮清理与续推进尚未提交、未推送，普通 Mieru native 生产登记保持关闭。

## 检查点结果

`8acd1cca` 保存的提交前源码：完整 workspace/all-features 为 3832 通过、0 失败、175 原有忽略项，155 个目标；严格 Clippy、DNS-only / no-default 编译通过。完整测试使用未优化、关闭调试符号的本地构建，保留断言、默认并发与 16 MiB 测试线程栈。

同一新二进制的 GUI 契约检查 12.70 秒、面板发布回归 53.44 秒通过；GUI 实例自然退出 0，无 terminate/kill，面板同 PID/core/revision 2 的 TCP/UDP 跨三个发布进程保持。消费者未设置 RUST_MIN_STACK，原 30/120 秒限额保持；消费者源码未改。这些结果不包含提交后的远端 CI、真实 GUI/TUN 或完整 1 CPU/1 GiB 资格。

## 历史记录

按用户要求，工作树已清理历史运行日志、临时脚本、源码快照、诊断副本、重复 gzip 和逐轮清单。它们保存在 Git 检查点 `8acd1cca` 中，失败、中断和未执行状态没有被改写为成功；历史说明中原路径均按该检查点解释。

需要复查具体记录时使用 `git show 8acd1cca:design/v0.0.3/implementation/<原路径>`。例如 `git show 8acd1cca:design/v0.0.3/implementation/evidence/parallel-progress/wireguard-stopold-recovery-review.portable.md.gz | gzip -dc` 可读取原归档正文。后续原始输出写到已忽略的 docs/ 或系统临时目录；本目录保留当前技术说明和可审阅结果，不再堆积运行产物。原始冻结方案、17 张任务卡和 baseline 不在此次清理范围内。

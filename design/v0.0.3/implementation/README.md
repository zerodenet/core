# 当前实施状态

版本 v0.0.3，分支 develop。云端 PR #124 的合入点是 `b963c864`；后续本地检查点为 `8acd1cca`。ALL17 / G0–G10 整体尚未完成，普通 Mieru native background 生产登记保持关闭。

截至 `f1bf1b6f` 的上一批本地变动已按职责分组提交到 develop。2026-10-28 的 MUX 原始所有权迁移亦已按基础设施 `b1fd79a8`、协议实现 `12133be4`、Proxy 装配与回归 `91b3a2a7` 分组提交；工程规则与本记录随文档提交保存。全部仅为本地检查点，未推送。下文各历史批次的“未提交”仅记录其验证时状态，不代表当前 Git 状态。分组提交没有改变已验证的内核源码或扩大生产准入范围。

## 当前实现

- [独立 zero-runtime 与宿主事务](runtime-extraction.md)：同一个宿主拥有目录、任务监督、配置操作和资源代际；旧资源确认清理后才能构造/启动替代资源。
- Mieru 的[共享池](mieru-pool.md)、[完整 profile 与角色](mieru-profiles.md)、[DNS/UDP underlay](mieru-carriers.md)、[prefix](mieru-prefix.md)和[跨 owner 原始转交](cross-owner.md)已分批接线。
- [原生 DNS](native-dns.md)已覆盖 UDP/TCP/DoT/DoH，[DoQ](native-doq.md)已接通实际 endpoint、驱动、用户/视图及原始 main/probe 关闭凭据。
- 剩余功能和验收条件见[清单](outstanding-20261025.md)。迁移仅覆盖既有能力；池化保留现有语义，插件清理覆盖实际拥有的进程/I/O，不扩成通用资源调度或进程沙箱。

## 对照原方案的整体进度（2026-10-28 复核）

原 Page revision 7 的 M0–M6 / G0–G10 是完成标准；17 张任务卡是逻辑交付边界，旧的“未开始”和检查点状态不代表当前代码进度。现在是基础骨架已落地、多条生产路径已迁移，但协议与其他层的职责迁移仍在进行，不能记为“架构完成、仅剩验收”。

| 原阶段 | 当前进展 | 未关闭的条件 |
| --- | --- | --- |
| M0 / G0 基线 | 版本化完整方案、任务卡、固定消费者与部分行为基线 | 完整资源/性能与平台基线仍有缺项；旧数字不能替代最终源码的比较 |
| M1 / G1 中立契约 | 公共组件、角色、通道、网络、错误与生命周期契约已有实际调用；traits/core 保持 no_std | 旧接入与出站接口仍有生产调用，契约存在不等于所有协议已迁完 |
| M2 / G2–G3 实例与出站 | 实际工厂、代际、依赖和普通 Stream 路径；Engine 目标解析返回中立身份；混合 TCP 已部分收敛，受控 MUX TCP/TLS/WS 子集已接入实际宿主及标准 Stream | 剩余池化、复杂载体与部分 TCP/UDP 前缀仍走兼容路径；MUX/Mieru 的普通 native 登记未开启 |
| M3 / G3–G4 UDP 与接入 | 逻辑 UDP 显式会话、SOCKS5 标准 Datagram；统一接入循环与协议自有握手已有迁移 | 其余 Any/resume/handler、InboundListenerCapability 及 MUX/QUIC 接入的整体标准化未关闭 |
| M4 / G5 宿主与事务 | 独立 RuntimeHost、配置操作所有权、任务监督、资源代际，以及已接管资源的确认停旧替换事务 | Engine 仍发起路由/计划编译与文件持久化，宿主准备/提交职责未全部迁出；领域快照安装与 revision 仍应由 Engine 保留 |
| M5 / G6–G7 基础服务与 Packet | DNS 原始查询 owner、Network 契约、公共 Flow/Packet 转换与设备接线已有覆盖 | system/detour/池化 DNS、config 资源加载、完整设备替换和系统恢复仍有实际实现缺口 |
| M6 / G8–G10 投影与收尾 | 公开配置/API/事件兼容边界保留；已有多批内核全量及冻结消费者检查，结果按下方对应源码批次记录 | 内部观测/控制投影整体对账、旧桥删除、生产可配置资源上限、完整互通/特权平台、真实 GUI/TUN、1 CPU/1 GiB 和最终交付未齐 |

需要纠正先前剩余清单的范围：协议资源之外，`crates/config/src/compile.rs` 仍读取 GeoIP 并准备规则资源，`crates/engine/src/runtime/configuration.rs` 仍编译与持久化配置。这对应原 PR08B 和 PR07B，是实际职责迁移债务；宿主已有串行事务不能代替这些迁出条件。规则加载发生在构建/重载，不能误写为逐连接加载。详见清单 Z003-09 / Z003-10。

Z003-07 也包含实现工作：例如 `component/native_dns.rs` 的查询/原始资源上限仍为内部常量 16，适用上限尚需接入既定生产配置管理，再做 1 CPU/1 GiB 资格；不能全部归为“只待验收”。按用户决策保持保守默认和按需分配，不为配置上限再增加通用资源管理框架。

当前传输层没有 config/engine/proxy/具体协议的生产依赖，runtime 对 proxy 的依赖只在测试侧；不能把已解除的依赖重新列作缺陷。原 Page 的 `runtime → proxy` 已按用户批准调整为 `proxy → runtime`，见[修订](runtime-extraction.md)，不是未批准的目标偏移。上述表格不折算任意完成百分比，也不将部分协议迁移或本地测试通过算作 G0–G10 整体关闭。

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

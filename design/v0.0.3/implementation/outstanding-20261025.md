# v0.0.3 本地检查点与剩余缺口

记录日期：2026-10-25。分支 develop，承接云端 PR #124 合入点 `b963c864a473f68f8d6a8444764e3e88761e6c04`。本地检查点保存已实现和已验证的工作，ALL17 / G0–G10 整体目标仍未完成，普通 Mieru native background 生产登记保持关闭。

下列项目区分原生路径的功能缺口、架构迁移债务和未执行的资格验收。未完成原生迁移不等于现有兼容路径不可用；不能通过开启未资格工厂或删掉兼容路径来关闭这些项目。

## 本次保存范围

- 独立 zero-runtime、共享 RuntimeHost、配置操作所有权、资源代际及确认停旧后再启动替代资源的事务。
- Mieru 实际共享池、完整 profile/凭据替换、TCP/UDP 角色、endpoint、prefix、跨 owner 和无独立 driver 原始转交的分批接线。
- 同一个 Proxy / DnsSystem / RuntimeHost 中的原生 UDP、TCP、DoT、DoH、DoQ 查询所有者、真实任务监督及原始关闭凭据。
- 通用 controlled metadata pool，以及通用 inventory 构造与 UDP handler 的独立功能门控。
- 对应协议、CI 路径、工程规则、兼容账本、回归与历史失败证据。协议上游版本基线保持不变。

代码验证来自[最终源码快照](local-native-doq-20261025/source-final.json)和[验证清单](local-native-doq-20261025/validation.json)：Rust 1.99 完整 workspace/all-features 为 **3832 通过、0 失败、175 忽略，155 个目标**；Rust 1.98 严格 all-targets/all-features Clippy、DNS-only / no-default 编译通过。完整测试为未优化、关闭调试符号的本地构建，保留断言、默认测试并发及 16 MiB 测试线程栈。

GUI 与面板使用同一个新二进制，分别 12.70 秒、53.44 秒通过原契约检查；消费者未设置 RUST_MIN_STACK，原 30/120 秒限额不变。GUI 的实际运行实例退出 0，无 terminate/kill；面板在同一个 PID/core/revision 2 下跨三个发布进程保留 TCP/UDP。消费者源码未改，公开配置、API、Engine、FFI、gRPC、根应用和 Cargo.lock 对检查点起点无差异。

这些是提交前源码及产物的验收记录，二进制构建信息仍指向当时的 base；不把它标为提交后重新构建的产物或新提交的远端 CI。各旧批次中的“未提交/未推送”保留当时语境。

原始命令输出保留全部字节，包括终端尾部空白；[Git 属性](../../../.gitattributes)只对这些 local 证据目录的 txt 捕获关闭空白错误检测，源码与正文维持常规检查。

## 剩余功能缺口和迁移债务

| 编号 | 优先级 / 原计划 | 当前缺口与影响 | 关闭条件 |
| --- | --- | --- | --- |
| Z003-01 | P1 / PR08A，关联 PR04B、PR05 | 原生 DNS 的 OS/system resolver、detour、打开失败的类型化重试与真正池化路径尚未闭合。内部原生路径在缺少授权 I/O 前拒绝，普通 Mieru native 生产登记仍关闭。 | 实际查询/载体所有者、TaskScope、原始 main/probe/driver 关闭凭据及依赖关系完整；取消、网络切换、配置替换和失败重试均通过，确认旧原始资源结束后再准入新资源。 |
| Z003-02 | P1 / PR04B、PR06B | VLESS/VMess 等 gRPC/H2/MUX 及 Hysteria2/其他 QUIC 的实际池化生产入口仍有迁移缺口。已有库或关闭接口不能代表宿主已使用同一个实际池和驱动。 | 将实际共享池、endpoint、原始 I/O、驱动及转交用户纳入同一宿主所有权；通过原固定上游基线互通和故障/替换回归后登记标准角色。 |
| Z003-03 | P1 / PR05A、PR05B | 剩余 UDP/packet-path 与 SOCKS5 UDP ASSOCIATE 双原始资源等路径尚未完成统一角色迁移；关联、连续性、返回归属和原始关闭需逐路径闭合。 | 使用实际可执行 Datagram/Packet 会话，覆盖多目标、关联控制、单接收者、背压、发送投递状态、逻辑归还及原始关闭，保留正确计量。 |
| Z003-04 | P1 / PR09 | WireGuard deferred device、真实设备替换、Packet/TUN 回程和系统恢复尚未取得完整资格。现有 raw-IP/设备支持不等于完整新宿主迁移。 | 接通实际设备和原控制器，验证密钥/peer 安全状态、回程、源地址、MTU、替换与失败恢复；确认旧设备/任务/I/O 和自有系统状态收尾后再启动新设备。 |
| Z003-05 | P1 / PR04B、PR07B | SS SIP003 已有实际 Child/readiness/monitor 所有者，但直接子进程被 reaped 仍不足以证明后代进程和转交 I/O 都结束。 | 明确并实现实际后代与转交 I/O 的关闭范围，使用原控制器和真实退出证据接入宿主事务；未资格路径保持兼容所有权。 |
| Z003-06 | P2 / PR03–PR06、PR11 | 旧 claim/prepared 转发和 Any/downcast UDP resume/handler 兼容链仍有调用者；协议接入标准尚未覆盖全部现有路径。 | 调用者迁到已构造的标准角色和协议拥有的可执行会话，回归通过后删除对应旧桥；同类协议接入只修改配置转换、协议实现、注册和必要测试，不要求通用运行时识别协议私有状态。 |

P1 表示阻断对应原生生产启用或原计划阶段关闭；不是对当前兼容执行路径已发生线上故障的断言。证据和协议覆盖明细见[迁移状态表](runtime-extraction.md#剩余协议迁移顺序与阻断条件)、[原生 DNS](native-dns.md)及[DoQ](native-doq.md)。仍在使用的旧接口可在[capability](../../../crates/proxy/src/protocol_registry/capability.rs)和[UDP resume](../../../crates/proxy/src/runtime/udp_flow/managed/flow/resume.rs)核对。

## 尚未完成的验收与交付

| 编号 | 优先级 / 原计划 | 缺少的证据 | 完成标准 |
| --- | --- | --- | --- |
| Z003-07 | P1 / PR01、PR07B、PR08A、PR11 | 生产可配置资源预算与当前最终实现的 1 CPU/1 GiB 完整压力/性能资格。内部 DNS 的 16 查询/16 原始额度、DoQ 最多 8 producer 组属于已测内部上限。 | 将适用上限纳入既定配置管理；实测稳态、突发、耗尽、空闲回收、网络/配置切换、失败恢复和退出。记录内存、FD、任务、吞吐和延迟；有效安全历史与活跃业务不能被通用淘汰规则清掉。 |
| Z003-08 | P1 / PR06B、PR09–PR11，G0–G10 | 当前检查点的完整官方工具互通、跨平台/特权 TUN、Packet 模式及原生业务/fallback/MUX/故障/计量组合矩阵。175 个忽略项不计已运行通过。 | 在相应环境执行并记录精确源码、工具/协议基线及原始结果；补齐真实 GUI/TUN 与面板所需场景，完成远端 CI、整体任务卡对账及最终旧桥清理。 |

本地 commit、推送、远端 CI、整体架构完成、发布和部署分别记录。本检查点仅进行本地提交；普通生产登记不因提交或本地测试通过而开启。原计划与验收标准仍见[17 张任务卡](../task-cards.md)和[交付规则](../delivery-policy.md)。

## 后续顺序

先闭合 Z003-01 的实际 DNS/provider 路径，再按真实所有权依赖推进 Z003-02 至 Z003-05，逐路径移除 Z003-06 的旧桥。Z003-07 的预算和资源证据与各原生路径同步补齐；最终以 Z003-08 的完整矩阵和 G0–G10 对账作为整体完成条件。每一步保持旧资源确认清理后再启动新资源，客户端与面板兼容边界不变。

# 实际 Proxy 的原生 DoQ 查询

接续 [原生 DNS 查询所有权](native-dns.md)。本批位于本地 develop / v0.0.3，Mieru / mieru-config 的 3.33.0 基线与既有公开配置、API、IPC、HTTP、gRPC、FFI 形状保持原约定。实际 Proxy 的 DoQ 原始所有权链已完成本批回归和消费者验收，结果见[验证清单](local-native-doq-20261025/validation.json)。普通生产登记继续关闭；未暂存、未提交、未推送。

## 通用 metadata 所有权

启动期的 `MetadataDatagram` 保留原语义。新增 `ControlledMetadataDatagram` 声明及 `MetadataPoolOwner`，复用平台原有 MetadataOwner、原始 native/probe 控制器、唯一 MetadataViewClaim 与 sealed 用户/驱动图。通用 runtime 只绑定实际实例、声明序号、Scope、上限和原始关闭凭据，不依赖 DNS、QUIC 协议或 Proxy。

宿主启动期登记实际所有者，随后每次按需申请必须使用同一个真实 Scope 中尚未 join 的原查询 witness。一次 permit 绑定这个实际查询，不能改由同 Scope 的另一条查询使用。每个原始 MetadataOwner 在 native acquire 前已存入有界槽位；拨号策略和真实 egress/network generation 通过原来的平台 socket 路径执行。UDP capability probe 也由这个原始所有者记录。对外只转交 RX/TX、观测权限与唯一 claim，原始控制器一直留在注册所有者中；出站 metadata sender 拒绝声明 peer 以外的目的地址。

`max_origins` 限制当前 reserved/live/retiring/Unknown producer 组；每组的主 socket、probe、初始化次数、发送并发和 poller 数由 MetadataLimits 分别声明。它不是终身绑定次数。完成的一组在原始凭据确认后归还额度，不追加终身私有记录，也不新增资源管理线程。

创建逻辑图之前被拒绝的装配，只能交回那个未被消费的唯一原始 claim；平台先核对原 issuer 和实际 Scope，成功后才消费调用方的 Option，拒绝则保留唯一凭据，不能污染其他槽位。Proxy 在可失败的归还前先把 claim 存入持久查询生命周期，清理可再次尝试。平台随后检查实际所有 RX/TX/poller 视图全部归还。这条路径不创建空的 QUIC 图，不伪造 driver/TaskId。已经绑定的图必须交付实际图的 sealed 关闭凭据；普通 Drop、视图数量、端口可用性、公开状态或另一个 owner 的 receipt 都不能替代它。

## DoQ 装配

仍是同一个 DnsSystem、RuntimeHost 和 `proxy.dns_queries` roleless 组件。zero-dns 提供实际后端解析出的 ClientConfig / SNI，并拥有 DoQ 的长度前缀、DNS 消息、FIN、响应解析和策略。Proxy 的窄 provider 负责打开声明过的 metadata 原始资源，并使用通用 zero-transport::quic::owned 类型执行 QUIC。

实际 OwnedEndpointOwner、原始 DriverExitReceiver 和 MetadataUsersCompletion 在调用 start 前保存在查询资源记录中。Quinn 的 endpoint 与 connection 两个真实驱动通过原 TaskRegistrar 登记；不使用裸 TokioRuntime 开 native socket，不另造 driver 占位。查询观测只保存当前 witness、实际弱 metadata lineage 和实际 I/O Pending 事实。

通用 inventory 宿主构造始终可用，实际 UDP handler 方法随 `udp-runtime` 编译。DNS 声明实际 QUIC transport 依赖，独立于 Proxy 的 UDP 框架和 `transport_quic` 能力开关。仅启用 DNS 与关闭默认功能均已独立完成编译检查。

DNS 仍最多登记 16 个查询。普通 TCP/UDP 原始槽位计一份；metadata 预留主 socket 与 probe 两份，所以与普通 DNS 共用的 native 上限仍为 16，纯 DoQ 同时最多八组。存在 DoQ 声明时才额外允许最多 16 个真实 Quinn 驱动任务，Scope 总预算为 32；没有 DoQ 时仍为 16。配置和空闲阶段不预先打开这些 socket/endpoint。这些内部资格常量尚不等于生产可配置资源预算。

## 关闭和替换

正常结束先归还实际 bidi stream，撤销该 endpoint 的用户/驱动准入，关闭实际 endpoint；原 owning receiver 逐个接收真实 TaskJoined 与原始错误，再确认原用户和原始 I/O views 都结束。随后由 canonical MetadataPoolPermit 验证同一图的 sealed 凭据，显式关闭同一主 socket / probe 原始控制器。只有私有原始 close stamp 匹配且存在实际 native acquisition，才允许后续 DNS fallback。

中断的清理保留 endpoint、receiver、已收集的原始 TaskJoined / source、completion 和 native permit。打开失败暂不授予重试权；不能因配置了备用 UDP 而绕回未经授权的裸 socket。原生 FD、逻辑用户、actual task join 和 driver reception 是分别核对的事实。

网络变化共用原 TCP/UDP 的串行准入栅栏：取消旧查询，等待实际 caller joins，接收真实 QUIC 驱动结果并确认旧原始 main/probe 关闭后，才能推进 epoch 和申请新原始资源。配置变化依旧先停止受影响的实际消费者；未归还的 Mieru 用户/控制债务会阻止 DNS 依赖清理。旧 DNS owner 的 native 与 Scope 证据全部完成后，才允许新 DNS owner 和传递消费者启动。

## 验收范围

真实 QUIC/TLS 对端测试覆盖查询和 FIN、SNI/证书名称、Mieru 域名接入、取消后的原始关闭、网络切换、跨消费者替换、混合 UDP/DoQ 总额度、worker 原始故障与 2 MiB worker。通用资源测试使用实际 factory / slot / TaskScope，覆盖 foreign scope、同 Scope 不同查询、原始视图保留、目标约束和连续复用。

最终结果以 `local-native-doq-20261025` 中的本批源码快照和日志为准；编译失败不记为行为负向证明。完整 workspace/all-features 使用未优化、关闭调试符号的测试构建，保留所有断言、原并发和 16 MiB 测试线程栈；2 MiB 用例保持显式限制。消费者使用同一新二进制和原有截止，不增加生产线程栈。

- [最终源码快照](local-native-doq-20261025/source-final.json)含 534 个路径：394 个实际文件、140 个预期不存在的历史迁移路径，校验无漂移。普通入口仍关闭 native background 登记。
- Rust 1.99.0 完整 workspace/all-features：155 个目标，**3832 通过 / 0 失败 / 175 忽略**，实际结束码 0。原忽略项保留；普通子进程默认栈转发、父 stdin EOF 退出和 bind 失败重试均通过。
- Rust 1.98.0 严格 workspace/all-targets/all-features Clippy、DNS-only / no-default 编译、格式、190/27 分层守卫、21 项 CI 范围检查、314/95 集成布局及冻结方案检查通过。功能开关编译日志保留各条件编译警告。
- Proxy 聚焦测试 **32/0**，包含新增 9 项 DoQ；通用 metadata pool **4/0**，平台 metadata **27/0**。后两组源码在夹具与构造门控修正期间保持一致，原始快照及[复用核对](local-native-doq-20261025/focused-source-reuse.json)保留，最终完整回归也执行了这些用例。
- GUI 原契约脚本 **12.70 秒**通过，唯一实际运行实例退出 0，无 terminate/kill。面板原发布流程 **53.44 秒**通过：同一 PID / core / revision 2，TCP/UDP 跨三个发布进程保留。两个仓库保持原提交且干净，面板沿用摘要不变的已编译验收入口。
- 消费者使用[同一个只读新二进制](local-native-doq-20261025/consumer-binary.json)，均不设置 `RUST_MIN_STACK`；GUI 的 30 秒和面板的 120 秒原限额保持不变。

候选编译中断、唯一 claim 交接修正、子模块方法可见性、空过滤器、冷启动 CA 夹具误用和通用构造误受 UDP 门控的诊断均保留。空过滤器未计入测试资格；CA 夹具修正继续使用原 TLS 后端，保留真实证书名称校验和原始关闭断言。功能门控修正后重新完成完整回归和两个消费者验收。

系统解析器、DNS detour、真正池化 DNS、打开失败的类型化重试、其他协议 carrier、完整业务/故障/计量/互通矩阵和可配置资源预算仍未完成。未新增线上部署、真实 GUI/TUN 或 1c1g 实机压测资格；整体 ALL17 和普通 Mieru 生产登记继续保持未完成/关闭。

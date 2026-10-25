# 实际 Proxy 的原生 DNS 查询所有权

接续 [跨 owner 共享资源替换](cross-owner.md)。实现位于本地 `develop`，归属 v0.0.3；Mieru / mieru-config 基线仍为 3.33.0。未提交、未推送。普通生产登记保持关闭，以下装配通过现有内部资格入口运行。

## 一个解析器、一个宿主

Proxy 保留原来的 DnsSystem，使用同一份 DNS 策略、缓存、Fake-IP、DNS 配置代次、egress authority 和发布事务。内部原生装配向这个解析器注入弱 NativeDnsSource，不创建第二个解析器、第二个 Proxy 或额外运行实例。

Proxy 在宿主冷准备中登记 `proxy.dns_queries` 的 roleless ComponentFactory。配置的 literal/bootstrap endpoint 与 TCP/UDP 类型形成精确资源声明；角色列表为空，不伪造 Stream/Datagram RoleId。DNS 的协议与 framing 仍归 zero-dns，Proxy 仅实现窄 NativeDnsProvider 和宿主装配；zero-runtime 不依赖 zero-dns 或具体协议。

Mieru 的 proxy 装配包装器声明实际 DNS 组件依赖，消费已经由宿主校验的 ComponentInstanceId 后再交给协议自己的共享池工厂。协议库不引入 Proxy、DNS 系统或宿主依赖。所有 Mieru profile 继续共享原来的实际 adapter pool。DNS 配置变化会传递替换这个实际池；未改变的 DNS 配置不凭空推进 DNS owner。

## 查询任务与原生资源

冷 build 只安装持久生命周期 owner、实际 TaskRegistrar、有限 CarrierOwner 和弱解析器引用。start 向同一 HostResources 登记真实 ControlledCarrierPool，启动 owner 后才向 NativeDnsSource 发布 provider。实际 endpoint/传输类型以外的原生申请在 I/O 前拒绝。

每个原始查询任务先安装持久 record，再通过实际 TaskScope 做一次 tracked spawn。记录实际 TaskJoinWitness 与取消源，之后才能进入解析和原生 I/O。实际 native 查询任务使用 Critical 分类：正常 DNS 错误和取消完成 job，不把网络失败当作任务失败；未捕获的 worker panic 由宿主监督为真实组件故障。provider 同时核对真实 registrar 的 Scope identity 和尚未 join 的实际查询 witness。查询合并和策略 fallback 复用这个原始任务；不创建业务 SessionId，不用服务用途枚举充当资源授权。

原生 TCP 的 controller 在连接 await 前安装到持久 CarrierOwner slot；UDP 使用同样登记过的实际 owner 和原始控制器。唯一 custody view 转成 StreamLoan / DatagramLoan，原生来源明确没有独立 driver，不创建占位任务，也不把查询任务冒充成 native driver。原始 native 控制器始终留在同一个宿主资源 owner 中。

同一个查询任务内执行 DNS UDP、TCP framing、DoT TLS 或 DoH TLS/H2。DoT/DoH 使用已有后端的实际 TLS profile，保留原来的证书、主机名和 ALPN 校验；DoH connection future 在原始查询任务内驱动，不另起未监督 H2 worker，也不启用另一套连接池。

当前内部 DoH 资格按每次尝试打开和确认关闭连接，尚不提供原有 DoH 池的连接复用资格。普通入口仍保留原池；本批不能据此宣布原生池化 DNS 的性能或生产行为等价。

原生查询边界与 TLS/H2 的大状态使用按需 `Pin<Box<...>>`，避免未启用原生登记时也扩大普通转发 future。它们仍由同一个实际查询任务驱动，不增加后台任务，也不提高生产线程栈上限。客户端/面板验收发现过本批调试构建在默认 worker 栈上的溢出；上一批二进制在同环境通过，故保留该失败作为修复前证据。新增回归分别检查普通子进程移除测试栈覆盖后的真实转发和自然退出，以及原生 DoT/DoH 在显式 2 MiB worker 上的查询和关闭；最终通过与否以修复后的完整验收记录为准。

## 关闭与后续拨号

一次尝试返回前先丢弃实际 I/O view，再调用同一 LoanOwner 的显式 close。只有以下凭据同时匹配，才允许 UDP 截断转 TCP、下一 endpoint 或策略后备 DNS：

- 同一个逻辑 loan 的匹配关闭凭据；
- 同一个原始 slot 的 native close receipt，且记录真实 native acquisition；
- 原始 controller 报告完整，未遗留原生 I/O 或用户债务。

逻辑归还、Drop、超时、EOF 或释放查询额度不能替代原始 native close。未确认关闭立即阻止后续拨号。失败结构同时保留查询错误和关闭错误；错误文本、ErrorKind 或 downcast 不是重试授权。打开阶段失败而无法提供关闭凭据时保守终止当前 fallback 链，持久 owner 继续保有原始责任。

查询取消会撤销实际工作并归还 I/O；控制器仍由原 owner 保留。provider 在 native open await 前保留同一真实 adoption，以有界 custody 记录内联驱动原始控制器，消费其真实关闭结果后回收私有 slot。不把每次取消的关闭条目追加为 owner 的终身历史，不创建维护线程；已关闭 custody 元数据在下一次申请或宿主关闭时回收。全局查询任务与所有 endpoint 的实际 reserved/retained 原始 socket 均限制为 16；失败和取消后尚未确认关闭的原始槽位继续计费。空闲配置不会预先分配 socket。该内部资格预算尚不代表生产可配置资源上限的完整验收。

## 配置替换、网络切换和退出

配置替换由同一个 RuntimeHost 控制：先 revoke 受影响的实际消费者和 DNS owner，停止旧查询 admission，取消旧 flight，等待其原始 Scope 的真实 joins；再消费所有原始 TCP/UDP controller 的关闭结果。保留的 Mieru 用户或控制债务会阻止 DNS 依赖完成。全部旧资源确认后，宿主才能 start 新 DNS owner 和消费者。

NativeDnsSource 只撤下身份匹配的原 provider；旧 owner 的重复 revoke 不能撤销新 provider。查询退出处理按原 query-task owner 定位 flight，旧 Scope 的迟到退出不得清除新 owner 的查询。

网络变化复用同一个实际 owner，但在任何新 native acquire 前串行完成：取消旧查询、等待其实际 witness join、回收原始 controllers、确认旧 slots 均已清空，再推进 network epoch。name/wire 两个 coordinator 都消费实际取消源并完成相应逻辑 flight，不能因另一路取消了 worker 而让原调用方永久等待。旧查询不能等待自己的 join 后跨网络重启。中断、超时或未完成清理不推进 epoch，也不开放新 socket。

退出仍立即取消业务；由现有宿主监督器驱动查询 joins，随后 DNS 生命周期和 HostResources 各自交付完整关闭证据，再允许完成退出。已经 join 的查询失败仍由同一 TaskScope 保留原始错误，不因诊断错误提前跳过 native 清理；超时、身份错误和未 join 继续阻挡完成。清理逐个尝试全部原始 owner，已取得的报告在生命周期内持久保存，取消后重试不会丢失。查询观测只返回有界的当前实际注册项、原始弱 lineage 和 I/O suspension facts，不授予 I/O 或关闭权限，不保留历史配置。

## 兼容与剩余门槛

普通 Proxy 入口仍使用原来的 DNS 和协议登记，内部资格入口才启用本批原生装配。公开配置、API、IPC、HTTP、gRPC、FFI、客户端和面板消息形状不变；产品版本、协议基线与 Cargo.lock 不变。

这不是完整 DNS/native 生产资格：DoQ 的实际 QUIC endpoint/driver 原始链、OS/system resolver 和 DNS detour 尚未接入这个原生 provider。内部路径在这些操作的未受管 I/O 前拒绝，不退回旧的裸解析路径。打开失败后的类型化重试资格、实际池化 DNS、其他协议 carrier、SOCKS5 UDP 双原始、原生业务 TCP/UDP、fallback/MUX、故障和计量归因互通矩阵，以及生产可配置资源预算仍需后续验收。

本批没有线上部署、真实 GUI/TUN 操作、1c1g 实机压测或新增外部官方工具互通。最终测试、源码冻结和同二进制客户端/面板证据由本目录的 `local-native-dns-20261025` 记录；只有实际完成的检查才能标为通过。

## 本批资格与验证

[最终验收记录](local-native-dns-20261025/validation.json)与 [修复后源码快照](local-native-dns-20261025/source-fixed-final.json)对应同一份本地源码，没有测试后再修改生产代码。

- Rust 1.99.0 完整 workspace/all-features：155 个目标，**3819 通过 / 0 失败 / 175 忽略**，保留原并发方式和 16 MiB 测试线程栈，实际结束码 0。新增普通子进程回归主动移除该测试栈覆盖，真实转发后通过父管道 EOF 自然退出；原生 DoT/DoH 在显式 2 MiB worker 上完成查询和原始关闭。
- Rust 1.98.0 的严格 workspace/all-targets/all-features Clippy、格式、190/27 分层守卫、21 项 CI 范围检查、314/95 集成布局以及冻结正文和本地链接检查通过。清单依赖边的检查不冒充独立 no_std 或最小 feature 编译资格。
- GUI 仓库原契约脚本通过，20.35 秒，唯一实际运行实例退出 0，没有 terminate/kill。面板原发布流程验收通过，83.84 秒：同一 PID / 实例 / revision 下，TCP/UDP 跨三个发布进程保留，重复发布不增加 revision，监听冲突拒绝并保留原流。面板使用先前已编译、摘要不变的同一验收入口；本批没有重新编译它。
- 两个消费者使用同一个只读二进制副本，SHA-256 为 `5a23d4b74fddc3866097685e331caccbafb7d23d689ef39e02f0742995da43d9`。最终消费者不设置 `RUST_MIN_STACK`，30 秒 GUI 命令和 120 秒面板限额没有扩大。消费者仓库保持原提交且干净；公开配置/API/Engine/FFI/gRPC/根应用与 Cargo.lock 对 HEAD 无差异。

早期夹具编译错误、后备 DNS 选择夹具错误、DoH 请求兼容修正、清理/跨 coordinator 修正、首次全量的绑定重试占用错误、默认栈溢出和编译期间的消费者超时均保留诊断日志。绑定重试在最终全量中按原断言通过，外部占用者身份没有被倒推。编译结束后，消费者在同一二进制和原限额下通过；超时留下的隔离测试内核已显式 SIGTERM 并核对进程退出，这不被当作原生 provider 的关闭凭据。

本批只取得上述范围的资格，**普通生产登记保持关闭，整体 ALL17 仍未完成**。未暂存、未提交、未推送。

后续 DoQ 的实际 endpoint、驱动和原始 metadata 关闭链已另行完成本地范围验收，见[原生 DoQ 查询](native-doq.md)。本文保留当时 UDP/TCP/DoT/DoH 检查点及原始诊断。

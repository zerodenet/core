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

最终验收记录（历史材料见检查点 `8acd1cca`）与 修复后源码快照（历史材料见检查点 `8acd1cca`）对应同一份本地源码，没有测试后再修改生产代码。

- Rust 1.99.0 完整 workspace/all-features：155 个目标，**3819 通过 / 0 失败 / 175 忽略**，保留原并发方式和 16 MiB 测试线程栈，实际结束码 0。新增普通子进程回归主动移除该测试栈覆盖，真实转发后通过父管道 EOF 自然退出；原生 DoT/DoH 在显式 2 MiB worker 上完成查询和原始关闭。
- Rust 1.98.0 的严格 workspace/all-targets/all-features Clippy、格式、190/27 分层守卫、21 项 CI 范围检查、314/95 集成布局以及冻结正文和本地链接检查通过。清单依赖边的检查不冒充独立 no_std 或最小 feature 编译资格。
- GUI 仓库原契约脚本通过，20.35 秒，唯一实际运行实例退出 0，没有 terminate/kill。面板原发布流程验收通过，83.84 秒：同一 PID / 实例 / revision 下，TCP/UDP 跨三个发布进程保留，重复发布不增加 revision，监听冲突拒绝并保留原流。面板使用先前已编译、摘要不变的同一验收入口；本批没有重新编译它。
- 两个消费者使用同一个只读二进制副本，SHA-256 为 `5a23d4b74fddc3866097685e331caccbafb7d23d689ef39e02f0742995da43d9`。最终消费者不设置 `RUST_MIN_STACK`，30 秒 GUI 命令和 120 秒面板限额没有扩大。消费者仓库保持原提交且干净；公开配置/API/Engine/FFI/gRPC/根应用与 Cargo.lock 对 HEAD 无差异。

早期夹具编译错误、后备 DNS 选择夹具错误、DoH 请求兼容修正、清理/跨 coordinator 修正、首次全量的绑定重试占用错误、默认栈溢出和编译期间的消费者超时均保留诊断日志。绑定重试在最终全量中按原断言通过，外部占用者身份没有被倒推。编译结束后，消费者在同一二进制和原限额下通过；超时留下的隔离测试内核已显式 SIGTERM 并核对进程退出，这不被当作原生 provider 的关闭凭据。

本批只取得上述范围的资格，**普通生产登记保持关闭，整体 ALL17 仍未完成**。未暂存、未提交、未推送。

后续 DoQ 的实际 endpoint、驱动和原始 metadata 关闭链已另行完成本地范围验收，见[原生 DoQ 查询](native-doq.md)。本文保留当时 UDP/TCP/DoT/DoH 检查点及原始诊断。

## TCP 打开失败后的既有后备查询

在检查点 `8acd1cca` 之后，原生 TCP provider 在打开前保留真实 adoption。打开失败后，先归还实际 view，再以原期限驱动同一原始控制器的完整 retire；只有匹配的 native acquisition、原始关闭和 users-closed 凭据齐备，才通过私有字段的 `NativeDnsOpenFailure` 保留打开错误和关闭证明。DNS 的后备逻辑只消费这个类型化结果；单独的错误码、取消或未使用的 reservation 都不能授权重试。清理未完成时保留打开与清理错误，继续阻断后备拨号，不延长期限。

新增回归使用内核拒绝 TCP 连接的广播目的地址，检查真实 socket 的原始错误和关闭凭据；另一条先保持 15 个实际 UDP 查询暂停于 I/O，确认失败的第 16 个 TCP 原始槽位清理后，既有 UDP 后备才可获得额度。初版夹具把“绑定但不监听”视为立即拒连，在 macOS 实际等待超时，已据真实失败修正，不扩大限额。普通错误种类的负向回归不构造成功关闭凭据。此变更不补造 DoQ failed-open、system resolver、detour 或池化资格；普通生产登记保持关闭。

2026-10-26，修正后的 Mieru/DNS 定向 34 项、错误种类负向回归及完整工作区验收通过；完整结果和同二进制客户端/面板兼容结果见[当前状态](README.md)。本轮原始输出仅留在系统临时目录，源码、公开契约与消费者状态在测试后复核；未提交、未推送。

## Z003-01 本地验收进展（2026-11-04）

本节保留各阶段实现和验证记录；当前实现及未完成边界以最后的“标准 detour 与同一持久池”小节为准，旧检查点结果不代表当前源码的全量资格。

在云端候选 `66902032` 的本地验收工作区推进，未提交、推送或合入 develop。Z003-01 仍按完整 DNS 职责域收尾；已确认纳入保持既有 detour 所必需的出站原件转交适配，沿用标准角色，不新增 SPI 或协议组合。下面是阶段性实现，不能据此关闭 Z003-01。

System 的普通库入口保留实际 blocking lookup 的原始 JoinHandle，调用者取消、关闭等待中断和容量回收都不能丢掉尚未 join 的原件。默认 System 后端也进入同一 DNS 资源退休事务；仅临时暂停的准入可在提交失败后恢复，已撤销的原件不可复活。冷候选回收不保留没有任务的历史 owner，原任务 panic 的来源仍保留。旧快照在真正开始 System lookup 前发生配置替换，返回 Interrupted，不以临时 resolver 继续查询或触发 unreachable panic。

标准 native System 查询使用原 DNS owner 的真实 TaskScope blocking 子任务。答案发送不等于完成，必须消费同一子任务的 join；OS resolver 的任务完成不冒充系统 DNS 服务或其私有 socket 的关闭凭据。普通 Proxy 已装配实际 roleless Node DNS owner，支持无显式 DNS 配置的标准 Node 请求。网络代次切换取消旧查询，并在父查询 join 后再次扫描原 blocking 子任务，旧代次任务未 join 时不放行新 native 申请。

UDP 和 DoQ 打开失败继续由实际 provider 保留原件。只有 matching Carrier/Metadata 关闭、转交用户归还和所需真实 driver joins 全部完成，类型化错误才允许已有后备。DoQ 的准备句柄在错误清理前归还；TLS 主机名校验失败不会进入协议请求。后备测试分别检查原件仍 pending 时禁止申请，以及原件确认关闭后允许原配置的 UDP 后备，不扩大期限或资源预算。

必须继续完成的实现：普通 name/wire 查询监督整体迁入同一 host；普通 System 的失败重试及换代旧链收敛；detour Stream/Datagram 的必要出站原件适配；既有持久 DoH 等池的实际 driver、用户和原件所有权；迁完裸流/回包调用者后删除 DnsOutboundConnector 旧执行链。现有 standard Service 身份或裸流 Drop 都不能替代这些关闭凭据；本批未把该旧桥宣告删除，也未开启普通 native Mieru/MUX 登记。

`TEMP-WG-OUTER-UDP-01` 单独保留，未关闭；上述 DNS 待办不是临时豁免。只改内核及其实施记录，未修改客户端、面板、公开配置、API/events 或 FFI。当前采用 Rust 1.99.0 的 DNS/Scope/Proxy DNS 定向回归及既有架构守卫，不重跑 workspace 全量；云端旧证据和本地不同源码快照的结果分别留档。

### 同日续推进：普通查询监督、持久池与前缀原件

普通 Proxy 的 name/wire 查询现在与 Node 查询共用同一个真实 DNS TaskScope，原始 query coordinator 不再在生产路径启动自己的 JoinSet。无 detour 的 System、UDP、DoT、DoH、DoQ 通过原 native provider 执行；standalone 库入口仍保留明确的独立执行方式，不能与同一个后端的宿主模式混用。

DoH 使用原有 clients 缓存和连接建立锁，没有建立第二个池。物理连接的准备、TLS/H2 和持久 driver 在原 Scope 中登记，真实 LoanOwner 在 TLS 等待前进入持久 owner；关闭独立检查 H2 用户归还、原 driver join、实际 logical loan 及 provider 原件。中断打开但尚未交付 LoanOwner 的原件也由对应 provider 的同一 caller 登记保留和关闭。成功请求只证明本次逻辑使用归还，健康物理连接保持复用；错误、取消或换代必须清理旧连接后才能替换。

宿主关闭显式清理绑定该原始 query Scope 的池，不能以新 Scope 或库内 JoinSet 替代旧凭据。DNS 配置或编译后 dispatch 变化时，已绑定旧宿主的后端进入原退休事务；相同服务器配置不能让旧 Scope 的池跨 owner 复活。实际无变化的配置继续保留原后端、物理连接和 driver。删除了 native 路径原来每请求内联建立 H2 的执行器及专用 request/response 转发方法。

SOCKS5 标准 Stream carrier 的非根分支改用既有 CarrierGrant::adopt_stream_loan：先将借来的真实控制器存入本组件原始 slot，再等待认证/CONNECT。无独立 driver 的转交保留这个 slot 的真实 parent，不伪造 TaskJoinWitness 或 native acquisition。每个有实际 carrier 声明的中间 owner 都取得其真实前缀授权，依赖记录继续用于确认停旧及传递替换。

必要定向结果已包含真实 Proxy 的 6 项加密 DNS/复用/网络换代/无变化 reload/owner 替换/退出检查、其余 DNS 宿主 24 项，以及两项实际单/多 taskless 前缀与保留控制视图的替换检查。detour 手动夹具的新 Scope join 监督修复和普通生产 Proxy 的新增 DoH 检查另留本轮结果，未用取消通知代替 join，也未扩大生产/测试期限。

剩余边界仍明确：生产 detour 的裸流和 packet exchange 出站适配、detoured DoH 池的完整原件转交、所有调用者迁完后删除 DnsOutboundConnector/service/packet 旧链。上述遗留路径仍是必须完成的实施缺口；本批不能把直连池及前缀基础完成等同 Z003-01 关闭。普通 native Mieru/MUX 登记未开启，TEMP-WG-OUTER-UDP-01 未扩展或关闭，客户端/面板与公开 schema 未改。

本轮收尾结果：Rust 1.99.0 的 detour 定向 10 项、普通 Proxy DoH 宿主池 1 项、DoH 库/池 18 项、最小 System 所有权 10 项及最小网络代次 blocking 子任务 1 项均通过；DNS/平台/SOCKS5 全 feature lib 严格 Clippy、DNS 最小 feature lib 严格 Clippy 通过。之前同批的 6 项加密宿主池、24 项其余 DNS 宿主及 2 项真实跨 owner 前缀结果分别保留，不冒充同一源码快照的全量验收。Proxy 的最后定向二进制之后仅补齐了 DNS 内部 cfg/穷尽匹配及格式，未重编其大测试目标；这些编译边界改动由最新 DNS 最小测试和全/最小 feature Clippy 检查。未执行 workspace 全量。

下一批必须沿实际生产调用者继续，而不是扩展协议组合：`crates/proxy/src/runtime/dns_outbound.rs` 中仍有 `Session::new(0, ...)` 的裸 TCP 后备和 `prepare_datagram_exchange` 回包桥；`crates/dns/src/backends/doh/client/connect.rs` 仍为 detour 池消费缺少完整原件的 `DnsOutboundCarrier`。先把这些既有出站的标准角色、真实原件和传递替换接通同一宿主，确认待定打开、TLS/H2 driver、转交用户、旧代次和失败重试的清理门槛，才能删除 `DnsOutboundConnector` 及对应执行链。不能靠返回裸流、Drop 或另设临时豁免来关闭 Z003-01。

### 标准 detour 与同一持久池

本地继续推进了一个完整的标准 Stream 职责类别：已登记、无独立任务及共享依赖的角色，由 DNS owner 提供其真实受控 TCP 原件，协议只在该 I/O 上执行既有握手、加密和封装。角色选择检查实际组件图及资源声明，不按协议名称选择执行器。现阶段要求默认 dial，资源限于无独立驱动的 Stream carrier 和已声明文件；Direct 只允许根位置。共享池、独立驱动、设备或进程路径不能据此取得资格，普通 Mieru/MUX 登记没有开启。

冷装配从现有 DNS detour 目标图提取 endpoint/dial 事实，声明 DNS owner 的实际原始 carrier slots；原件归属与静态身份包含这些完整事实。节点域名仍由真实 Node DNS provider 在同一 Scope 解析，未借用系统裸 resolver。所有权在 native open、协议握手和 TLS/H2 等待前安装，失败候选必须确认自己的真实原件和嵌套工作清理，才可尝试原配置的下一个候选。已经确认关闭的失败 IP 原件从同一有界 custody 中移除，不积累查询历史。

生产 detoured DoH 已接入原来的 clients/connect_lock 和真实宿主 H2 driver，未另建 resolver、连接池或执行器。单次调度解析只发生在新物理连接建立时，健康缓存复用不再消耗 load-balance 轮转；selector、实际角色代次及网络代次进入原 binding。配置替换、选择变化或网络换代先停止旧使用、消费真实 driver join、logical loan 和原始 controller 凭据，再开放新连接。标准角色的 TCP DNS、DoT 及既有 UDP→TCP 后备也复用同一个实际 Stream 执行循环。

本次接管的 Stream 查询取消收尾留在原始注册任务中：先丢弃已取消的实际 I/O future，再消费本 caller 的原件；不等待下一次请求才清理，不另起清理任务。嵌套 Node/System 工作保留真实原任务祖先身份，父任务完成不释放仍在执行的 blocking lookup；清理必须等待原 blocking 子任务的 Scope join。持久池 driver 使用独立生命周期，不作为首次查询的待结束子任务。弱 provider 引用不会额外保留宿主；清理超时或失败保留实际责任并阻止完成，不能伪造成功关闭。DoH 查询和原件清理同时失败时保留两个原始错误，不丢弃导致清理的查询错误。

上述接管路径不调用 `ProxyDnsOutboundConnector`。该旧桥仍由尚未取得完整原件资格的既有出站使用，包括 resourceful/shared-pool/设备等路径及原 packet exchange；不得删除后让这些能力静默失效。`runtime/dns_outbound.rs` 的裸 TCP 后备、`Session::new(0, ...)`、`prepare_datagram_exchange` 及 standalone detour 的 `DnsOutboundCarrier` 消费者仍存在。**Z003-01 尚未关闭**：下一批必须迁完这些真实调用者和必要出站所有权，再删除对应旧链；这不是仅剩验收，也没有新增临时豁免。

本批只修改内核及其实施记录，公开配置、API/events、FFI 和客户端/面板项目没有改动；兼容性结论限于这些边界保持，未重做 GUI/面板联调。`TEMP-WG-OUTER-UDP-01` 单独保留，未扩展、未关闭。所有改动留在本地验收分支，未提交、推送或合入 develop；测试仅使用 Rust 1.99.0 的必要定向范围，不重跑 workspace 全量。

最终源码的定向结果：Proxy DNS detour 13 项、DoH 库/持久池 18 项、DNS 原始任务/嵌套 blocking 所有权在全 feature 和最小 feature 下各 10 项通过。取消用例检查真实 I/O Pending、原始任务 join 和 matching native close，不以对端 EOF 为清理凭据；持续复用用例连续完成超过单类查询额度的请求，仍只建立同一个健康物理连接。DNS/平台/SOCKS5 全 feature lib 严格 Clippy、DNS 最小 feature lib 严格 Clippy、格式检查通过；这不是 workspace 严格 Clippy。既有 189/27 项边界守卫及 315 source/95 target 集成布局检查通过。

中间失败分别保留：初版 native detour 准备缺少 Datagram 方法/类型标注、夹具监督未接通、旧 H2 嵌套错误的提取，以及取消后清理延迟至下一次申请等；最终结果不覆盖或隐藏这些诊断。源码摘要、精确命令和各阶段日志存于仓库之外的本地 `zero-cloud-acceptance-20261104` 任务产物，最终主记录为 `z00301-standard-detour-results.json`。此前 HY2/设备回滚修复的 9 个文件摘要未变化，`HEAD=66902032`、`develop=bab00b69` 未移动；未启动全量或消费者工程验收。

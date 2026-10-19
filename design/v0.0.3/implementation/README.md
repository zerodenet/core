# 实际执行记录

完整规格及原任务卡保留原始来源；本文件记录后续代码和实际验证。所有检查点继续留在同一分支及 Draft PR124，整体尚未完成，不逐项合并。

## PR00 / PR01

PR00 方案提交为 `122eb04b806da6a31495345d79d3e4f591f027b3`。同一 PR 的交付规则提交为 `3abbde91a1e8a6dae3d9391ad254ff8eded64d9c`。

PR01 基线提交为 `f90f012689dce16cdbe4fa7616492b8fa8e5cc9e`，运行源码仍为 `develop@2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`。原始输入、回执、FFI/消费者及真实 1 CPU/1 GiB 资源证据见 [baseline](../baseline/README.md)。没有把零长 UDP 当 EOF，没有把 OS 线程数当 Tokio 任务数，也没有凭空设置新的生产容量默认值。

[f90 远端资格记录](../baseline/ci-evidence-f90.json) 区分了实际结果：静态、最小 feature、三平台编译及 musl 成功；Linux workspace 有一个既有 Hysteria 认证测试在 10 秒总截止时间超时，结果为 2634 通过、1 失败、159 忽略，根因尚待定位。三个 hosted TUN job 执行了主要特权场景，独立 smoke 步骤仍跳过；Windows 出口切换明确 `executed=false`，缺少合适的第二接口。Optional surface job 未执行。这些结果不适用于后续提交，不能称为全绿或完整真实设备验收。

## PR02：公共契约与集中擦除

代码提交：`4d76d1663d610cf110fc4feeb972600a2055b5ce`。验证前冻结的 Git tree：`99c60f08224c6baafed63a0cd03bc95ba3b34b86`，与该提交的 tree 完全相同。详细命令、耗时、原始输出压缩件和 SHA-256 在 [pr02-validation.json](pr02-validation.json)。本地构建时 HEAD 仍为 f90，不能把它的 build-info 标签描述成新提交的发布二进制。

- [zero-core](../../../crates/core/src/component.rs) 保持 no_std，新增有区别的资源/角色/会话身份、检查溢出的代际、纯请求事实、完整 IP 报文所有权及投递分类。认证主体 Debug 隐去协议身份值。
- [zero-error](../../../crates/error/src/channel.rs) 保留阶段、原因、范围、投递与重试分类及来源链。未能证明未提交时，默认状态为 Unknown。
- [运行契约](../../../crates/platform/tokio/src/contract/mod.rs) 位于 platform-tokio，使用关联具体类型和公共动态包装。没有将 Tokio 引入 traits/core，也没有提前提取 zero-runtime。
- Stream 支持 `!Unpin`、`!Sync` 具体流和延迟确认；Datagram/Packet 独立，接收器及控制器仅要求 Send。poll 辅助没有为每个报文分配 boxed Future。部分成功、超长、错误目标和投递不确定均有明确处理；丢弃 pending send 会隔离逻辑会话。
- Packet 有独立端点事实；不要求伪造业务 SessionId。握手仅在识别出业务请求后申请会话身份，Control 不会自动消耗业务身份。上游绑定含前缀角色的资源代际、硬拨号约束和网络代际，重建前缀不会复用旧池身份。
- 工厂 describe 只返回角色及相对资源归属；实际 ID 由宿主分配后传入 build。build 同步构造状态和唯一生命周期所有者；有副作用的启动在 start。角色不必实现未声明的空接口。
- [TaskScope](../../../crates/platform/tokio/src/contract/scope.rs) 复用提供的 Handle，限制活动任务及未消费完成记录；区分关键/辅助任务，回收完成记录，阻止关闭后的登记。关闭超时保留待清理任务；报告保留任务失败及来源链。Drop/abort 不会产生关闭成功报告。

本地冻结源码的检查：fmt、workspace check、workspace/all-target/all-feature 严格 Clippy、完整 workspace 测试及默认 release build 均成功；测试共 **2665 通过、0 失败、159 忽略**。其中 platform 公共契约 suite 为 70 项，覆盖单 Stream 工厂、动态调用、非 Sync Rx/Control、Pin 流、空数据报、部分发送取消、半关闭、Packet 无业务身份、任务容量、登记/关闭竞争、跨 scope ID 和关闭失败来源链。

实际 `thumbv7em-none-eabi` 检查通过。依赖守卫检查实际 Cargo 图：traits/core 无 Tokio，platform 无 config/engine/proxy/具体协议，config validation 不引入 Tokio/platform/transport；守卫及 no_std 目标检查已接入 CI。

后续 `6cd36858` 的[远端记录](ci-evidence-6cd.json)不是全绿：Linux workspace 停在 VLESS Browser Dialer 生命周期测试，报告运行超过 60 秒后持续等待，最终在 workflow 的 45 分钟 job 上限处取消；后续包没有完整执行。六项协议互通、静态、feature、musl 和三个平台编译 job 成功；TUN 主要场景成功但仍有独立 smoke/macOS offline 跳过，Windows 出口切换实际 `executed=false`。这些结果也不适用于后续代码提交。

## PR03 进行中：冷路径工厂与 Direct 真实角色

本检查点起点为 `6cd36858201c0285c9e0a450118bab2827e12955`，代码提交为 `47cc1a137440a69b0775546acd7c2161864e32a7`，源码 tree 为 `7496ca64973b22d78288a2d94f4c75d2c578a016`。[完整验证记录](pr03-validation.json)保存命令、原始输出和摘要。

- 类型化登记只保留具体工厂及强类型 Options；冷路径转换一次，不保存输入文档，不使用 Any 恢复类型。未知、未编译、重复及 feature 元数据错误分别处理，原 ConfigError 和字段路径保留。
- 组件 ID 与检查溢出的资源代际由装配分配；未变化的 Direct 配置复用已构建角色。纯描述/同步 build 不获取 I/O，唯一所有者先安装再 start。失败 start 保留所有者，关闭未确认可继续清理，不能由 Drop 伪造成功。
- Direct 的诊断与策略探测已经通过实际注册的 Stream 角色拨号，使用已有探测截止时间及独立 Service 身份，没有伪造业务 SessionId。现有探测结果代码、消息、健康隔离及真实本地/远端网络信息保留。
- 临时 slot 只接受实际零资源的 Stream 描述；TaskScope::empty 拒绝任何内部任务登记，没有随意添加容量默认值。Host 现有 5 秒协调宽限期只用于零 I/O/零驱动实例的启动。普通业务连接、DNS、relay 与其他协议仍使用现有执行者，迁移后删除这个边界中的旧分支。
- 标准 Direct 错误保留原生 OS 来源；临时 relay facade 以独占轮询访问满足旧 Sync 约束，不加 pipe、驱动任务或运行时。连接事实区分载体可用与对端确认。

本地冻结源码：fmt、workspace check、workspace/all-target/all-feature 严格 Clippy、完整 workspace 测试、默认 release、no_std 和最小 proxy feature 检查均成功，**2684 通过、0 失败、159 忽略**。Clippy 在相同源码 tree 提交前执行，完整测试及 release 在代码提交后、干净源码上执行；没有将最小 feature 中既有 warning 隐去。

Hysteria 原 10 秒认证测试增加阶段诊断，未提高截止时间。真实 1 CPU/1 GiB 中两路并发重复 200 次通过，transport 整套 144 通过/0 失败/1 忽略；VLESS Browser Dialer 两路并发重复 200 次也通过，两例在本地本提交完整测试中通过。两项历史远端问题的根因仍未确定，不称为已修复或以重跑抹去失败。诊断重复不是 RS01 性能或新提交的远端协议/设备资格。

后续仅给现有 VLESS 测试增加直接 stderr 阶段记录，覆盖监听初始化、读写、同步 Drop、peer join 和测试正文结束；即使测试线程或 runtime teardown 卡住也能保留最后位置。没有改变运行实现、断言或原 5 秒测试截止时间。[诊断验证](pr03-vless-checkpoints.json)保存单例、VLESS 严格 Clippy 及 1 CPU/1 GiB 的整个 VLESS 库测试结果；该受限库运行 50 通过、0 失败，不冒充历史挂起已修复。

## PR03 / PR04A 进行中：普通 Direct 与 SOCKS5 原生 Stream

从远端 `c9c06b2a` 继续；Direct 业务接入为 `447866d8`，传输修复 `a5d24eb7`，VLESS fixture `503af4c0`，SOCKS 工厂 `6f1ae3b0`，统一登记与冷路径初始化修正为 `dcb97f9c`、`87852165`。实际冻结源码 tree 为 `e282d3172361fe4c66163b29aec09511597e03d3`。本项仍是部分迁移，未完成 PR04A 或整套 17 项。

- 普通业务 Direct 和 SOCKS5 已经调用构建后的 Stream 角色。业务身份来自真实 Engine 会话和 ingress 所有者，不使用伪造的 SessionId(0)。Service 有独立事实及可选会话身份，TUN TCP/UDP/Packet 的 ingress 服务共用实际所有者。
- Open/CarrierContext 的附加绝对 deadline 改为 Option。普通业务的 None 保持已有 DNS、每候选 10 秒及 Happy Eyeballs 间隔；诊断保留原有绝对截止时间。启动阶段现有五秒协调预算不变。
- SOCKS5 的 Options、认证及 CONNECT framing 在 owning protocol，Options 冷路径转换一次；未变化配置复用角色，静态变化递增资源代际。身份提示使用进程内 keyed hash，Debug 不泄露凭据。所有者先保存后 start；SessionOwner 只用于无共享资源、每会话转移 I/O 的真实角色，不能用于 replay 池、设备或驱动。
- 绑定 opener 使用现有 node DNS/bootstrap 和真实网络代际、端点及约束。协议只得到已选上游，不能绕过前缀或直接改用系统 DNS。适配只借用 pinned Send-only 流以满足旧 AsyncSocket Sync 约束，返回原流，无 pipe、驱动任务或新 Tokio Runtime。
- 标准错误保留原始 OS 来源及旧 wire 消息。CONNECT 可能部分提交或前缀已执行时保持 Unknown、禁止盲目 fallback；只有证明空 root 上尚未提交 CONNECT 才允许 BeforeSubmission。取消、撤权及本地配置/代际错误不惩罚远端健康。
- 节点 socket 使用既有 IoObserver；handshake 计数结束后不会把后续 payload 再计为 control bytes。六项真实业务测试验证实际原生候选、13/12 字节握手计数、预读/半关闭、V1 拒绝回执等价、撤权不退回每次 claim、未知投递不重放及严格 Packet 拒绝。
- 新注册与旧 capability 的编译 feature 清单在同一处装配；catalog 的工厂表只在真实 prepare 时初始化。架构守卫仍检查实际 facade 子模块、具体协议门控位置及单一登记清单，没有删断言或添加绕过白名单。

冻结 `87852165`、Rust 1.99.1、Tokio 1.54.0 的完整 workspace 已通过 **2699 / 0 / 159**（155 条 libtest 摘要）。默认 release、实际 thumb no_std、最小 feature 和实际依赖边界也通过；[精确验证](pr04-validation.json)保存全部命令及原始日志摘要。锁文件仅从旧本地 Tokio 1.53.3 调整为 CI 实际的 1.54.0，协议 reference 和产品版本没有升级。

Browser Dialer 和 XHTTP 503 的根因、消融和新的参考客户端计数缺陷见[独立根因记录](pr04-ci-root-causes.md)和[原始摘要](pr04-ci-root-causes.json)。官方 reference 在后续受限重复中仍有失败：20 轮 19/1，带抓取 40 轮 39/1；新失败的所有 HTTP 上传均为 200，参考客户端在不同上传序号中重复完整 VLESS 请求头。真实 pinned upstream buffer/pipe/WriteAllBytes 的确定性对照重现计数为零及重复提交；这不等于已修改官方二进制或官方资格全通过。

普通 SOCKS relay、其他协议、DNS/内部调用及 UDP 路径还有旧执行者；下一项是实际上游组合和链式 Stream，再迁移有真实池/驱动生命周期的协议。runtime 提取、单实例事务、DNS/Network/纯路由、Packet/Endpoint 所有权、旧桥清理及最终消费者/FFI/资源资格仍未完成。每个实际调用只选一个执行者，保持同一分支和 Draft PR124 继续整体实施。

## PR04A 继续：原生 Stream 前缀与 HY2/QUIC 进度

本次代码提交为 `acc5410a`、`bae5bfe7`、`6a1d8bcb`，最终冻结源码 tree 为 `a9ada0f1b7514175e65051df3b859ade3ddff715`。全原生网络节点链已进入相同 Stream.connect 入口；当前实际协议为 SOCKS5，两跳/三跳与旧执行者对照保持 wire、预读、半关闭、标签、物理 socket 元数据、逐层 Outer 和原有 relay Flow 计数。中间角色撤权在拨号前失败，错误保留实际组件与业务身份；执行过前缀后的取消/网络变化保持 Unknown。链路组装只消费中立候选，拨号策略在冷路径组件中保存，没有扩大 engine-leaf 守卫白名单。

[本次精确验证](pr04-relay-validation.json)：fmt、workspace check、严格 workspace/all-target/all-feature Clippy、完整 workspace **2709 / 0 / 159**（155 条摘要）、Quinn **269 / 0 / 0**、默认 release、实际 thumb no_std、最小 proxy feature 和实际依赖边界全部通过。所有阶段保持 HEAD/源码冻结，包括晚期 rustdoc 和 release。实际 release build-info 为 `6a1d8bcb`，SHA-256 为 `947313e4622f3a7d6e231109045bbfa7ff5dc1ac749baf3c3442adb7647703c4`；可选 connector/grpc/WireGuard 不在默认 release 功能内。

[独立根因与范围](pr04-relay-ci-root-causes.md)及[原始输出/SHA 清单](pr04-relay-ci-root-causes.json)保留首次完整测试唯一的架构失败及修正，也保留 HY2 的一个 MTU 窗口停滞、只改两个包仍失败、Quinn close-only 拥塞阻塞的真实对照。两项修正合用后，实际 carrier 13 项通过，1 CPU/1 GiB 中原/新用例各 20 次合计 40/0。原十秒、载荷、EOF/FIN 和 join 断言保持。旧 CI 未采集 RTT/cwnd，不倒推其精确状态；官方 Xray 字节计数资格失败仍见前一节。

该检查点仍未完成整个 PR03/PR04A。接下来补齐被中止 start 的所有者保留、真实任务预算和驱动清理，再迁移资源协议。a0 的七项协议 workflow/hosted TUN 成功仅属于 a0：VLESS250/0/1，三个独立 smoke/macOS offline 跳过，Windows route-switch 明确 executed=false。新 head 的远端资格和最终真实设备/消费者资格分别记录。

## PR03 进行中：中断保留、显式任务预算和确认清理

代码提交为 `0dd18de5f968ad91ce09c87733d361f0faa6fe48`，冻结源码 tree 为 `e6b47120c54aebf4330d4c2783d68d4754cdcd7e`。实际命令、原始日志压缩件和 SHA-256 见 [pr03-owner-validation.json](pr03-owner-validation.json)。本项为资源协议迁移的前置修正，整体及 PR03/PR04A 仍未完成。

- 冷路径同步安装唯一 Live/Owner 后才等待 start。启动调用方被中止、启动总截止耗尽或正常失败时保留该所有者；后续调用不能重建或重新启动部分初始化实例。等待方使用自己的原绝对截止时间，短等待不会取消初始化者。没有分离的后台初始化任务。
- `ComponentSpec::task_limit` 显式给出已登记任务及未消费完成记录的上限，None 为零。Direct/SOCKS5 保持零内部任务。生命周期启动与 Stream 句柄获取拆分，纯资源所有者不必伪装成 Stream，也不获得虚构角色 ID。
- 关闭先撤销准入，再同时推进协议清理和 TaskScope 取消/等待退出。scope 已确认的退出及原始失败先存回资源所有者；协议清理失败后仍可重试。TaskScope 自身也在持有的状态中保留逐项收集结果，因此丢弃尚未完成的 shutdown future 不会丢失已经收到的 TaskId、关键/辅助分类或来源链。
- 驱动回归通过真实授予的一项任务容量启动登记任务，使用 Tokio DuplexStream 和实际 peer EOF 验证 I/O 释放，覆盖关闭互等、容量拒绝、关闭后拒绝登记、超时保留 I/O/Join 所有权、协议失败后保留任务失败并完成重试。它不是生产插件、监听器或设备的专项验收。

三项旧实现失败均保留：中止 start 后 owner 被直接丢弃；顺序清理在原两秒截止仍留下 I/O 与实际 TaskId；中断 scope shutdown 后重试只得到两项任务中的后一项。对应修复后的组件过滤为 27 项通过（含 26 项 proxy 及 1 项 transport），platform suite 为 72 项通过。初次驱动正向编译中的 TaskClass 导入错误也保留，未将编译失败算作行为负向证明。

严格 workspace/all-target/all-feature Clippy 在提交前通过，随后逐文件摘要及完整 tree 与提交完全一致。提交后保持源码、HEAD 和锁文件不变，完成 fmt、workspace check、完整 workspace **2716 / 0 / 159**（155 条摘要，包含晚期文档测试）、默认 release、实际 thumb no_std、最小 proxy feature 和实际依赖边界验证。产品版本、固定协议 reference、既有协议认证与截止时间不变；默认 release 不表示可选 connector/grpc/WireGuard 的 release 资格。

上一 head `67ee1737` 的远端结果单独保留：主 workspace2709/0/159，静态、musl、feature、三平台编译和 CI 汇总成功，Optional surface 跳过；六项协议 workflow 成功，VLESS 的扩展 metadata packet-up / zero_client=false 用例报 early EOF。新失败原因尚未确定，不能直接套用此前已证明的参考客户端字节计数缺陷。Windows route-switch 的原始报告明确 executed=false，缺少合适第二接口，独立 TUN smoke 和 macOS offline 仍跳过。这些结果不代替本提交的远端、物理设备或消费者资格。

本提交完整测试二进制另在只读挂载、独立网络命名空间、1 CPU/1 GiB 中诊断同一扩展参数测试，20 轮20/0。对首组 header/cookie 参数的两种 runtime 重组抓取，共检查160条 TCP与40条 MUX/UDP，序号连续，实际字节数与载荷正确，无重复或缺口。全部日志、执行/分析程序、全部本地 raw capture 的摘要及完整代表抓取保留；其余 raw capture 留在云端工作区。这些通过未定位远端失败，不能将该失败改写为已修复或参考缺陷同源。


## PR03 / PR04A 进行中：健康观察、同次注册和 SS 目标事实

代码为 `de7c2f10fa9c0d6ec114dd425c3cfc43fbe6eea7`，冻结源码 tree 为 `fa7a5ab661085aa713f97d98c9ec5c724d7f8975`。[精确验证](pr04-bound-ss-validation.json)保存23项压缩原始证据及 SHA。

- 实际前缀健康对象的旧实现保活退役配置，新增回归先在旧实现断言失败。Engine 的 OutboundHealthObservation 只持有配置代际、当前快照容器、健康状态和事件日志的弱引用及 tag；不会保活旧配置或停止后的 Engine。当前代际继续使用原有可用性计算、失败阈值、即时 URLTest 选择和事件；迟到失败及成功均不能改变新代际健康。没有新增观察任务、回调分发框架或运行时。
- 运行注册表现在使用与旧能力适配器同次装配产生的实际组件工厂表，避免后续为资源协议重复装配池状态。实际 Direct 工厂状态回归验证冷准备、真实角色构建、缓存复用和关闭后的工厂状态释放。
- Shadowsocks 的预备 TCP 配置提供只接收 Destination 的 establish_tcp_to；原 Session 入口与新入口共用同一私有 framing，目标地址/端口以借用传入。legacy stream、AEAD 和2022沿用原密码学、salt/replay状态和响应处理，不制造 SessionId。新的地址/分片/载荷测试与整套 runtime62项通过；SS 资源工厂与共享 replay/plugin 生产迁移仍未激活。

组件过滤29项（28 proxy、1 transport）、实际 Engine 健康套件19项和 SS runtime62项通过。一次误选 engine_policy 的过滤运行实际为0项，已保留并改用真正的 engine_observability suite，不将空运行当资格。

严格 workspace/all-target/all-feature Clippy 通过；之后只有 AGENTS 和兼容性账本 Markdown 改动，19个 Rust 文件摘要及完整 tree 差异已核对。提交后冻结源码、HEAD 和锁文件，fmt、workspace check、完整 workspace **2722 / 0 / 159**（155条摘要，含晚期文档测试）、默认 release、实际 thumb no_std、最小 feature 与实际依赖边界全部通过。实际 release 的 git_hash 为 de7c2f10，SHA为 `77404be733beb8949fbe312b7882f5b65d429ba543c590f4f76e763d40c022ac`；默认功能不代表可选 connector/grpc/WireGuard release 资格。

上一 head730d 的七项参考互通 workflow 成功，VLESS251/0/1；Linux workspace2715/1/159，失败为 SS 插件启动失败用例把预期退出码7的子进程判为启动成功。旧日志没有探针端点或实测子进程退出状态，原因尚未确定。真实 Linux socket 的独立实验已证明，无监听器时仍能连接到自身相同地址端口并回读数据；受控子进程随后确认退出7。这只是旧探针“连接成功即就绪”可能误判的实际输入，不倒推旧CI的精确原因。本代码提交尚未修改 process.rs，后续以实际 readiness 实现的决定性回归继续处理。67ee的XHTTP earlyEOF和此前参考客户端计数问题仍保留，不能由这次互通成功抹去。

## 尚未完成的生产迁移与资格

普通 Direct/SOCKS5 和全原生已登记网络节点的 relay 已走实际角色；混合旧协议链、其他协议、DNS/内部调用、Datagram/入站/Packet 仍有迁移工作。runtime 提取、单实例配置事务、DNS/Network/路由装配、旧桥清除及最终消费者回归尚未完成。PR03/PR04A 和整体均保持进行中，不能以当前检查点宣布 G0–G10 整体完成。

继续全部任务卡。起点已有 transport 无 config/engine/具体协议依赖、中立 Engine Proxy identity、协议所属私有 plan，不重做已经消失的旧层。固定官方 reference 的已知计数缺陷资格、Hysteria 历史失败根因、Packet/协议专项资源、完整 CLI 任务归属、容量预算、完整 GUI/物理设备和 Windows 真实出口切换缺项分别保留；不得用 hosted job 成功、旧基线或模型对照代替最终提交资格。

# 验收门禁与证据分工

以下均为未来验收要求，只有文末 PR00 检查记录描述本次已执行检查。缺失环境、ignored 项、未选中 CI 和真实设备资格不得标记通过。

[方案入口](README.md) · [架构](architecture.md) · [契约](contracts.md) · [迁移与任务卡](migration.md) · [兼容](compatibility.md) · [验证](validation.md) · [来源](references.md)

<!-- source-section: 19 -->
# 云端环境与验收分工

## 构建环境

基于仓库当前 CI 选择 Linux 开发环境，安装 Rust stable、rustfmt、clippy，以及构建所需 C/C++ 工具链、CMake、pkg-config、Python、Node、Bash 和 protoc。CI 当前通过 setup-protoc 安装 35.0；云端记录实际 rustc、cargo、protoc 与系统版本，构建失败时按依赖错误补齐，不随意升级项目依赖。

协议互通使用各 workflow 和 reference 工程指定的 Go 版本、不可变上游提交及准备脚本。容器内没有 Docker、特权网络或外网下载条件时，将对应任务交给具备能力的 CI；普通编译通过不替代这些证据。

当前 Cargo.lock 被 Git 忽略，云端首次检出可能没有 lockfile。常规 workspace 测试遵循现有脚本；只有按对应互通流程生成解析结果后才使用其要求的 --locked。M0 把实际依赖解析与工具版本作为基线产物记录，不擅自改变 lockfile 追踪政策或执行 cargo update。

云端不需要生产密钥、SSH 运维权限或真实用户配置。fixtures 必须使用合成身份、证书和本地端点；公开或私有项目读取权限按实际可用情况记录，不复制本机凭证。

## 检查分层

| 验收层 | 云端可做的工作 | 完成边界 |
| --- | --- | --- |
| 文档 PR00 | 链接、路径、目标和版本安排核对，git diff --check | 不把无代码变更包装成已经跑完 workspace |
| 每个代码 PR | workspace 编译、fmt、clippy、全功能测试、相关契约和最小 feature | 运行结果与失败原因绑定实际 head SHA |
| GitHub CI | 当前选择的 Linux、平台编译、musl、协议互通与特权 TUN 任务 | 被选择且成功的 job 才算该项证据 |
| 最终完整资格 | 显式完整 CI、所有受影响互通、可用的平台 E2E | PR 默认跳过的 exhaustive 项不能算已通过 |
| GUI 与面板 | 有源码权限时冻结提交、执行消费者测试和一次性内核组合 | 无访问权限时提交脱敏样本和待验证清单，不宣称兼容完成 |
| 本机和设备 | macOS/Windows 宿主资源、网络切换、系统 DNS 和真实 GUI | 编译、模拟和 hosted runner 不等于全部真实设备验收 |
| 发布与生产 | 独立授权的发布和部署流程 | 本任务的 PR 合并不等于生产验收 |

实现阶段沿用现有入口：

```sh
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings
RUST_MIN_STACK=16777216 bash scripts/test-workspace.sh
cargo build --release
```

scripts/test-workspace.sh 默认覆盖 workspace、all-features、单元、集成和 doctest，并检查测试布局；带 --test 是聚焦回归，不是完整资格。编辑期间可以聚焦复现，交付时不能把聚焦结果当作全量通过。

代表性最小构建和每协议最小构建采用当前 ci.yml 的命令；新 zero-runtime 的最小和可选能力矩阵应在引入该 crate 的 PR 内一并增加。

## CI 路径覆盖

现有 CI 接受指向 develop 的 PR；ci-scope 根据路径选择昂贵检查，显式 dispatch 和定时任务采用完整范围。不能假定每个 PR 已跑全平台、exhaustive 或全部 ignored 测试，也不能假定仓库保护规则已经配置。

新增 crates/runtime 等路径和代码迁移时，必须同步审查 .github/actions/ci-scope/scope.mjs、其测试，以及受影响的协议互通和 TUN workflow 路径过滤器。特别是宿主、engine/core 的生命周期变更也可能影响系统捕获，不能因新路径未匹配就跳过检查。

CI scope 调整应保持或补齐真实覆盖，不降低现有门禁。需要工作流手动触发但云端缺少权限时，输出精确的 ref 和待触发任务，由有权限的执行者接续；不把待触发状态写为通过。

## 消费者基线

本次本地读取的 GUI 为 zerodenet/znet-sink，HEAD fd198ba52711f018334860b8a6704546ccd349fd，但工作区有未提交改动；这些改动不属于云端可直接检出的基线，也不在此次任务中自动提交或上传。面板为 zerodenet/zboard，HEAD f19994768204485c479f72b2832a11771e872502。

M0 必须重新冻结云端或 CI 真正可取得的消费者提交和待对比的旧内核版本，记录现有测试、fixtures 与真实组合场景。不能仅用上述 SHA 声称已复现本机工作区，也不默认修改这两个消费者仓库。

GUI 验收覆盖 IPC 解析、事件订阅和 stats reset、配置投影、进程及 TUN 生命周期和 zero-rule 编译兼容。面板验收覆盖配置 renderer、build-info、validate、reload 回执、无变化发布、事件身份幂等和流量结算。

当前 GUI 的真实内核版本矩阵包含需要外部 captures 的 ignored 测试；应准备捕获后显式运行。没有 captures 的普通测试通过不能充当这项验收。
<!-- /source-section: 19 -->

<!-- source-section: 22 -->
# 验收矩阵与工程检查

以下是未来实施的验收要求，不是本设计已经通过的测试结果。

| 场景 | 必须证明的结果 |
| --- | --- |
| 配置校验 | 有效旧配置继续通过；非法协议参数清晰报错；不启动设备或修改持久化 |
| Stream 与半关闭 | 业务载荷、EOF 和双向关闭一致；慢客户端受背压 |
| Datagram | 固定及多目标、报文边界、响应归属、超长和队列满语义正确 |
| 链式与多载体 | 复用、双流、重连遵守已选前缀，无静默 direct |
| MUX 与 QUIC | 慢子流握手不阻塞其他流；子流结束不误关共享连接 |
| Packet 与转换 | MTU、IPv4/IPv6、回程关联、源地址和模式保持，未支持转换明确拒绝 |
| 配置更新 | 无变化复用；普通规则更新不切断连接；重建仅中断受影响连接；失败不提交新版本 |
| 失败恢复 | 先清候选再恢复旧资源；恢复失败隔离故障并保留控制面；实际中断如实报告 |
| 网络变化 | 旧出口连接不能在新代际错误发送；失败不撤销 DialPolicy |
| 停止与 TUN | 立即关闭准入并取消业务；确认任务及 I/O 退出、拥有设备关闭、路由与 DNS 恢复后才正常退出；取消发出或 abort 不能冒充完成，失败明确报告，借入资源保持拥有关系 |
| 主体与统计 | 撤权终止存量，限速实时作用于存量；计量与结算语义保持 |
| 事件 | ACK、snapshot、增量、缺口、重试、幂等及 durable 义务保持 |
| GUI | 现有 IPC 解析、配置、管理待命、TUN、事件和统计在新内核上通过 |
| ZBoard | renderer 产物可 validate 和 reload；真实回执、无重启发布与事件消费通过 |
| 嵌入与 FFI | Rust 迁移样例及实际下游编译；FFI ABI、释放、线程与旧功能保持 |
| 低资源替换 | 1 核 1 GiB 运行场景下重复更新与失败恢复；同一资源所有者无新旧运行实例重叠，峰值 RSS、任务、线程、FD 和缓冲可追踪，无累计泄漏 |
| 容量与空闲回收 | 旧而活跃、暂时安静、单向活动和半关闭业务不被容量淘汰；无占用池资源可回收；借用与回收无竞态；UDP 超时及迟到响应正确；有效防重放状态不被 LRU 清理 |

M0 应保存旧内核产生的配置与 wire 样本，并在后续阶段用同一组样本和一次性真实内核交叉运行。快照测试只能验证形状，不能替代 ACK 时机、任务退出、网络恢复与真实数据传输。

性能基线覆盖吞吐、首包和连接延迟、UDP 并发、空闲资源、峰值 RSS、任务及线程、FD、队列、连接池、分配、锁竞争、二进制与编译成本。1 核 1 GiB 是必测运行部署场景，覆盖稳态、配置替换、失败恢复和重复更新。资源默认采用保守的固定上限、按需分配，并允许配置调整；直接在队列、缓存、池、握手及任务入口实施有界限制，复用必要的小工具，不增加独立的通用资源调度、动态扩缩容或全局分配追踪框架。准备数据与并发构建同样有界；耗尽时明确背压或拒绝，不无限排队。数值默认值、测试负载和回归阈值按基线测量冻结，不承诺任意业务规模都能放入 1 GiB。

按仓库现行要求，每次实际行为迁移执行：

```sh
cargo fmt --all
cargo check --workspace
RUST_MIN_STACK=16777216 cargo test --workspace --all-features
cargo clippy --workspace --all-targets
cargo build --release
```

feature 组合检查补充默认、最小和关键可选组合，防止验证功能拉起数据面或形成依赖环。客户端和面板各自的集成测试单独报告；本地通过、发布、部署与生产验收始终是不同事实。

## 契约冻结与兼容执行门禁

问答已经确定架构方向和行为取舍。以下是实现前后的必交付规格和证据；不能把 Page 保存、文档签名或本地编译当作运行验收。PR00 保存本方案，PR01 采集旧行为和资源基线；PR02 在任何协议切换生产路径前验证目标 Rust 声明及统一擦除辅助。数值预算由测量产生，不让后续任务临场随意填写。

| 门禁 | 要固定的内容 | 证据与负责阶段 |
| --- | --- | --- |
| CF01 原生/动态接口 | 关联 Stream、Tx/Rx、Control 与 Options 的绑定，Future 的 Send 与拥有边界 | PR02 编译契约用例：仅 Stream 实现、非 Sync 接收器、非 Unpin 流、动态注册与调用 |
| CF02 依赖与 feature | traits/core 保持 no_std，config validation 不启用协议 runtime，公共运行接口无 proxy 依赖 | PR01/02 最小 feature 与依赖树；实际代码依赖变化时更新 CI scope |
| LF01 单实例替换 | 有副作用构建发生在目标旧资源确认释放后 | PR07B 重复重载及阶段故障注入，记录资源所有者及代际无重叠 |
| LF02 关闭确认 | 取消发出、任务退出、I/O 关闭、系统状态恢复是不同事实 | 参考现有 shutdown 与 management_lifecycle 测试扩展；超时和恢复失败不能报告成功 |
| IO01 Stream | 明文接受、flush、半关闭、预读、延迟确认和响应失败 | PR04/06 各协议 Stream 与 carrier 基线互通 |
| IO02 Datagram/Packet | 投递状态、单接收者、超长、取消中途写、队列满、迟到响应 | PR05/09：不重复投递，不把 retryable 压力记为最终 discard |
| RT01 路由与 DNS | 模式/bypass/规则、NeedFacts、fake-IP、bootstrap 和上游链约束 | PR08 以固定配置与输入事实对照旧决策；循环明确失败 |
| EX01 外部契约 | V1 原始配置、命令响应、事件、错误、ACK 时机和修订关系 | PR01 保存真实旧样本；PR10 新内核与固定消费者组合执行 |
| RS01 低资源 | 1 CPU/1 GiB 下启动、稳态、重载、失败恢复、持续新请求拒绝 | PR01 基线，PR07/11 新旧对照；记录峰值 RSS、线程/任务、FD、缓冲和回收后余量 |
| MIG01 删除边界 | 每个旧 claim/operation/manager 桥是否仍有实际调用者 | 对应 PR 删除条件与 PR11 最终检索、依赖和真实路径核对 |

### 兼容样本与场景清单

目标保存位置为 design/v0.0.3/fixtures 与 validation.md；它们属于工程设计证据，不是运行期依赖。样本必须来自固定旧内核或既有消费者的真实测试输入，记录来源提交、feature、命令及可脱敏字段，不能手写一份“看起来兼容”的 JSON 当旧基线。

| 样本/场景 ID | 内容与检查 |
| --- | --- |
| V1-CONFIG | 各协议最小和代表性载体配置、完整 renderer 产物、默认值、相对路径与无效私有字段路径；旧配置在新内核验证与执行 |
| V1-CONTROL | build-info、status、配置成功/无变化/失败、selector、关闭连接、TUN 控制等现有方法的响应及退出语义 |
| V1-EVENT | engine、config、flow.snapshot 与后续增量、flow.completed、stats.reset、序号缺口、ACK 和 Webhook 重放；身份和结算不重复 |
| TX-BOUNDARY | 调用方超时发生在准备、停旧、持久提交和响应发送各阶段；通过状态核对真实结果，不盲目重试重建 |
| RECOVERY | 端口冲突、候选清理失败、恢复旧实例失败、持久化失败及崩溃窗口；控制面可报告实际状态 |
| IDLE | 长而活跃、暂时安静、单向活动、半关闭、空闲池借用竞争、UDP 截止与迟到包、防重放有效期 |
| GUI-V1 | 现有 IPC 解析、配置往返、管理待命、TUN 完成确认、父进程退出、事件与统计；不依靠客户端升级适配新内部模型 |
| PANEL-V1 | renderer → validate → reload 确认 → core_instance_id/revision 查询 → 发布回执，失败不偷偷重启；Webhook 仍为 zero.event.v1 |
| RUST-FFI | 公开 Rust 变更迁移样例及实际下游编译；FFI 符号、分配释放、线程、句柄与旧功能范围保持 |

消费者实际入口为 [GUI 事件规范化](references.md#consumer-gui)、[GUI 命令等待](references.md#consumer-gui)、[面板配置发布阶段](references.md#consumer-panel)、[面板不确定结果判定](references.md#consumer-panel)。GUI 工作区未提交修改不属于云端固定基线；云端需固定实际可检出的消费者提交，或以脱敏样本记录尚待真实组合验收。

### 保守资源预算的落地范围

整个应用进程复用一份 Tokio 运行环境；RuntimeHost 接收现有 Handle，不为每个 Proxy、协议、DNS 或组件新建 Runtime。FFI 当前仅 Engine 的功能范围不因此暗中扩展。I/O 转发优先同一条调用链，正常业务不因每个包装层增加任务、内存管道或缓冲副本。CPU 密集和阻塞工作受有限并发约束，不为每个连接启动线程或任由 blocking 任务扩张。

| 所有者 | 必须有界的资源 | 配置与默认值规则 |
| --- | --- | --- |
| 宿主/会话执行器 | 未认证握手、活跃逻辑会话、等待准入、组件后台任务 | 使用保守默认和显式上限；容量前置检查，排队有截止，不无限创建任务后再拒绝 |
| 协议实例 | 池连接、子流/UDP 注册、分片重组、协议缓存 | 优先使用已有协议配置字段和固定上游限制；不在 proxy 重复解释或另设冲突上限 |
| stack/Packet | socket 队列、报文及字节预留 | 复用 runtime.network.udp_receive_queue，不把每 stack 的上限错称进程全局内存上限 |
| 事件/Connector | 内存投递数、字节、源队列、outbox/磁盘保护 | 复用已有 api dispatcher/outbox 参数及可靠义务，不以省内存丢弃未确认事实 |
| 准备/配置事务 | 规范化输入、文件读取、构建并发和补偿记录 | 候选计划有界，无双运行实例；不复制整套池或 stack |

已核对的旧默认包括 TCP idle 300 秒、UDP upstream idle 30 秒、Packet-to-Flow 每 socket 队列 64 项；这些值用于兼容基线，不因“默认保守”擅自缩短业务寿命。当前 UDP 字节上限的 null 含义及每 stack 作用域也要保持，新增资源约束须明确配置映射和行为账本。[TCP 默认](../../crates/proxy/src/runtime/tcp_ingress/runtime/model.rs#L68)、[UDP 默认](../../crates/config/src/model/mod.rs#L222)、[队列及字节作用域](../../crates/config/src/model/udp_queue.rs#L1)。

PR01 的预算表必须逐项记录现有字段及缺省值、实测使用量、建议保守值、可配置入口、作用域、耗尽行为和兼容影响。已有配置复用；确实缺失的硬上限才增加最小可选字段，并验证旧 GUI/面板配置往返，不把所有限制扩成一套新的资源配置 DSL。协议安全状态若不能提前回收，满时拒绝新增，不以统一容量算法改变其安全窗口。

1 CPU/1 GiB 测量针对构建好的运行二进制，编译和互通参考进程另计并明确环境。报告基线和目标的同一负载、连接数、包大小、feature、系统及采样方法，覆盖稳态和最坏切换峰值；不得只报告平均内存或在 OOM 后宣称“最终回收正常”。具体 RSS 余量、吞吐/延迟回归阈值在基线后形成数字门禁，未取得测量前保持待验收，不用猜测数字替代证据。
<!-- /source-section: 22 -->

# PR00 检查与 PR01 交接记录

## PR00 实际执行范围

工作环境为云端 `/workspace/core`，Linux x86_64；实际 base 为 `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`。只新增本目录工程文档、来源记录及只读检查辅助；未改变运行代码、依赖、配置、产品版本、锁文件政策或 CI。

检查命令：

```sh
python3 design/v0.0.3/check-plan.py
git diff --check
git diff --cached --check
git diff --name-only 2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a
```

`check-plan.py` 仅使用 Python 标准库：验证源分节数量/摘要/唯一归属、全部 G/M/PR 与门禁、17 张任务卡字段、代码围栏和本地链接/行号。它不访问 Page、消费者仓库或外网，也不执行 Rust 测试；在线引用只检查明确的 HTTPS 形式。本次全文还与读取的完整源快照逐节比对，确认仅发生声明的链接转换。

已执行完整源快照比对：revision 7 的 66,787 字符、406 块按全部 25 个分节保存，仅发生声明的链接转换与分节末尾换行规范化。原始正文 SHA-256 为 `2e5bb071f1c51f1767d097a86c0142ac10cc581e1fadd8d0fdf7910d91dc1657`。

已通过本目录检查：25 个分节摘要、G0–G10、M0–M6、10 项门禁、17 张完整任务卡、133 个本地文件/锚点/行号链接及代码围栏。20 个在线引用仅核对 HTTPS URL 形式，未验证远端可用性。提交与 PR 描述另登记最终 diff 和干净检出复验结果。

本目录不是 build 或 CI 运行依赖。Rust/workspace、互通、CF/LF/IO/RT/EX/RS/MIG 门禁及消费者资格均未由 PR00 执行。

## PR01 开工前环境待办

| 环境项 | 本次观察 | 下一单元处理 |
| --- | --- | --- |
| Rust stable / cargo / rustfmt / clippy | 当前 PATH 均无可用工具 | 在云端补齐，记录实际版本，先复现基线 |
| protoc | 当前不可用；CI 使用 35.0 | 按仓库 setup-protoc 方式准备，不改项目依赖 |
| C/C++ / CMake / pkg-config | cc、make、pkg-config 可用；CMake 尚未就绪，C++ 待核对 | 依据合法构建依赖补齐并记录 |
| Python / Node / Bash | Python 3.12.14、Node 24.19.0、Bash 可用 | 记录用于基线的实际工具版本 |
| OpenSSL | pkg-config 返回 3.5.7 | 按实际 feature 验证构建依赖 |
| Go 参考工具链 | PATH 有 go，但 go version 返回 Unknown option，未确认 Go 编译器 | 按各 interop workflow 固定要求准备，不能仅凭路径算可用 |
| Cargo.lock / target | lockfile 被忽略且不存在，无 target 缓存 | 按现有脚本首次解析并记录；不 cargo update，不改追踪政策 |
| 设备 / 特权 / RS01 | 无 /dev/net/tun；Docker、所需能力及 CPU/内存约束尚未验证 | 特权/平台项按实际环境或现有 CI 接续；真实 1 CPU/1 GiB 运行并采集峰值 |
| Actions 可见性 | gh api 读取基线 Actions runs 返回 Forbidden | 核对可用只读接口或由有权限执行者提供结果；未声称基线 CI 通过 |
| 消费者 | 本次仅保存源方案记录，未冻结或运行消费者 | 固定实际可检出 GUI/面板提交及 captures，登记权限和设备缺口 |

## PR01 具体交接

必须先确认 PR00 已合入，然后重新核对最新 develop，从该提交创建 `codex/v003-pr01-baseline`。读取全部方案及当前 AGENTS.md，不沿用 PR00 工作分支继续堆叠实现。

1. 登记实际内核 base/head、旧二进制来源/feature/摘要、依赖解析、工具与系统版本。消费者重新冻结可检出提交，不能复现的未提交工作区明确排除。
2. 在 `design/v0.0.3/fixtures` 保存来自固定旧内核/消费者真实输入及输出的脱敏样本和来源清单；覆盖正文全部 V1、TX-BOUNDARY、RECOVERY、IDLE、GUI/PANEL 和 RUST-FFI 项，不手写猜测旧行为。
3. 盘点公开 Rust/FFI 表面、逐接口/逐协议真实调用路径、依赖树、no_std/validation-only 和默认/最小/可选 feature；登记现有失败与环境限制。
4. 在实际约束的 1 CPU/1 GiB 环境对构建后运行二进制测启动、稳态、重载/失败恢复、重复更新和容量拒绝，记录 RSS 峰值、任务/线程、FD、缓冲、队列、池、分配与回收后余量；编译及参考进程另计。
5. 资源预算表逐项记录当前字段/默认值、实测量、建议保守值、配置入口、作用域、耗尽行为与兼容影响；相同负载冻结吞吐/延迟和 RSS 余量数字门禁。本 PR 没有预填阈值。
6. 建立当前 CI scope/interop/TUN 过滤器覆盖清单及真实消费者组合计划，明确每项所需权限、captures 和平台。交付 PR01 的证据、实际 head、外部待验收及源码回退边界。

PR01 完成后仍需评审合入，才开始 PR02；本次不会自动推进后续单元。

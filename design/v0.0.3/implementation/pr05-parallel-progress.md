# 并行集成进度与失败证据（未完成资格）

所有工作继续同一 `codex/v003-pr00-plan` / Draft PR124，基线为 develop `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`。产品仍为 `0.0.3-dev.202610162206`，协议固定 reference 未升级。本文整理到代码提交 `9cc8bbeb005ccca7860a2ab83d7001a69c860517`，**不是 ALL17 完成报告或当前 head 资格**。

最后已推送且完成本地资格的检查点仍为 `c0682292d3b32cf5b9f6748cc3bebae3fc68325d`。后续并行代码尚未推送，没有对应的当前 head 远端 CI。该旧 head 的 Windows route-switch 明确没有执行，不能把旧 workflow 成功当成全部场景通过。

## 已接入及保留的边界

- 受控 TCP/UDP/carrier 保存实际原生 owner；取消、超时、Drop 和任务退出都不等于原始 FD 已关闭。只有实际登记任务 join 与原生关闭分别确认后才释放相应容量，Unknown 保留 owner、许可和原错误来源。
- Direct UDP 新库路径使用同一实际 peer task/driver、窄宿主授权和私有 binding namespace，支持实际关闭后循环使用预算。测试覆盖共享源端口、逻辑隔离和实际网络 generation 轮转；生产仍未激活，显式调用方测试预算不是默认值。跨地址族安全重试也仍有未迁移边界。
- SOCKS CONNECT 已交付标准 AcceptedStream 并复用同一准入、路由、统计和 relay。HTTP CONNECT、forward 消息及 Trojan 普通 TCP 的代码也已接入该宿主身份路径；forward 保留原消息执行器，Trojan 保留无额外响应字节的 wire 行为。UDP/Mux 等剩余入口没有被假称全部迁移。
- 宿主唯一签发 sealed SessionIdentity，认证 sidecar 保留真实 principal 可空时的完整策略。弱宿主源、静态依赖与实际 pool/role 退休检查分别承担职责，不能把静态选项相等当作资源许可。详细限制见 [retained DNS carrier 记录](pr05-retained-dns-carriers.md)。
- Mieru 原生库使用实际同池、同任务及原 FD 关闭证据。gRPC 的生产原生资格已收紧，继续走实际旧共享 owner；准备返回原 H2 driver 和原 read/write future 的 seam 已实现，完整有限 owner 尚未完成。没有用新的独立池掩盖共享连接退化。
- Listener 故障的实际 join/关闭及监督交付与候选接受分开；补偿、DNS prepared token 取消重入和启动代次一致性继续修正。真实 candidate Ready ACK、完整 RuntimeHost M4 和所有转交 I/O 闭环仍未完成。

## 实际验证

[完整清单及 247 个原始证据压缩件](pr05-parallel-progress-validation.json)保存逐文件 SHA-256、命令、冻结 head/tree/lock、失败和未执行项。多个过滤范围重叠，**不能相加成 workspace 通过数**。

| 冻结范围 | 实际结果 | 限制 |
|---|---|---|
| root20 | fmt/check/no_std/依赖检查通过 | 测试链接 ENOSPC/LLVM 错误，未进入运行；release/minimal 也受空间耗尽影响。保留旧缓存清理清单。 |
| root21 `12a68916` | fmt/check/release/no_std/minimal/依赖检查通过 | strict 和完整测试失败；Proxy lib 因确定性夹具挂起被 SIGTERM，中止记录独立保留。没有完整 workspace aggregate。 |
| root22 | 无资格 | root 在 cherry-pick 冲突后误启动 runner；相关进程已中止，全部结果标为 INVALID_ABORTED。 |
| root23 `29d6b8b1` | fmt/check；HTTP 协议组件 3/0、纯协议 10/0；Engine static 5/0；retained recipe 9/0 | 仅各自冻结范围，不代表后续新代码。 |
| root23 SOCKS/peer/TCP | SOCKS component 6/0、relay 3/0；peer owner 12/0；TCP idle 1/0、TUN 正向 1/0 | HTTP/SOCKS integration target 因新增测试缺 EventSource import 编译失败，未执行；已另行修复。 |
| root23 gRPC/诊断 | 原 gRPC 三路复用测试 1/0；公开诊断原四项 4/0；health 11/1 | 原先实际 3 条连接已恢复原测试要求；health 剩自动 URLTest 启动代次失败。 |
| root23 新资源/恢复 | bound wrapper 1/0、弱关闭观察器 1/0 | endpoint 16/1、Source 1/1、UDP idle 0/2、恢复 0/1、冷取消 0/1，均保留原始失败。 |
| root23 架构守卫 | runtime 186/3、workspace 24/2 | 已修正生产 feature gate、claim 投影及真实测试夹具的检查范围；后续 std-only 同源码守卫 189/0、26/0，仅证明静态边界。 |

root21 runner 的原统计正则还匹配了 GitHub 错误注释中重复的摘要。manifest 保留其原值，但将 counts 置空；按整行摘要统计得到的 2484/23/157 仍遗漏被中止的 Proxy lib，明确不是完整测试总数。没有用重跑抹掉首次失败，也没有将编译错误、零测试过滤或平台跳过算作行为通过。

root23 后已准备修复：真实命令回执先于取消唤醒、夹具真实 TCP EOF/retirement 协调、Source 配置枚举拼写、同一 DNS prepared token 及实际组件候选的取消重入、已物理关闭并交付故障后的有限补偿重入、启动全过程同一捕获 snapshot，以及冷取消断言的原始 io payload 检查。这些修复和新增 HTTP forward/Trojan/gRPC seam **仍待下一冻结树运行验证**。

## 依赖与资源测量

HTTP `runtime` 的可选依赖只给锁中的工作区 `http@0.0.1` 增加已有 `zero-platform-tokio`。全图 offline metadata 的锁摘要从 `a003359e…` 变为 `ffed93de…`；441 个包总数不变，所有 registry 版本、来源及 checksum 逐项相等。原始比对见清单中的 `pr05-parallel-http-lock-change.json.gz`。

[Direct UDP 既有测量](direct-udp-budget/README.md)保留实际 steady/切换/突发缺包，没有凭外层 peer 上限或 RSS 推定内层预算。[观察器](direct-udp-budget/observer/README.md)补充真实 Tokio metrics 和默认 CLI 独立 old/new pair；本文截止时尚未产生新的运行测量。旧/新 Tokio 1.53.3/1.54.0 差异如实记录，业务 owner 队列字节无公开读口时保持 null。

## 后续必要工作

下一冻结树先验证上述失败和新生产入口，再运行完整 workspace、严格检查、最小 feature/no_std/依赖检查及默认 release。生产 Direct UDP 预算和启用、其余 shared protocol 实际 owner、mixed DNS 每次连接预算、全部入站/Packet/Control/Mux、Ready ACK/RuntimeHost M4、后续 runtime 提取及最终消费者/FFI/资源/平台资格仍需完成。当前没有合并、发布、部署或访问用户 Mac。

## root24–26 后续冻结结果

证据清单现保存 303 个可校验原始压缩件。root24 暴露的 gRPC 原始 H2 reset 错误源丢失已由 root25 的全部 10 项原断言回归通过验证。root25 的 HTTP 14/0、Trojan 新入口 3/0 与原入口 3/0、SOCKS TCP 20/0、UDP 29/0 以及原 gRPC 共享复用 1/0 均在同一干净冻结树运行。实际 SIP003 初始化超过 5 秒时仍未 ACK，释放真实 gate 后才 Ready，该回归 1/0。

root25 的 peer 14/1 是 panic 检查依赖 Display 文本；后续断言改查原始 ChannelError/JoinError、实际 TaskId/Critical 类别与同一 Arc，root26 peer 15/0 通过。root26 的 VMess 库 22/0 和合同 11/0 通过，但 root 编写的 wire 夹具误用私有构造器使整个 proxy_dispatch 目标编译阻塞，相关集成没有执行；已改用原有公开 request bundle，待下一冻结树运行。strict 仍失败，原始 type complexity 与 DNS needless_return 诊断及对应修复均保留，没有声称严格检查通过。

当前代码还集入 Ready 前停机的同一事务补偿、有限 gRPC owner 库、Service relay 每次需求与 Direct UDP 副本/字节一致计费，以及 VLESS 普通 TCP 及原始 SNI 接线，均须分别运行验证。原 listener Bound 尚未完整交给持久原生关闭控制图；Scope 退出、空 report 或 Drop 不证明原 FD 关闭。资源生产启用和整个 RuntimeHost M4 尚未完成。

[旧源 library 实测](direct-udp-budget/old-library-results.md)已保存 policy 与 network 各三轮；20–21 是 runtime-owned async task 峰值，排除 blocking pool，不是全部工作或关闭证明。全部缺包及非原子 proc 限制保留，当前源与 17 个实际 feature 匹配的 CLI 配对仍待冻结构建。

## root27–31 后续冻结结果

最新完整 Proxy 库冻结点为 `9e3f3ed33024e32f61685b0bff5bb4611f365109`，609 成功、0 失败、2 忽略；全 workspace、all-features/all-targets 类型检查与格式检查通过。SS 新收流入口 3/0 保留五种真实 wire 子案例，静态 runtime 边界 189/0、架构 26/0。过滤目标互有重叠，不能相加为总测试数。

root30 Proxy 的真实停止补偿故障已修复：被接受事务在停止时仍驱动同一旧快照恢复 ACK 和 DNS fake-IP 应用 hook，保留全局 closing 状态。VMess CA 原 raw TLS/WS 测试继续走真实 File grant 并证明 connect 不重读，原未注册 gRPC owner 明确在读取/拨号前返回 Unsupported；这不等于新 native gRPC owner 已完成。旧实际 CA+gRPC 互通在 root30 的 dispatch 97/0 中通过。

root27 VLESS 新夹具错误地给匿名主体设置 device limit；日志捕获真实拒绝原因后，另行修正为合法完整认证以及匿名 rates-only 案例。root30 真实入口 3/0，通过过程未改 Engine 准入或弱化超时/原认证断言。root30 SS 的三项失败同为配置语法不合法，root31 分别使用 SIP022 单 PSK 与 SIP023/EIH identity_password 及托管用户后全部通过，保留原加密/重放/响应语义。

严格 Clippy 仍失败：root31 记录 lib 109 项和 lib-test 17 项诊断，涉及尚未接入生产的私有实现、枚举大小、类型复杂度与同步测试锁的词法作用域。不能以编译/局部运行替代严格门禁或全套资格。新的 pre-native TCP owner、同句柄视图及预绑定 lease 接线仍在隔离树实施和审查，尚无运行资格。提前准备 VMess/Trojan 会修改活跃认证 profile 的问题已发现，正改为安装阶段提交；原配置重载后的在线凭据更新仍须保留。

原始失败、编译阻塞、后续修复及独立诊断全部收入验证 JSON 和压缩证据；Windows 只有交叉编译，未伪称原生运行。当前仍不合并、不发版、不部署，未访问用户 Mac。

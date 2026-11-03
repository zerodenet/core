# Agent C：集成、审查与本地验收交接

你是**一个云端环境中的主 agent**，工作分支为 `codex/v003-cloud-integration`，不直接修改 develop/main，不修改消费者仓库。先读 [任务入口](README.md)、A/B 任务和 [验收标准](acceptance.md)。由你在同一环境中派生 A/B 子 agent，使用各自 Git worktree；三个分支不表示三个环境，用户只需启动你一次。当前准备工作没有启动任何云端 agent；远端分支只是预置入口。

## 开工和共享边界

1. 验证远端基线、A/B 初始 head 与 baseline.json，记录实际 SHAs、工作树和工具链；不能从远端 develop 重建任务起点，不能恢复已删除 listener/handler 链。在这个环境中一次性准备工具链/依赖，再创建或复用 A/B 的独立 worktree，将绝对工作路径、任务卡和分支分别交给两个子 agent。不得让两个子 agent 在主目录编辑或切分支；不得自动另建云端环境。
2. 核对 A 的准备值/事务边界与 B 对既有宿主接口的使用。共有 core/platform/runtime 契约默认冻结；确有缺口时指定唯一实现人及提交依赖，协调后再让另一分支使用。
3. 共享文档、全局守卫、CI 由你汇总。实现 agent 的轻量静态检查可以并行，但共享接口改动和集成分支写入必须串行。
4. 源码工作并行，Rust 构建与对应测试由你按冻结提交排队、完整收集结果；默认共享一个受独占使用的 target 和下载缓存。不同时启动多套全量或让另一工作区覆写正在运行的测试产物。轻量静态检查可并行，确需独立 target 时再分配。使用当前环境提供的子 agent 调度工具等待进度与接收阻断项，不要求用户手工搬运两个环境的结果。

## PR 审查与集成顺序

- A/B 各自提交 Draft PR，base 必须为 `codex/v003-cloud-integration`，使用 [模板](pr-template.md)；实现完成后补齐真实验证，不能把空分支当完成 PR。
- 审查 A 的 config/engine/runtime DAG、磁盘与 Ready 顺序、一次 ACK、engine-only/FFI 范围；审查 B 是否确实复用原认证池/endpoint、实际 provider 和 Scope。
- 首轮建议先集成 A。明确没有缺失前置证据后，可将 A 合入**集成分支**；这不构成 develop 集成或整体通过。
- 再让 B 更新到当前集成 head。B 自行解决所属代码冲突，你确认交叉契约和必要回归后再集成 B。不得在不检查的情况下采用 ours/theirs，或用强推覆盖其他 agent。
- 使用普通追加提交更新分支，避免改写其他 agent 已依赖的历史。A/B PR 合入集成分支后保留提交及审查记录。
- 最终可创建/更新 `cloud-integration → develop` 的 Draft PR，明确“等待本地验收，禁止合并”；不开 auto-merge，不创建 release/tag/部署任务。基线 Draft PR 仅作为初始交接审阅入口，不自动合并。

## 验证和剩余项对账

运行候选对应的统一检查，记录源码和二进制 SHA、命令、结果、失败与忽略，不引用旧 head 的通过结果。当前主 CI 已允许集成分支为 PR base；未覆盖该 base 的专门 interop/TUN workflows 必须按精确候选 ref 手动触发，不能把未出现的 check 当通过。不得触发 release/publish/nightly 工作流。

核对 Z003-09/10 的实际关闭边界、Hysteria2 子域及剩余 Z003-01/02/03/04/05/06/07/08；已完成 listener 等职责不重新列为待迁移。其余 Mieru/MUX/载体、Packet/TUN、System/detour DNS、生产预算、G8 投影及 G9/G10 资格保持逐项记录，不任意折算百分比。TEMP-WG-OUTER-UDP-01 始终单列，首轮不得标已关闭。

## 本地交接

冻结最终候选后交付：候选分支和完整 SHA、A/B PR 与提交列表、当前证据表、适用 feature/互通结果、完整失败/忽略/待验项目、旧链归零检索、兼容性影响和恢复边界。原始日志放 CI artifact 或仓库外，不提交临时 gzip/快照/构建输出。

本地随后在独立验收 checkout 使用该精确 SHA；不得覆盖已有 dirty 工作树或自动拉取/合并到 develop。真实 GUI/面板/TUN 和平台系统恢复由本地适用环境执行；读取/测试消费者不授权修改消费者代码。若必须改变外部契约，报告具体差异并交用户决策，不静默适配其他项目。

全部结果清楚后停在候选交接状态。合入 develop 仍需用户后续决定，本卡不授权自动合并、发布或部署。

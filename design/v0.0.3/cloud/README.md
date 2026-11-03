# v0.0.3 云端并行任务包

2026-11-03 用户授权：整理当前本地内核增量、分组提交并推送独立云端基线，随后以多个 agent 的分支和 PR 并行推进，最终拉回本地验收。**本轮及云端任务不得合入 develop/main，不发布、不部署。** 本包覆盖此前文档中的“不提交、不推送”和单一旧 PR 的历史执行安排，技术标准与兼容目标不变。

## 基线与开工

- 仓库：`zerodenet/core`，GitHub remote 为 `github`；不向另一个 GitLab remote 推送。
- 产品阶段：v0.0.3；不更新产品版本、协议上游基线或锁文件政策。
- 云端基线：`codex/v003-cloud-base`。源码检查点、分组提交、已运行检查和未验收范围见 [baseline.json](baseline.json)。本包所在提交及其远端分支 head 是开工时必须记录的实际任务基线。
- 集成分支：`codex/v003-cloud-integration`，初始与基线完全相同。Agent A/B 的 PR **base 均为此分支**，不得直接指向 develop。
- 本地起点 `bab00b6922600ecdc9461c28ddcf0630b8c730c8` 与远端 develop 分叉。本包没有合并、变基或覆盖其中任何一方；远端差异见 baseline.json。最终本地集成时单独核对重复补丁和版本提交，不能用强推替代。
- **只使用一个云端环境。** 用户启动主 agent C，由 C 在同一环境内派生两个实现子 agent A/B；不要求用户分别创建三个云端任务、容器或虚拟机。
- C 使用主 checkout，A/B 各使用同一仓库的独立 Git worktree 和分支。三个工作区共享 Git 对象、工具链与依赖下载缓存，但有各自的文件和 Git index，不能在同一个可写目录中并行切分支或编辑。

```text
一个云端环境
  └── 主 agent C：协调、审查、集成
        ├── 主 checkout：cloud-integration
        ├── 子 agent A → worktree A：cloud-config-transaction
        └── 子 agent B → worktree B：cloud-hy2-outbound
       共享工具链、Git 对象、Cargo 下载缓存和受协调的构建目录
```

云端网络、工具链、protoc 与权限只初始化一次；若提供 managed cloud environment 技能，先读取其运行规则。新 checkout 的 GitHub remote 可能叫 origin：C 先确认其 URL，再按本包需要配置 github 别名，不覆盖已有 GitLab remote。fetch 后记录实际 SHA、核对 clean 状态，在启动子 agent 前创建/复用 A/B worktree。分支已存在时直接使用，不重建或强制重置。不复制本地凭据、绝对路径、构建缓存或私有日志；版本化任务包不依赖它们。

默认共享一个 Cargo target，由 C 协调排队：同一时刻只执行一组 Rust 构建及其对应测试，直到该组结果收集完再交给另一工作区。A/B 的设计、代码修改和轻量静态检查仍并行；不同时运行两套全量，不把另一 worktree 的产物当本分支结果。共享 target 不等于并发写构建产物，不引入新的任务调度框架；确有隔离需要再使用独立 target，不强制复制整套构建环境。

## 给云端主 agent 的启动指令

只需在一个云端环境启动一次以下任务，不分别启动 A/B 的独立云端环境：

```text
在当前这一个云端环境中执行 design/v0.0.3/cloud/README.md。
你是协调主 agent C，读取 agent-c.md 和 acceptance.md，核对 cloud-base。
在同一环境内创建/复用 A/B 的独立 Git worktree，派生两个子 agent，
分别执行 agent-a.md、agent-b.md；明确给出各自 worktree 的路径和分支。
你负责共享契约、构建/测试排队、PR 审查和 cloud-integration 的串行写入。
不要要求用户另开两个云端环境，不在共享目录中并行切换分支。
只改内核，TEMP-WG-OUTER-UDP-01 单列，其他路径确认停旧后启新。
A/B PR base 为 cloud-integration，候选交回本地验收，不合入 develop/main。
若当前执行器缺少子 agent 或 worktree 能力，如实报告，不能假称已并行。
```

## 分工与 PR 依赖

| Agent | 分支 | 目标 / 原方案 | PR base / 依赖 |
| --- | --- | --- | --- |
| [A](agent-a.md) | `codex/v003-cloud-config-transaction` | Z003-10 → Z003-09；PR08B / PR07B，配置资源准备与事务整体收尾 | `codex/v003-cloud-integration`；A 内部先资源准备、后事务；不依赖 B |
| [B](agent-b.md) | `codex/v003-cloud-hy2-outbound` | Z003-02/03/06 的 Hysteria2 既有出站共享池完整职责域；PR04B / PR05 | 同上；不依赖 A 的新 API，保持基线配置与宿主接口；不以其他协议为前置 |
| [C](agent-c.md) | `codex/v003-cloud-integration` | 合同审查、冲突整合、当前源码验证与 G0–G10 对账 | 等 A/B PR 具备证据后集成到该分支；不合入 develop |

```text
cloud-base（冻结交接）
  ├── A：资源准备 → Engine/宿主事务 ── PR ──┐
  └── B：Hysteria2 出站共享池 ────── PR ──┤
                                           ↓
                              cloud-integration（C）
                                           ↓
                            精确候选 SHA → 本地平台验收
                                           ↓
                           用户决定后再集成 develop
```

A/B 可以并行，但 C 只串行写集成分支。A/B 不自行 merge PR、不互相 cherry-pick、不强推。C 建议先集成 A，再将 B 更新到集成 head、完成实际冲突与必要回归后集成 B；不能把两个各自通过的分支直接视为组合通过。远端 A/B 分支已准备并不表示实现 agent 已启动或 PR 已有代码。

## 文件所有权与协作

| 范围 | 首轮唯一写入负责人 |
| --- | --- |
| `crates/config`、`crates/router`、`crates/engine`、宿主配置事务、Proxy handle/reconcile 配置链 | A |
| `protocols/hysteria2` 出站、`adapters/hysteria2` 出站、QUIC 实际池/endpoint/driver 所有权、Hysteria2 出站登记块 | B |
| `crates/core`、`crates/traits`、`platform/tokio::contract`、通用 catalog/slot/provider、CI 和全局架构守卫 | C 协调；首轮默认复用现有契约 |
| `AGENTS.md`、兼容账本、实施总表、cloud 总表 | C；A/B 在各自 PR 说明记录更新建议，由 C 汇总 |
| Agent 自己的定向回归及局部模块说明 | 对应实现 agent |

共享契约若确有缺口，先提交具体签名、所有权理由及调用者影响给 C，确定唯一实现分支和依赖顺序再改；其他 agent 等该接口提交，不自行造替代 SPI。C 不能通过弱化关闭证明、缩小既有能力或改消费者来解决冲突。A 不改 Hysteria2 登记块，B 不改配置持久化/事务链。跨域需求用明确阻断项交接，不顺手扩范围。

## 已完成与尚未完成

全局 InboundListenerCapability、PreparedInboundListenerOperation、BoundInbound、definition 包装及旧 listener 执行链已删除；此前完整 TCP/UDP claim 包装、managed UDP handler/Any/resume 旧链也已删除。不要恢复它们或重复建立迁移任务。入站迁完不等于所有出站标准角色、共享原件和 TUN 平台资格完成。

首轮 A/B 之后仍需按现有职责域推进：其余出站池/载体、System/detour DNS 和失败重试所有权、剩余 Packet/设备/TUN 归属、适用资源预算配置，以及控制观测对账与完整互通/平台/消费者/1 CPU/1 GiB 验收。B 不承担全部出站债务；不得用 Hysteria2 子域通过关闭全部 Z003-02/03/06。详见 [验收与剩余范围](acceptance.md)。

## 固定约束

只改内核仓库。保留既有配置、CLI、zero.api.v1、zero.event.v1、FFI ABI 及客户端/面板语义；公开 Rust 内部迁移按账本说明。不得修改客户端、面板及其他仓库，不把消费者改造当兼容方案。

仅迁移既有能力，不新增协议组合、不升级上游、不造第二宿主或同资源双实例、不建通用调度/进程沙箱。保守默认、按需分配，容量不足拒绝新增而不淘汰活跃业务或有效安全历史。标准角色已选中后的撤销/失败不能切回第二套兼容池。

除 [TEMP-WG-OUTER-UDP-01](../lifecycle.md#temp-wg-outer-udp-01) 外，真实旧任务、用户与原始 I/O 清理确认后才能构造/启动替代资源；失败候选先确认清理再恢复。临时例外只限既有 WG outer_udp_proxy/reconnect，保持单独未关闭，首轮不修改、不扩大、不顺带纠正。

任务包、历史方案、当前实现的冲突优先级：用户最新授权及临时债务边界 → 本包当前任务拆分 → 已批准的 [proxy → runtime 修订](../implementation/runtime-extraction.md) → 冻结技术标准。历史 PR #124、任务卡的初始“未开始”和旧运行数字不表示本轮状态。

使用 [PR 模板](pr-template.md) 交付，按 [验收分工](acceptance.md) 记录真实结果。任务完成、提交、推送、PR、CI、云端集成、本地验收和最终 develop 集成必须分别报告。

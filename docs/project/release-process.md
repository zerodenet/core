# 版本演化与发布流程

本文定义 Zero Core 仓库的内部版本演化、发布审批、标签创建和制品生成规则。

该流程属于 Core 项目的工程治理规范，不是对外 API、配置或产品使用文档，因此不需要同步到 `zerodenet/docs` 仓库。对外文档只在公开接口、配置、协议能力或用户可见行为发生变化时更新。

## 基本原则

1. `develop` 是 dev 构建来源，`main` 是 RC 与正式版的权威发布分支。
2. 标签是所有版本与质量检查通过后的结果，不是发布检查的起点。
3. 版本号由发布工具根据当前版本和目标阶段计算，日常发布不手工拼写版本号。
4. 每条发布线只能向前演进，不能回退基础版本、阶段或 UTC 构建时间戳；`develop` 已进入下一基础版本的 dev 阶段时，`main` 仍可继续当前基础版本的 RC/正式版。
5. `Cargo.toml`、兼容性台账、Git 标签和 GitHub Release 必须表示同一个版本。
6. 标签不可移动或覆盖；同一提交的失败构建可以复用原标签重试。晋级成功后，同版本线已经被替代的预发布 Release 与标签会被删除。
7. 发布线严格遵循 `dev → rc → stable`：首个 RC 必须存在同基础版本 dev，正式版本必须存在同基础版本 RC。

## 当前分支模型

双分支的职责固定如下：

- `develop` 接受日常开发，并允许发布 `X.Y.Z-dev.YYYYMMDDHHMM`；时间戳使用 UTC 分钟，不允许无时间戳的 `-dev`。同一分钟只允许一个新 dev 版本。
- 第一个 RC 自动选择同基础版本最新的 dev 标签创建 release 分支，也可以通过 `source_tag` 显式选择；它不能直接使用 `develop` 的浮动 HEAD。
- 后续 RC 根据 `main` 当前 RC 生成新的 UTC 分钟时间戳；正式版自动使用 `main` 当前 RC 标签。`source_tag` 只用于显式覆盖来源。
- 下一基础版本的 dev 标签不阻断当前 `main` 发布线的 RC/正式版；下一基础版本一旦进入 RC 或正式版，就不再接受旧基础版本的发布。
- `release/promotion-source` 记录晋级来源。创建标签前必须验证该来源是发布提交的祖先。
- dev 标签必须属于 `develop`，RC 与正式标签必须属于 `main`。
- 版本历史校验只采用当前源提交的祖先标签。并行的较高 develop 版本不会阻止 main 延续较低的 RC 发布线；同版本线已发布 stable 的关闭规则仍然有效。

## 版本格式

新版本只允许以下形式：

```text
X.Y.Z-dev.YYYYMMDDHHMM
X.Y.Z-rc.YYYYMMDDHHMM
X.Y.Z
```

其中：

- `X`、`Y`、`Z` 为没有前导零的非负整数；
- `YYYYMMDDHHMM` 为固定 12 位 UTC 年月日时分，dev 与 RC 都使用这一构建标识；其首位非零，因此同时符合 SemVer 数字标识符不得包含前导零的要求；
- 正式版本没有后缀；
- 标签始终在版本前增加 `v`，例如 `v0.0.16-rc.202609040522`。

以下形式无效：

```text
0.0.16-dev
0.0.16-rc
0.0.16-bate.1
0.0.16-preview.1
0.0.16-rc.01
00.0.16
```

状态机仍可读取历史 `X.Y.Z-dev.N`、`X.Y.Z-rc.N` 和 alpha/beta 标签以验证旧发布并平滑迁移，但发布入口不再允许创建这些编号式版本。其他非标准历史标签只作为历史事实保留，不得作为新版本格式继续使用。

## 版本状态机

版本规则仍能读取历史编号式 dev/RC 以及 alpha/beta 标签，但标准发布路径固定为：

```text
dev < rc < stable
```

编号式 dev/RC、`alpha` 和 `beta` 不再由发布工作流创建。

### 同一阶段

dev 与 RC 的 UTC 分钟时间戳都必须严格递增：

```text
0.0.16-rc.202608131530 -> 0.0.16-rc.202608131531
0.0.16-dev.202608131430 -> 0.0.16-dev.202608131431
```

以下变化会被拒绝：

```text
0.0.16-rc.202608131531 -> 0.0.16-rc.202608131530
0.0.16-dev.202608131431 -> 0.0.16-dev.202608131430
```

`--allow-gap` 只保留给管理员验证旧的编号式发布历史，不参与新的时间戳发布流程。

### 切换阶段

进入 RC 阶段时生成当前 UTC 分钟时间戳：

```text
0.0.16-dev.202608131430 -> 0.0.16-rc.202608131530
```

RC 不能回退到 dev，也不能跳过 dev 从上一正式版直接进入新的 RC 发布线。

### 正式版本

正式版本只能从同一基础版本的 RC 演进：

```text
0.0.16-rc.202608131531 -> 0.0.16
```

不能从 `dev`、`alpha` 或 `beta` 直接发布正式版本，也不能在正式版本发布后继续创建同一基础版本的预发布标签。

### 新开发周期

正式版本之后，新基础版本必须增大，并从当前 UTC 分钟对应的 dev 开始：

```text
0.0.15 -> 0.0.16-dev.202608131430
```

新基础版本必须从 `dev.YYYYMMDDHHMM` 开始，不能直接发布 RC 或正式版。

## 权威文件

发布流程涉及三个不同职责：

| 文件 | 职责 |
|---|---|
| `Cargo.toml` | 当前构建身份 |
| `release/breaking-changes.md` | 已封板版本的兼容性与迁移记录 |
| `release/promotion-source` | 当前版本的不可变晋级来源 |
| `docs/project/release-process.md` | 版本如何演化和发布 |

`release/breaking-changes.md` 不记录工作流操作说明；本文件也不重复每个版本的消费者迁移内容。

## 本地命令

检查当前 Cargo 与兼容性台账：

```bash
./scripts/release.sh --check
```

计算下一个版本：

```bash
./scripts/release.sh --next dev --bump patch
./scripts/release.sh --next rc
./scripts/release.sh --next stable
```

在 `develop` 使用本地兼容发布入口时，只需提供基础版本：

```bash
./scripts/release.sh 0.0.17
```

脚本会根据当前 UTC 分钟自动解析为 `0.0.17-dev.YYYYMMDDHHMM`。如果该分钟的标签或对应正式标签已经存在，则拒绝创建 dev；需要新版本时等待下一 UTC 分钟。

本地入口确认发布后，默认将当前分支与新标签同步推送到所有已配置的 Git 远端。`--remote <name>` 可将本次发布限制到单一远端，`--no-push` 则只创建本地提交和标签。每个远端上的分支与标签使用一次原子 push，避免该远端只收到其中一项。

检查两个 Git ref 的版本变化：

```bash
./scripts/release.sh --check-transition origin/main HEAD
```

验证标签、Cargo 版本、兼容性台账和阶段对应的分支归属：

```bash
./scripts/release.sh --verify-tag v0.0.16-rc.202608131530
```

只预览封板差异：

```bash
./scripts/release.sh 0.0.16-rc.202608131530 --seal-only --dry-run
```

Windows 使用 `scripts/release.ps1`。PowerShell 文件只负责参数转发，实际规则统一由 `scripts/release.sh` 执行，避免两套状态机产生差异。

## 标准发布流程

### 1. 准备 Release PR

在 GitHub Actions 中运行 `Prepare Release`：

1. 选择目标阶段：`dev`、`rc` 或 `stable`；
2. 只有开启新基础版本时，`bump` 才决定使用 `patch`、`minor` 或 `major`；
3. dev 从 `develop` 自动生成 `dev.YYYYMMDDHHMM`；首个 RC 自动选择同基础版本最新 dev，后续 RC 从 `main` 当前 RC 生成 `rc.YYYYMMDDHHMM`；正式版自动识别 `main` 当前 RC。`source_tag` 仅为可选的来源覆盖；
4. 工作流同步修改 `Cargo.toml` 和兼容性台账；
5. 工作流创建 `release/v<version>` 分支；
6. dev PR 目标为 `develop`，RC/正式版 PR 目标为 `main`。

此时不会创建标签。

### 2. Release PR 校验

Release PR 至少经过：

- 发布脚本语法检查；
- 版本状态机测试；
- 当前版本契约检查；
- PR 基线版本到目标版本的变化检查；
- PowerShell 包装入口检查；
- 仓库原有格式、Clippy、测试和构建检查。

版本未变化的普通代码 PR 可以通过版本转换检查；一旦修改版本，就必须满足状态机。

### 3. 合并到权威分支

dev Release PR 合并到 `develop`；RC 与正式版 Release PR 合并到 `main`。

合并只表示版本契约已经准备完成，不表示标签和制品已经发布。

### 4. 创建标签

在 GitHub Actions 中运行 `Publish Release Tag` 并明确确认：

1. 工作流按阶段从 `develop` 或 `main` 读取版本；
2. 验证版本格式、历史演进、Cargo 与台账一致性；
3. 验证远端不存在同名标签；
4. 要求当前权威分支的精确提交已有成功的 `CI` push 运行；
5. 所有检查通过后创建 annotated tag；
6. 工作流显式调用 `Release` 构建制品。GitHub 默认 `GITHUB_TOKEN` 创建的标签不会再次触发 push 工作流，因此不能只依赖标签推送。普通本地 Git 推送标签仍可触发 `Release`。dev 在 `develop` 构建；RC/正式版在 `main` 构建；公开制品前要求精确提交的权威 CI 成功。

标签不能从本地开发分支直接推送作为标准发布方式。

### 5. 构建 GitHub Release

`Release` 工作流再次验证：

- 标签符合严格格式；
- 标签版本等于 Cargo 版本；
- 兼容性台账已经封板；
- dev 标签提交属于 `develop`，RC/正式标签提交属于 `main`；
- 发布质量检查通过。

随后生成 Linux GNU、Linux musl、macOS Intel、macOS Apple Silicon 和 Windows 制品以及 SHA-256 校验文件。Windows x86_64 制品必须同时包含 `zero.exe`、匹配架构的 `wintun.dll` 和 Wintun 许可文件；工作流从官方固定版本下载分发包并验证固定 SHA-256，缺少 DLL 时发布构建直接失败。

预发布版本创建 GitHub prerelease。正式版本创建 Draft Release，人工检查制品和发布说明后再公开并标记为 latest。

新阶段成功后，工作流进行同基础版本的定向清理：RC prerelease 及其所有平台制品创建成功后，删除全部 `X.Y.Z-dev.*` 以及更早的 `X.Y.Z-rc.*` Release 与标签；正式版 Draft 经人工确认并实际公开后，删除同版本线剩余的全部 dev/RC Release 与标签。清理不会跨基础版本，也不会删除 stable，也不会在构建、发布失败或正式版仍为 Draft 时运行。

## 夜间自动预发布

`Nightly Release` 每天北京时间 03:17（UTC 19:17）检查 main 和 develop。
GitHub 定时任务可能延迟执行；它只从仓库默认分支读取工作流，因此这些发布治理文件必须合并到默认分支才能启用。
也可以手动运行，选择 `both`、`main` 或 `develop`；手动运行同样跳过无更新且制品完整的版本。
仅有 Git SSH 权限时，可将已部署的默认分支提交普通推送到 `codex/nightly-release-validation`，显式触发一次双分支验证。
这个入口仍从默认分支读取控制脚本，分别从 main／develop 读取发布源码，不把验证分支作为制品来源；完整 CI、标签和发布门禁保持一致。
例如默认分支为 develop 时：`git push github develop:refs/heads/codex/nightly-release-validation`。后续推送必须 fast-forward；没有新提交时 Git 不产生新的 push 事件。

处理规则如下：

1. 分别读取分支 Cargo 版本，寻找同基础版本、同阶段、当前提交祖先中的最新标签；例如 main 的 `v0.0.2-rc.*` 与 develop 的 `v0.0.3-dev.*` 分别计算。
   这些版本号仅为示例，工作流没有固定基础版本。每次执行都会重新读取分支状态：main 发布 `v0.0.2` 后暂时跳过；当人工流程建立 `v0.0.3-rc.*` 发布线后，夜间任务自动继续 `v0.0.3-rc.<UTC时间戳>`。develop 进入新的 patch、minor 或 major dev 发布线时也自动跟进，无需修改工作流。
2. 分支 HEAD 已发布且五个平台归档及 SHA-256 文件完整时跳过；没有新提交也不生成新的时间戳版本。
3. HEAD 对应标签已创建但制品缺失时，复用该不可变标签重试；同标签已有构建正在运行时跳过，避免重复构建。API 错误不会被当作“没有发布”。
4. 有新提交时，复用 `scripts/release.sh --next`、`--start-development`／`--seal-only` 更新 Cargo 和兼容性台账，写入 `release/promotion-source`，生成 `release: nightly v<version>` 提交，普通 fast-forward 推送到对应权威分支。
5. 显式调用该分支的完整 `CI` 手动运行并等待精确 SHA 成功。普通 push 的裁剪 CI 不替代夜间完整门禁。CI 失败时不创建标签，下次复用未发布的夜间版本提交重试，避免只因自动版本提交而不断制造新版本。
6. 标签创建前再次验证 Cargo／台账、提交归属和不可变标签。分支在准备推送时前进会产生普通 push 冲突；分支在 CI 启动前前进则停止本轮，下一轮重新检查。若 CI 已验证的候选仍是分支祖先，可以发布该固定提交，不以新的浮动 HEAD 替换它。
7. 显式调用既有 `Release` workflow，继续使用相同平台、feature、SHA-256 文件和发布说明范围。调度成功只代表构建已启动，必须查看 `Release` 的最终结果才算发版成功。Release 按标签串行执行，失败重试不移动标签。

自动任务只延续已建立的 dev／RC 发布线。首个 dev／RC 标签、新基础版本、阶段晋级及 stable 都通过人工发布流程准备；main 进入 stable 后夜间任务跳过它，不自动发布正式版或开始新的版本线。无对应祖先标签、方向错误或版本回退会明确失败。

工作流使用仓库默认令牌的 `contents: write` 和 `actions: write`，不需要额外 PAT。
仓库必须允许 Actions 写入权威分支；分支保护或 ruleset 拒绝机器人推送时任务会失败，不使用 force push、不自动修改保护规则，也不绕过审批。
启用前需要确认这个工程治理权限与项目分支规则一致；若权威分支必须通过 PR 更新，应继续使用人工 Release PR，而不能把定时任务的失败当成已启用。

部署只同步发布脚本、工作流、测试和发布治理文档到 main／develop，不能将 develop 的协议或运行时开发变更整体合并到 main。
本任务不改变本地 GitLab 镜像；自动发布直接针对 GitHub 仓库。

## 兼容性台账规则

- `Unreleased` 始终保留一个矩阵行和一个章节；
- `dev.YYYYMMDDHHMM` 版本不进入已发布兼容性台账；
- `rc.YYYYMMDDHHMM` 和正式版本在封板时生成对应矩阵行与章节；
- 没有兼容性变化的版本仍要明确记录 `No compatibility changes`；
- 同一版本不得重复出现；
- 台账不能被用来证明一个从未创建标签的版本已经公开发布。

## 发布失败处理

### Release PR 失败

修复 Release PR 分支并重新运行检查。失败的 PR 不创建标签，因此不会污染发布历史。

### 标签发布前质量检查失败

修复代码并重新走 Release PR。不得绕过检查手工创建标签。

### 标签已经创建但制品失败

标签不得移动或覆盖。应修复构建流程，并在确认源提交没有变化的前提下，通过 `Release` workflow 的 `tag` 输入重新构建已有标签；如果源代码必须变化，则创建下一个合法版本。只有成功晋级后的定向清理可以删除上一阶段标签。

### GitHub Release 为 Draft

Draft 可以补充说明或重新上传同一标签对应的制品，但不能改变标签指向。正式发布前必须确认所有平台制品及校验文件完整。

## 紧急修复

紧急修复仍遵守状态机：

1. 从当前 `main` 创建下一个 patch 基础版本并同步到 `develop`；
2. 先发布 dev，再发布时间戳 RC；
3. 验证后发布正式 patch；
4. 将修复同步到后续开发线。

紧急程度不是跳过版本审计、RC 或质量门禁的理由。

## 修改本流程

修改版本格式、状态转换、分支来源、标签时机或制品分类时，必须在同一个 PR 中同步检查：

- `scripts/release.sh`；
- `scripts/release.ps1`；
- `scripts/test-release-policy.sh`；
- `scripts/nightly-release.py` 和 `scripts/tests/test-nightly-release.py`；
- `.github/scripts/nightly-release.cjs` 和对应测试；
- `.github/workflows/nightly-release.yml`；
- `.github/workflows/version-contract.yml`；
- `.github/workflows/prepare-release.yml`；
- `.github/workflows/publish-release.yml`；
- `.github/workflows/release.yml`；
- `.github/workflows/cleanup-prereleases.yml`；
- `docs/project/release-process.md`；
- `docs/project/tooling.md`。

发布机制属于 Core 仓库内部工程规范，除非同时改变了公开契约，否则不要求更新外部文档仓库。

## 2026-09-08 一次性发布历史重置

项目所有者明确授权清理全部旧 Release/tag，并将 Core、ZNet Sink、ZBoard 统一从 `v0.0.1` 正式版重新编号。本次从 `main@c3ae49a5d04206e560bbc69b306167cb21eebd2b` 加入发布前的跨平台 TUN 修复；版本与兼容性台账同步重置。只有本次允许回退版本编号和不保留旧 RC tag，后续继续使用上文的版本晋级策略。

本次在发布提交的主 CI、跨平台 TUN 检查通过后创建 annotated tag，既有 Release 工作流继续执行全 feature 测试、Clippy、多平台构建与校验文件生成。正式 Draft 仍在核验产物后公开。

## 发布说明比较范围

发布说明由 `scripts/release-notes.py` 生成，不按全仓库标签排序选择前一版。

- 后续 RC 只与同基础版本、当前提交祖先中的上一 RC 比较。
- 后续 dev 只与同基础版本、当前提交祖先中的上一 dev 比较。
- 正式版与祖先中的上一正式版比较，包含本轮 dev/RC 累积交付的内容。
- 首个 dev/RC 回退到较低基础版本的祖先正式版；不存在时明确标记初始发布。
- 排除其他版本线、其他阶段、未来标签及非祖先标签；输出实际比较范围。

例如 `v0.0.2-rc.*` 不会使用 `v0.0.3-dev.*` 作为基线。发布范围由所选提交的 Git 差异决定，WireGuard 与端点扩展不会因跨版本标签排序进入 v0.0.2 的发布说明。

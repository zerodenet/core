# 版本演化与发布流程

本文定义 Zero Core 仓库的内部版本演化、发布审批、标签创建和制品生成规则。

该流程属于 Core 项目的工程治理规范，不是对外 API、配置或产品使用文档，因此不需要同步到 `zerodenet/docs` 仓库。对外文档只在公开接口、配置、协议能力或用户可见行为发生变化时更新。

## 基本原则

1. `main` 是当前唯一权威发布分支。
2. 标签是所有版本与质量检查通过后的结果，不是发布检查的起点。
3. 版本号由发布工具根据当前版本和目标阶段计算，日常发布不手工拼写版本号。
4. 版本只能向前演进，不能回退基础版本、阶段或阶段编号。
5. `Cargo.toml`、兼容性台账、Git 标签和 GitHub Release 必须表示同一个版本。
6. 已推送的公开标签不可移动、覆盖或复用。
7. 正式版本必须经过同一基础版本的 RC 阶段。

## 当前分支模型

当前 `develop` 落后于 `main`，因此在完成同步前：

- Release PR 必须以 `main` 为基线并合并回 `main`；
- `develop` 不得作为标签或发布制品的来源；
- 发布标签必须指向 `main` 已包含的提交；
- `develop` 后续同步时应以 `main` 为事实来源，不能反向覆盖已经发布的版本状态。

未来如果重新启用稳定的双分支模型，必须先修改本规范、发布脚本和工作流，并在同一个 PR 中完成验证。

## 版本格式

只允许以下形式：

```text
X.Y.Z-dev.N
X.Y.Z-alpha.N
X.Y.Z-beta.N
X.Y.Z-rc.N
X.Y.Z
```

其中：

- `X`、`Y`、`Z` 为没有前导零的非负整数；
- `N` 为从 `1` 开始、没有前导零的正整数；
- 正式版本没有后缀；
- 标签始终在版本前增加 `v`，例如 `v0.0.16-rc.1`。

以下形式无效：

```text
0.0.16-dev
0.0.16-rc
0.0.16-bate.1
0.0.16-preview.1
0.0.16-rc.01
00.0.16
```

历史上已经存在的非标准标签只作为历史事实保留，不得作为新版本格式继续使用。

## 版本状态机

阶段顺序固定为：

```text
dev < alpha < beta < rc < stable
```

允许在不回退的前提下跳过中间预发布阶段，例如：

```text
0.0.16-dev.3 -> 0.0.16-rc.1
```

### 同一阶段

同一阶段的编号默认必须连续：

```text
0.0.16-rc.1 -> 0.0.16-rc.2
```

以下变化会被拒绝：

```text
0.0.16-rc.1 -> 0.0.16-rc.3
0.0.16-dev.2 -> 0.0.16-dev.1
```

`--allow-gap` 只用于管理员处理明确的历史缺口，不得用于普通发布。

### 切换阶段

进入新阶段时编号必须从 `.1` 开始：

```text
0.0.16-dev.5 -> 0.0.16-beta.1
0.0.16-beta.3 -> 0.0.16-rc.1
```

以下变化会被拒绝：

```text
0.0.16-dev.5 -> 0.0.16-rc.6
0.0.16-rc.2 -> 0.0.16-beta.3
```

### 正式版本

正式版本只能从同一基础版本的 RC 演进：

```text
0.0.16-rc.2 -> 0.0.16
```

不能从 `dev`、`alpha` 或 `beta` 直接发布正式版本，也不能在正式版本发布后继续创建同一基础版本的预发布标签。

### 新开发周期

正式版本之后，新基础版本必须增大，并从预发布阶段的 `.1` 开始：

```text
0.0.15 -> 0.0.16-dev.1
0.0.15 -> 0.1.0-alpha.1
0.0.15 -> 1.0.0-rc.1
```

新基础版本不能直接发布为正式版本。

## 权威文件

发布流程涉及三个不同职责：

| 文件 | 职责 |
|---|---|
| `Cargo.toml` | 当前构建身份 |
| `release/breaking-changes.md` | 已封板版本的兼容性与迁移记录 |
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

检查两个 Git ref 的版本变化：

```bash
./scripts/release.sh --check-transition origin/main HEAD
```

验证标签、Cargo 版本、兼容性台账和 `main` 归属：

```bash
./scripts/release.sh --verify-tag v0.0.16-rc.1
```

只预览封板差异：

```bash
./scripts/release.sh 0.0.16-rc.1 --seal-only --dry-run
```

Windows 使用 `scripts/release.ps1`。PowerShell 文件只负责参数转发，实际规则统一由 `scripts/release.sh` 执行，避免两套状态机产生差异。

## 标准发布流程

### 1. 准备 Release PR

在 GitHub Actions 中运行 `Prepare Release`：

1. 选择目标阶段：`dev`、`alpha`、`beta`、`rc` 或 `stable`；
2. 只有开启新基础版本时，`bump` 才决定使用 `patch`、`minor` 或 `major`；
3. 工作流从当前 `main` 读取版本并计算唯一的下一版本；
4. 工作流同步修改 `Cargo.toml` 和兼容性台账；
5. 工作流创建 `release/v<version>` 分支；
6. 工作流创建目标为 `main` 的 Draft PR。

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

### 3. 合并到 main

Release PR 通过检查后合并到 `main`。

合并只表示版本契约已经准备完成，不表示标签和制品已经发布。

### 4. 创建标签

在 GitHub Actions 中运行 `Publish Release Tag` 并明确确认：

1. 工作流重新从 `main` 读取版本；
2. 验证版本格式、历史演进、Cargo 与台账一致性；
3. 验证远端不存在同名标签；
4. 重新运行格式、Clippy 和全 feature 测试；
5. 所有检查通过后创建 annotated tag；
6. 标签推送触发 `Release` 工作流。

标签不能从本地开发分支直接推送作为标准发布方式。

### 5. 构建 GitHub Release

`Release` 工作流再次验证：

- 标签符合严格格式；
- 标签版本等于 Cargo 版本；
- 兼容性台账已经封板；
- 标签提交属于 `main`；
- 发布质量检查通过。

随后生成 Linux GNU、Linux musl、macOS Intel、macOS Apple Silicon 和 Windows 制品以及 SHA-256 校验文件。

预发布版本创建 GitHub prerelease。正式版本创建 Draft Release，人工检查制品和发布说明后再公开并标记为 latest。

## 兼容性台账规则

- `Unreleased` 始终保留一个矩阵行和一个章节；
- `dev.N` 版本不进入已发布兼容性台账；
- `alpha.N`、`beta.N`、`rc.N` 和正式版本在封板时生成对应矩阵行与章节；
- 没有兼容性变化的版本仍要明确记录 `No compatibility changes`；
- 同一版本不得重复出现；
- 台账不能被用来证明一个从未创建标签的版本已经公开发布。

## 发布失败处理

### Release PR 失败

修复 Release PR 分支并重新运行检查。失败的 PR 不创建标签，因此不会污染发布历史。

### 标签发布前质量检查失败

修复代码并重新走 Release PR。不得绕过检查手工创建标签。

### 标签已经创建但制品失败

公开标签不得移动或覆盖。应修复构建流程，并在确认源提交没有变化的前提下重新运行对应 workflow；如果源代码必须变化，则创建下一个合法版本。

### GitHub Release 为 Draft

Draft 可以补充说明或重新上传同一标签对应的制品，但不能改变标签指向。正式发布前必须确认所有平台制品及校验文件完整。

## 紧急修复

紧急修复仍遵守状态机：

1. 从当前 `main` 开启下一个 patch 基础版本；
2. 至少发布一个 `rc.1`；
3. 验证后发布正式 patch；
4. 将修复同步到后续开发线。

紧急程度不是跳过版本审计、RC 或质量门禁的理由。

## 修改本流程

修改版本格式、状态转换、分支来源、标签时机或制品分类时，必须在同一个 PR 中同步检查：

- `scripts/release.sh`；
- `scripts/release.ps1`；
- `scripts/test-release-policy.sh`；
- `.github/workflows/version-contract.yml`；
- `.github/workflows/prepare-release.yml`；
- `.github/workflows/publish-release.yml`；
- `.github/workflows/release.yml`；
- `docs/project/release-process.md`；
- `docs/project/tooling.md`。

发布机制属于 Core 仓库内部工程规范，除非同时改变了公开契约，否则不要求更新外部文档仓库。

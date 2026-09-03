# 工程规则

本文档记录当前 workspace 布局、构建入口、feature 策略和文档维护规则。这里只描述当前事实，不记录版本演进历史。

## 命名

- package 名称使用 `zero-*`。
- 外部字段名、协议名、状态值、错误码、事件名和能力码使用 `snake_case`。
- 目录名保持简短，例如 `crates/engine`、`crates/proxy` 和 `protocols/socks5`。
- 根二进制入口固定为 `src/main.rs`。
- Rust 模块和函数使用 `snake_case`，类型使用 `CamelCase`。

## Workspace 命令

默认运行 workspace 级命令：

```powershell
cargo fmt --all
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
cargo build --release
```

启动代理和查询状态：

```powershell
cargo run -- run <config>
cargo run -- status --json <config>
```

运行单个测试：

```powershell
cargo test <test_name>
```

修改协议行为、配置解析、路由、运行时接线或日志后，应运行完整测试集。

公开文档站由 `zerodenet/docs` 仓库独立构建和部署。本仓库只维护工程资料；公开文档变更应提交到该仓库并运行其 `pnpm check:build`。

## CI 分层

`CI` 与 `Privileged TUN E2E` 使用 `.github/actions/ci-scope` 读取完整 Git 差异，
在 job 层决定执行范围，而不是用 workflow 路径过滤留下 Pending 检查。
PR 比较 merge base 到 head，push 比较 before 到 after；历史缺失、首次推送或未知事件
保守执行全量检查。范围规则可通过 `node --test .github/actions/ci-scope/scope.test.mjs` 验证。

| 场景 | 验证范围 |
|---|---|
| 仅 Markdown、`docs/` 或 LICENSE 变化 | 运行轻量范围自测与汇总，不启动 Rust 构建 |
| 普通代码或测试变化 | fmt、全目标全 feature Clippy、全 workspace 测试、三平台原生后端 check、独立 feature 组合检查 |
| 构建依赖、原生平台、协议或 transport 变化 | 额外构建 musl 静态制品、编译较旧平台上的 TUN E2E harness |
| `src/`、`crates/`、`protocols/`、`proto/`、TUN 测试或构建设施变化 | 运行 Linux、macOS、Windows 特权 TUN 回归；不限于直接修改 `tun/` 的变更 |
| 目标为 `main` 的 PR、`main` push、手动运行 | 不按文件过滤，执行完整候选验证 |

`examples/` 和 `proto/` 可能被编译或测试消费，不属于纯文档豁免。构建设施包括任意
`Cargo.toml` / `Cargo.lock` / `build.rs`、工具链配置、`.cargo/`、`.github/` 和 `scripts/`。
兼容性检查另外覆盖 `src/`、`protocols/`、`crates/{platform,tun,transport,ztls}/`。
日常三平台原生后端检查使用 Ubuntu 22.04、macOS 14、Windows 2022；完整 TUN 运行使用
Ubuntu 24.04、macOS 15 Intel、Windows 2025。两组系统版本的兼容性覆盖不视为完全重复。

全 workspace 测试已经包含 `zero-proxy` 的 `runtime_boundary`，不单独重复编译运行；
Clippy 已执行全目标类型检查，不再叠加同范围的 `cargo check --workspace --all-features`。
最小 feature 组合检查仍保留，以发现 all-features 下被掩盖的条件编译错误。

macOS 特权测试由普通用户运行 Cargo 编译，通过
`CARGO_TARGET_X86_64_APPLE_DARWIN_RUNNER=sudo` 仅提升测试可执行程序的权限。
不得恢复 `sudo cargo test`，以免产生 root 所有的构建缓存，导致缓存上传失败。
三平台 TUN 任务在测试失败时也保存依赖缓存，但任务本身仍返回失败，不将缓存策略当作重试或豁免。

汇总检查名为 `CI result` 和 `TUN E2E result`，选中的任务失败或取消时汇总失败；
无关改动正常返回跳过结果。配置分支保护时可要求这两个汇总检查，不要要求可能不触发的
路径过滤工作流。工作流修改本身不会自动修改仓库分支保护设置。

封板时在候选 ref 上手动运行 `CI`，并运行 `Privileged TUN E2E` 的 `hosted-all`。
单平台手动入口只用于定向验证，不代表完整候选通过。本轮分层不新增夜间定时任务，
也不削弱 TUN 断言、增加失败重试或修改发布流程。

## 版本与发布

完整的版本格式、状态转换、分支来源、Release PR、标签创建和失败处理规则见[版本演化与发布流程](./release-process.md)。该流程属于 Core 仓库内部工程规范，不需要同步到外部文档仓库。

`develop` 是 `-dev.YYYYMMDDHHMM` 的构建与发布来源，`main` 是 RC 和正式版来源。dev 使用 UTC 分钟时间戳，RC 的阶段编号由脚本自动递增，正式版自动识别 `main` 当前 RC。RC 也可从明确的 dev/前序 RC 标签晋级，但不能直接使用浮动的 `develop` HEAD。

发布规则统一实现在 `scripts/release.sh`。`scripts/release.ps1` 只负责将 Windows 参数转发到 Git for Windows Bash，避免维护两套不同的状态机。

在 `develop` 上，本地兼容入口接受基础版本并自动补全 UTC 分钟时间戳。例如 `./scripts/release.sh 0.0.16` 会解析为 `0.0.16-dev.YYYYMMDDHHMM`；如果同一分钟的 dev 标签或 `v0.0.16` 正式标签已经存在，则拒绝创建。

本地发布默认把分支和标签原子推送到所有 Git 远端。使用 `--remote <name>` 可只同步一个远端，使用 `--no-push` 可完全跳过推送。PowerShell 入口采用相同默认值和覆盖规则。

检查当前版本契约：

```bash
./scripts/release.sh --check
```

计算下一版本：

```bash
./scripts/release.sh --next dev --bump patch
./scripts/release.sh --next rc
./scripts/release.sh --next stable
```

检查 PR 的版本变化：

```bash
./scripts/release.sh --check-transition origin/main HEAD
```

验证标签：

```bash
./scripts/release.sh --verify-tag v0.0.16-rc.1
```

预览候选版本封板：

```bash
./scripts/release.sh 0.0.16-rc.1 --seal-only --dry-run
```

日常发布不应直接使用本地脚本提交和推送标签。标准入口是：

1. GitHub Actions `Prepare Release` 根据所选阶段自动计算完整版本号；dev 创建目标为 `develop` 的 Draft PR，RC/正式版创建目标为 `main` 的 PR，通常不填写 `source_tag`；
2. PR 通过版本契约和仓库质量检查后合并；
3. GitHub Actions `Publish Release Tag` 从阶段对应的权威分支重新验证来源并创建标签；
4. 标签触发 `Release` workflow 构建制品和 GitHub Release；RC 成功后清理同版本 dev，正式版 Draft 实际公开后清理同版本 RC。

命令语义：

| 命令 | 是否写文件 | 作用 |
|---|---:|---|
| `--check` | 否 | 检查 Cargo 与兼容性台账当前状态 |
| `--next <stage>` | 否 | 根据当前版本计算唯一的下一版本 |
| `--check-transition <base> <head>` | 否 | 检查两个 Git ref 的版本是否向前演进 |
| `--verify-tag <tag>` | 否 | 检查标签格式、版本、台账、历史和阶段对应的分支归属 |
| `--start-development` | 是 | 开启严格的 `X.Y.Z-dev.YYYYMMDDHHMM` 开发版本 |
| `--seal-only` | 是 | 更新 Cargo 并将 `Unreleased` 封板到候选或正式版本 |
| 普通 release 命令 | 是 | 兼容的本地发布入口；标准流程不使用它直接推送标签 |

版本格式只允许：

```text
X.Y.Z-dev.YYYYMMDDHHMM
X.Y.Z-alpha.N
X.Y.Z-beta.N
X.Y.Z-rc.N
X.Y.Z
```

标准发布阶段为可选的 `dev`、随后 `rc < stable`；底层状态机继续兼容历史 `dev.N` 和 alpha/beta。新 dev 使用严格递增的 UTC 分钟时间戳，RC 等编号阶段默认连续且从 `.1` 开始，正式版本必须由同一基础版本的 RC 演进。

## 根 package 的 feature

根 package `zero` 是对外构建入口，它把协议和控制面 feature 转发到内部 crate。

| Feature | 说明 |
|---|---|
| `default` | 等同于 `full,status-api` |
| `full` | 启用全部协议能力和 `dns` |
| `dns` | DNS 子系统 |
| `socks5` | SOCKS5 入站和出站，包括 TCP CONNECT 与 UDP ASSOCIATE |
| `http` | HTTP CONNECT 入站 |
| `mixed` | 同端口识别 SOCKS5 TCP/UDP 与 HTTP CONNECT TCP；依赖 `socks5` 和 `http` |
| `vless` | VLESS 入站、出站及相关传输 |
| `hysteria2` | Hysteria2 入站和出站 |
| `shadowsocks` | Shadowsocks 入站和出站 |
| `trojan` | Trojan 入站和出站 |
| `vmess` | VMess 入站和出站 |
| `mieru` | Mieru 入站和出站 |
| `status-api` | 运行时控制端点和 selector 切换 |
| `event-dispatcher` | 事件分发基础设施和 sink 投递状态 |
| `sink-jsonl` | JSON Lines 事件 sink；依赖 `event-dispatcher` |
| `connector` | 通用 Webhook 事件投递；仅依赖 `event-dispatcher`，控制入口按需启用 |
| `grpc-api` | gRPC 控制面适配器 |

`zero-proxy` 还有面向内部接线的 transport feature。外部构建者应使用根 package feature，不应依赖内部 crate 当前的 feature 组合。

配置引用未编译的协议时，程序必须在启动早期返回清晰错误。

## 代码边界

- `zero-traits` 和 `zero-core` 不绑定 Tokio。
- 具体协议实现位于 `protocols/*`。
- `zero-config` 拥有配置类型和验证。
- `zero-router` 拥有规则匹配。
- `zero-engine` 拥有决策、计划、状态、分组、会话、统计和事件。
- `direct` 和 `block` 的目标语义位于 `zero-engine`，socket 执行位于 `zero-proxy`。
- 监听生命周期、运行时编排和协议能力接线位于 `zero-proxy`。
- 通用载体位于 `zero-transport`，协议如何使用载体由协议 crate 和适配器决定。
- 根二进制不得实现协议细节。

更完整的所有权和依赖规则见[架构](./architecture.md)。

## 文档边界

- 配置结构变化时，同步更新配置文档、协议配置速查和示例。
- 协议能力变化时，同步更新协议详情、能力矩阵和限制说明。
- 控制面请求、响应或事件变化时，在 `zerodenet/docs` 仓库同步更新公开控制面文档和 GUI 指南。
- 运行时分层变化时，同步更新 `docs/project/`。
- 版本和发布治理变化时，同步更新 `docs/project/release-process.md`，不需要同步外部文档仓库。
- `https://docs.zerodenet.org/projects/core/control-plane/` 描述当前外部契约；`control-plane/` 仅保存历史设计背景，不作为实现依据。
- 文档只描述当前事实，避免使用“从某版本开始”“截至目前”等版本历史措辞。
- Rust 标识符、配置字段、协议名称和标准术语可以保留英文；普通叙述和章节标题统一使用中文。
- 修改本仓库工程文档后检查本地链接；修改公开文档时在 `zerodenet/docs` 仓库运行 `pnpm check:build`。

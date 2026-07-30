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

公开文档站由 `zerodenet/docs` 仓库独立构建和部署。本仓库只维护工程资料；
公开文档变更应提交到该仓库并运行其 `pnpm check:build`。

## 版本契约与封板

开发构建号、待发布兼容性记录和正式发布版本由现有 `scripts/release.ps1` 与 `scripts/release.sh` 管理。不要手工把 `-dev` 版本写进 `breaking-changes.md`，也不要在发布 workflow 中临时覆盖 Cargo 版本。两套脚本提供相同语义，不需要额外安装 Python、Node 或其他版本工具。

开发期间，已实现但尚未封板的语义变化登记在 `Unreleased`：

```powershell
./scripts/release.ps1 -Check
```

Linux/macOS 使用 `./scripts/release.sh --check`。正式版本测试完成后，可先只读预览封板差异：

```powershell
./scripts/release.ps1 -Version 0.0.16 -DryRun
```

实际发布仍通过现有 wrapper 完成。wrapper 会调用版本契约脚本，同时更新 `Cargo.toml` 和兼容性文档，然后提交、打 tag，并按参数决定是否推送：

```powershell
./scripts/release.ps1 -Version 0.0.16 -NoPush
```

```bash
./scripts/release.sh 0.0.16 --no-push
```

发布后开启下一个开发周期时使用：

```powershell
./scripts/release.ps1 -Version 0.0.17-dev -StartDevelopment
```

Linux/macOS 对应命令为 `./scripts/release.sh 0.0.17-dev --start-development`。

命令语义：

| 命令 | 是否写文件 | 作用 |
|------|------------|------|
| `-Check` / `--check` | 否 | 根据 Cargo 当前是 `-dev` 或正式版本，检查文档状态 |
| `-StartDevelopment` / `--start-development` | 是 | 设置开发构建身份，保留新的 `Unreleased` 区域 |
| 普通 release 命令 | 是 | 将 `Unreleased` 封板为正式版本、更新 Cargo、提交并打 tag |
| `-CheckRelease` / `--check-release` | 否 | 校验 Cargo、兼容矩阵和版本章节与 tag 一致 |

独立 CI workflow 会在 Cargo、兼容台账、发布脚本或 workflow 变化时分别执行 Bash 和 PowerShell 契约检查；tag release 还会再次执行 `check-release`。

## 根 package 的 feature

根 package `zero` 是对外构建入口，它把协议和控制面 feature 转发到内部 crate。

| Feature | 说明 |
|---------|------|
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
- `https://docs.zerodenet.org/projects/core/control-plane/` 描述当前外部契约；`control-plane/` 仅保存历史设计背景，不作为实现依据。
- 文档只描述当前事实，避免使用“从某版本开始”“截至目前”等版本历史措辞。
- Rust 标识符、配置字段、协议名称和标准术语可以保留英文；普通叙述和章节标题统一使用中文。
- 修改本仓库工程文档后检查本地链接；修改公开文档时在 `zerodenet/docs` 仓库运行 `pnpm check:build`。

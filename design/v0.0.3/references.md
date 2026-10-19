# 来源与源码索引

本目录已保存全部设计正文，来源 Page 仅作出处，不是后续任务、构建或 CI 的依赖。Core 引用改为仓库相对路径；消费者引用指向下方来源索引，不要求访问原作者机器。

[方案入口](README.md) · [架构](architecture.md) · [契约](contracts.md) · [迁移与任务卡](migration.md) · [兼容](compatibility.md) · [验证](validation.md) · [来源](references.md)

<!-- source-section: 24 -->
# 源码与规范索引

## 上游参考

- [sing-box Box](https://github.com/SagerNet/sing-box/blob/dfb80349bf5779ec0a3f834c9ec2794345bff8f0/box.go)

- [sing-box Inbound](https://github.com/SagerNet/sing-box/blob/dfb80349bf5779ec0a3f834c9ec2794345bff8f0/adapter/inbound.go)

- [sing-box Outbound](https://github.com/SagerNet/sing-box/blob/dfb80349bf5779ec0a3f834c9ec2794345bff8f0/adapter/outbound.go)

- [sing-box Endpoint](https://github.com/SagerNet/sing-box/blob/dfb80349bf5779ec0a3f834c9ec2794345bff8f0/adapter/endpoint.go)

- [sing-box Registry](https://github.com/SagerNet/sing-box/blob/dfb80349bf5779ec0a3f834c9ec2794345bff8f0/adapter/outbound/registry.go)

- [sing-box Lifecycle](https://github.com/SagerNet/sing-box/blob/dfb80349bf5779ec0a3f834c9ec2794345bff8f0/adapter/lifecycle.go)

- [Rust dyn compatibility](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility)

- [Tokio JoinSet](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html)

## 当前 Zero 代码

- [PR 和平台 CI](../../.github/workflows/ci.yml)

- [CI 范围选择](../../.github/actions/ci-scope/scope.mjs)

- [全量测试入口](../../scripts/test-workspace.sh)

- [定时开发版流程](../../.github/workflows/nightly-release.yml)

- [特权 TUN 验证](../../.github/workflows/tun-e2e.yml)

- [基础抽象](../../crates/traits/src/lib.rs)

- [入站标准](../../crates/core/src/inbound.rs)

- [配置编译](../../crates/config/src/compile.rs)

- [Engine 计划](../../crates/engine/src/plan/mod.rs)

- [Engine 配置更新](../../crates/engine/src/runtime/configuration.rs)

- [Proxy 注册契约](../../crates/proxy/src/protocol_registry/capability.rs)

- [Proxy 事务入口](../../crates/proxy/src/runtime/handle/configuration.rs)

- [UDP 类型恢复](../../crates/proxy/src/runtime/udp_flow/managed/flow/resume.rs)

- [应用服务](../../src/application/services.rs)

- [FFI](../../crates/ffi/src/lib.rs)

- [兼容性账本](../../release/breaking-changes.md)

## 当前消费者

- [GUI IPC 命令](references.md#consumer-gui)

- [GUI 事件解析](references.md#consumer-gui)

- [GUI 配置兼容测试](references.md#consumer-gui)

- [GUI 真实内核版本矩阵入口](references.md#consumer-gui)

- [GUI Rust 依赖](references.md#consumer-gui)

- [ZBoard 配置 renderer](references.md#consumer-panel)

- [ZBoard 配置发布](references.md#consumer-panel)

- [ZBoard 发布脚本](references.md#consumer-panel)

- [ZBoard 能力解析](references.md#consumer-panel)

- [ZBoard 事件接收](references.md#consumer-panel)
<!-- /source-section: 24 -->

# 来源快照与转换规则

- 源题名：Zero v0.0.3 架构设计与云端 PR 实施方案。
- 出处：[源 Page revision 7](https://chatgpt.com/space/page_b20205c79b6c8191b93cc373fd831fe4)，读取时 current_sequence/projection_through_sequence 均为 7，406 块、66,787 字符。
- 原始全文及各保存分节的 SHA-256 见 [provenance.json](provenance.json)。25 个分节包含设计开篇及全部 24 个一级章节；所有正文、表格、签名和任务提示词均保存，不以摘要替代。
- 只转换 Markdown 链接目标：Core 本机源路径转换为仓库相对文件及行号锚点；GUI/面板路径转换到本文件的消费者来源索引。正文历史陈述保持原样，实际 PR00 基线与执行状态由 README 和 validation 补充。
- sing-box/Xray 的固定观察提交保持原样，不能替代各协议固定的 immutable upstream。在线规范链接作为参考，本次不宣称逐页复核外部规范或版本。
- 后续设计变更须显式评审并更新来源/摘要记录，不把目标签名改写为已验证实现。

<a id="consumer-gui"></a>

## GUI 来源索引

历史观察仓库为 [zerodenet/znet-sink](https://github.com/zerodenet/znet-sink)，观察 HEAD 为 `fd198ba52711f018334860b8a6704546ccd349fd`，但当时工作区存在未提交修改。以下路径与行号是源方案的观察位置，未声称该提交完整包含原工作区内容。本次未读取用户机器、未上传未提交内容，PR01 需重新冻结云端真正可取得的提交、fixtures 和 captures。

| 仓库内路径 | 源方案行号（如有） | 观察用途 |
| --- | --- | --- |
| `src-tauri/src/kernel/zero/commands.rs` | 38 | IPC 命令等待 |
| `src-tauri/src/kernel/zero/events.rs` | 20 | 事件解析与规范化 |
| `src-tauri/src/kernel/zero/config_compatibility_tests.rs` | — | 配置兼容 |
| `src-tauri/src/kernel/zero/compatibility_tests.rs` | — | 真实内核版本矩阵及外部 captures |
| `src-tauri/Cargo.toml` | — | Rust 依赖与 zero-rule |

<a id="consumer-panel"></a>

## 面板来源索引

历史观察仓库为 [zerodenet/zboard](https://github.com/zerodenet/zboard)，观察 HEAD 为 `f19994768204485c479f72b2832a11771e872502`。以下路径是源方案来源说明，本次未运行消费者测试或验证该仓库访问权限；PR01 需重新冻结实际可检出的基线。

| 仓库内路径 | 源方案行号（如有） | 观察用途 |
| --- | --- | --- |
| `backend/internal/adapters/zero/runtime_configuration.go` | — | renderer |
| `backend/internal/adapters/zero/configuration_publisher.go` | 135 | 发布与不确定结果判定 |
| `backend/internal/adapters/zero/configuration_script.go` | 69 | 发布脚本与阶段 |
| `backend/internal/adapters/zero/native_capabilities.go` | — | 能力发现 |
| `backend/internal/handler/zero_events.go` | — | 事件接收 |

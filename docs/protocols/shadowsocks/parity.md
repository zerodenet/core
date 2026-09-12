# Shadowsocks 1.21.2 实现对照

固定参考：`shadowsocks-rust 1.21.2`，提交
`a03006a753486e64717d6e3afa91e0c6d043c557`；该版本锁定的密码库为
`shadowsocks-crypto 0.5.5`。范围是 SS 协议及线路完整实现，管理使用 Zero 原生接口。
生产验证是独立的最后步骤，不计入实现完成度。

## 实现及证据

以下路径以仓库根目录为基准。状态表示代码实现，最终门禁另列。

| 契约 | 实现归属与验证 | 状态 |
| --- | --- | --- |
| 全部 v1 方法、别名、大小写、plain、流密码及可选 AEAD | `validation/legacy.rs`、`shared/{legacy,extra_aead}.rs`；`reference_ciphers` 对照固定密码库，`reference_interop` 双向 TCP/UDP | 已实现 |
| 四种 AEAD 2022 | `shared/{keys,headers,chacha8}.rs`、`udp/wire/`；三种官方发布方法互通，ChaCha8 对照官方可选后端 `reference_2022` | 已实现 |
| TCP 握手与响应绑定 | `inbound/{request,identity}.rs`、`stream/read.rs`；SIP022、SIP023 双向互通、响应 request salt 绑定 | 已实现 |
| 可变头续读、v1 地址跨 AEAD chunk | `shared/target.rs`、`inbound/`；`tcp_accept_boundaries`、`legacy_replay` | 已实现 |
| 失败握手读取边界 | `inbound.rs`；统一 2 秒总 deadline 与 1 MiB drain 上限，慢输入测试 | 已实现 |
| TCP 局部读写、取消、flush、shutdown、EOF、块长与 nonce | `stream/{read,write}.rs`、`shared/tcp.rs`；逐字节 Pending I/O 和取消恢复，v1 16383、2022 65535 字节 | 已实现 |
| v1 重放策略、生成 nonce 的反射防护 | `shared/legacy_replay.rs`；上下行四策略、认证失败不污染状态、已生成 salt 反射拒绝 | 已实现 |
| 2022 salt 精确重放与回收 | `shared/replay.rs`；61 秒、无隐藏默认容量、可选准入上限、空闲维护；重复服务端 salt 换请求仍拒绝 | 已实现 |
| UDP 用户与 endpoint/session 隔离 | `udp/inbound/association.rs`；`shadowsocks_isolation`、`udp_state/identity.rs` 跨用户同 wire ID | 已实现 |
| 多目标 socket 复用与 endpoint 迁移 | 协议响应 binding + runtime 中立关联 ID；真实代理多目标回包和迁移测试 | 已实现 |
| 单用户取消不关闭共享 listener | `zero_core::DatagramUdpResponder` 中立契约 + runtime relay 生命周期；真实代理撤销测试 | 已实现 |
| sender ID、packet ID、方向、回显、重放窗口 | `udp/{client,session}.rs`、`shared/window.rs`；连续 2050 包、8128 窗口与精确集合差分、终止 ID 拒绝 | 已实现 |
| UDP 容量、空闲回收、任务与 socket 回收 | 默认 300 秒、无隐藏容量；显式准入限制、持久维护、flow drop 取消、错误 peer 丢弃和大包测试 | 已实现 |
| 空 UDP 包及 padding | v1 全方法差分、2022 ChaCha8 字节对照、AES EIH 空响应、原生 datagram 路径 | 已实现 |
| SIP023 EIH 与用户热更新 | `inbound/{model,profile,store,identity}.rs`；O(1) 身份索引、双向官方互通、原子替换及旧凭据状态清理 | 已实现 |
| SIP003/SIP003u | `transport/plugin/`，中立 `InboundCarrierPlan`；三种模式真实代理收发、环境/obfsproxy 参数、启动失败、退出、重载、进程回收 | 已实现 |
| 原生配置和分层 | 私有凭据/方法/插件校验归 `validation/`；config 仅映射错误和原生 principal 规则；最小 feature 检查 | 已实现 |
| 能力元数据 | `supported`，移除将外部验证当作实现缺失的 limitation；导出测试同步 | 已实现 |

所有具体协议代码位于 `protocols/shadowsocks/src/`。代理 runtime 不读取
密码、cipher 字符串或 wire session ID。插件进程、加密状态和不透明 cache key
由协议构造。共享 accept loop、路由、socket 与统计继续由 runtime 执行。

## 基线互操作

`reference_interop` 使用官方发布程序支持的完整方法/别名集合，逐项执行
Zero → 官方、官方 → Zero，分别检查 TCP 和 UDP。
官方发布二进制没有编译 extra AEAD 和 ChaCha8；这些方法与该提交锁定的
0.5.5 密码库进行字节对照，不换用其他版本冒充固定参考。
`external_sip023` 单独覆盖两种 AES EIH 的 TCP/UDP 双向线路和连续 UDP 会话。

`prepare-ss-interop.py` 校验官方归档 checksum，测试验证精确 1.21.2 版本；
CI 使用同一准备工具与矩阵。附加版本验证不能改变上述基线。

## 原生宿主边界

[configuration.md](configuration.md) 记录管理、DNS、路由、用户策略、状态
限制及插件配置映射。保留 Zero 空用户注册表的禁用语义；有效的空 v1 密码
可通过显式 user 表达。原生 socket 策略、平台开关、官方 manager/订阅工具
接口与部署方式不是 SS wire 协议；不另建控制面。

外部插件不能包装已经存在的 Zero relay stream，也不能作为嵌套 UDP
packet-path codec。官方没有这些 Zero 额外组合能力；配置插件覆盖的网络类别
不能静默绕过插件。显式原生容量采用拒绝新状态策略，保证活跃重放条目不被驱逐。

## 最终门禁

2026-09-12，当前工作区实现与本地开发门禁已完成：

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all`、`git diff --check` | 通过 |
| SS `validation`、`runtime` 最小 feature 构建 | 通过，无警告 |
| `cargo check --workspace` | 通过，无警告 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 通过 |
| `RUST_MIN_STACK=16777216 cargo test --workspace --all-features` | **1717 通过、0 失败、114 忽略** |
| 上述工作区中的项目结构/分层约束 | **185 项通过** |
| 固定官方 `reference_interop` + `external_sip023` | **7 项通过**；48 个方法名称/别名分别完成双向 TCP 与 UDP，另含 5 项 SIP023/连续 UDP 测试 |
| 完整 Zero 内核 → 官方 ssserver UDP | **1 项通过**，覆盖三种常规 AEAD 和三种标准 2022 方法 |

114 项为工作区按原 `ignore` 标记跳过的测试，不能算作通过；上述 SS 官方
基线的 8 项忽略测试已另外显式执行并通过。其他外部程序/特权环境用例未由本次
SS 开发结果代替。最后的插件空选项缓存身份修复包含在 1717 项完整重跑中。

```sh
cargo fmt --all
cargo check -p shadowsocks --no-default-features --features validation
cargo check -p shadowsocks --no-default-features --features runtime
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUST_MIN_STACK=16777216 cargo test --workspace --all-features
SS_RUST_BIN_DIR=/tmp/ss-reference RUST_MIN_STACK=16777216 cargo test -p shadowsocks --all-features --test reference_interop --test external_sip023 -- --ignored --nocapture
SHADOWSOCKS_RUST_BIN=/tmp/ss-reference/ssserver RUST_MIN_STACK=16777216 cargo test --workspace --all-features --test shadowsocks_xray_interop zero_ss_outbound_interops_with_ssrust_inbound_udp_all_ciphers -- --ignored --nocapture
```

这里的 `/tmp/ss-reference` 由准备脚本校验并生成。实际本地验证使用已校验的
`/tmp/ss-reference-1.21.2`。本地日志分别为 `/tmp/ss-final2-workspace-tests.log`、
`/tmp/ss-final-official.log` 和 `/tmp/ss-final-kernel-official.log`。

尚未推送、部署或执行生产资格验证。生产验证保留为最后的运行步骤，
不从已完成的协议实现中扣减。历史审计文档保留当时发现，以本矩阵作为当前清单。

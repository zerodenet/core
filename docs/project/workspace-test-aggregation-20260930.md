# 工作区 Integration 测试聚合验收（2026-09-30）

## 范围与目标

以 `c9eb8a75` 为布局基准，将相关 integration 用例合并到领域 suite，降低
重复链接、可执行文件启动及加载成本。264 个 integration 目标降至 88 个
（减少 66.7%）；264 个原根用例文件均有且只有一个入口，其中 217 个通过
聚合 suite 引入，其余保留原执行边界。不修改产品实现、依赖、feature 图或
测试断言。旧缓存未删除，这一布局优化不等于已经回收磁盘空间。

## 包级变化

| 包 | 原目标 | 新目标 |
| --- | ---: | ---: |
| `zero-api` | 9 | 2 |
| `zero-config` | 22 | 3 |
| `zero-engine` | 17 | 4 |
| `zero-proxy` | 72 | 31 |
| `zero-transport` | 22 | 6 |
| `zero-platform-tokio` | 5 | 2 |
| `ztls` | 4 | 1 |
| `hysteria2` | 6 | 1 |
| `shadowsocks` | 11 | 4 |
| `vless` | 22 | 2 |
| `mieru` | 9 | 2 |
| `vmess` | 2 | 1 |
| `trojan` | 2 | 1 |
| `wireguard` | 3 | 2 |
| `zero-rule` | 4 | 1 |
| `zero-connector` | 4 | 3 |
| `zero-dns` | 11 | 3 |
| `zero-stack` | 12 | 2 |
| `zero-tun` | 5 | 3 |
| `zero` | 16 | 8 |

未列出的 workspace 成员布局不变。

## 保留的执行边界

- 专门的外部官方实现互操作、真实浏览器、特权 TUN/ICMP、平台 E2E 目标继续独立运行；混合在普通源文件中的 ignored 用例保留原标记及模块过滤入口。
- Proxy 的全局 tracing subscriber 用例继续独立；大型静态架构检查保留原目标。
- Connector 长期运行/中断资格测试保持原入口。
- WireGuard 协议 runtime 保留原目标，供 CI 精确选择恶意报文隔离用例。

`tests/suites/*.rs` 只声明原文件模块和 feature 门槛。原用例路径、内嵌模块、
`include_str!`/`include_bytes!` 相对路径保留。Proxy 与根二进制用例统一引用
suite 的共享 support，避免每个模块重复初始化 Rustls、分离端口分配器。
逐文件对照基准，217 个聚合源文件仅调整共享 fixture 引用：`mod support;` 改为
`use crate::support;`；Stack Echo 和 Shadowsocks socket helper 也统一引用
suite 中的唯一声明。测试函数与断言保持原样。原 Cargo `required-features`
逐项对照并保留在对应成员模块外层条件中，原文件内部条件不变。

## 入口与防漏

```bash
./scripts/test-workspace.sh
./scripts/test-workspace.sh --test endpoint_contracts --test proxy_control
python3 scripts/check-test-layout.py --json
cargo test -p zero-proxy --all-features --test proxy_control core_capabilities::
```

布局检查检查各包的 `tests/*.rs` 与已注册 `tests/suites/*.rs`，拒绝漏接、
重复入口、未注册 suite 和缺失成员路径；不宣称解析所有嵌套 Rust 模块。
全量入口固定 workspace/all-features/16 MiB 测试栈，输出 UTC 起止时间、
耗时、退出码；focused 明确不算全量验收。CI 与本地调用同一入口，协议
互操作 workflow 的普通回归目标更新到新 suite，外部/特权目标保持原名。

## 验证记录

- 布局检查：264 个源文件、88 个目标、0 错误。
- 布局检查自测：5/5；统一入口自测覆盖 scope、参数、测试栈、计时、Cargo/
  日志/汇总器失败以及 metadata 失败在执行测试前阻断。
- CI 范围与 workflow 契约：20/20。
- 原用例内容与 required-features 对照通过；格式检查通过。
- 工作区全量测试：146 个 harness，2,280 passed、0 failed、155 ignored、
  0 filtered out，与聚合前通过/忽略数量一致。退出码 0。
- 全量开始 `2026-09-30T09:23:52Z`，结束 `2026-09-30T10:03:37Z`，
  总耗时 2,385 秒（39 分 45 秒），编译 4 分 22 秒；各 harness 报告的
  执行时间累计 177.09 秒。其余时间包含启动、加载、Cargo/rustdoc 调度等，
  尚未逐项归因；聚合完成不代表所有测试性能问题已消除。
- 日志：`/Volumes/tool/tmp/zero-workspace-aggregation-tests-20260930.log`；
  汇总：同目录 `zero-workspace-aggregation-results-20260930.json`。运行前后
  源文件哈希一致，证明测试结果对应本轮源文件。
- Clippy `--workspace --all-targets --all-features -- -D warnings` 通过；
  修复聚合时暴露的单条件 `cfg(all(...))` 冗余及重复 helper 模块声明，
  不禁用 lint。最终 Clippy 退出码 0，耗时 191 秒。
- 最终 fixture 接线调整后的四个受影响 suite 定向回归：133/133，通过；
  退出码 0，耗时 106 秒。四个 suite 的用例名称和结果逐项与全量记录一致。
  全量结果来自该接线微调之前，最终结果结合定向回归判定，不将 focused
  冒充第二次全量。纯 helper、测试函数和断言逐文件对照未变。
- 日志：同目录 `zero-workspace-aggregation-clippy-accepted-20260930.log`、
  `zero-workspace-aggregation-final-fixtures-20260930.log`。

此前非聚合全量曾耗时约 126 分钟；缓存、构建状态与系统负载不同，不能
直接据两个运行时间宣称同条件加速比例。保留测试范围与实际运行结果作为
验收依据。外部网络、特权设备、跨平台资格仍以独立任务真实结果为准。

# 第二轮验收与交接

本轮基线已保存本地定向修复和 DNS 增量，已有结果见 [baseline.json](baseline.json)。提交和推送不会赋予未运行资格；原始日志不随源码发布，也不是新 checkout 的运行依赖。

## 每个实现域

交付必须同时具备：实际现有调用者迁移、普通生产装配、真实 owner/Scope/provider、停止与替换屏障、旧执行链删除、必要回归。仅声明、准备接口、私有测试 assembly 或一条协议组合不算整域完成。公开库 convenience API 可以保留真实职责，但不能作为生产的第二执行者或资源缺失退路。

使用真实原 controller 的 matching close，分别确认 logical user/control 归还、原始 provider、driver/Scope joins。显式建立实际 await I/O Pending，再测试取消；PID、延迟、任务 body 完成、abort、Drop、EOF、端口重绑都不能替代原件关闭。清理失败/超时保留实际原件和原错误，不取得新资源、不发明新的临时豁免。

每个 PR 首先完成必要定向，M 排队执行构建；不为每个中间提交重跑全量。已有源码和二进制精确相同时复用结果，有影响的源码变更则补对应回归，不跨输入挪用资格。

## 轻量检查

```bash
git diff --check
cargo +1.99.0 fmt --all -- --check
python3 scripts/check-test-layout.py
python3 scripts/check-runtime-contracts.py
python3 design/v0.0.3/check-plan.py
```

`check-runtime-contracts.py` 调用 Cargo，确保 PATH 的 Cargo 来自 Rust 1.99.0，或以 `rustup run 1.99.0 python3 scripts/check-runtime-contracts.py` 执行。所有工具报告实际版本；不能只用默认 feature 证明独立 feature/no_std。

DNS 基线回归示例，按实际改动选取而不是机械全跑：

```bash
cargo +1.99.0 test --locked -p zero-proxy --no-default-features --features dns,socks5 --lib runtime::dns_outbound::tests -- --test-threads=1
cargo +1.99.0 test --locked -p zero-dns --all-features --lib query_tasks:: -- --test-threads=1
cargo +1.99.0 test --locked -p zero-dns --all-features --lib backends::doh:: -- --test-threads=1
cargo +1.99.0 test --locked -p zero-dns --no-default-features --lib query_tasks:: -- --test-threads=1
```

必要的 16 MiB 测试线程栈使用 `RUST_MIN_STACK=16777216`，不因此提高生产任务栈或放宽既有超时。真实地址夹具保留原绑定 socket 到交接；`ZERO_TEST_HOST_IPV4` 只能来自本机实际分配接口，不能为了测试修改用户网络。基线 HY2 first candidate 的 Linux 127/8 行为与 macOS 实际地址不同，保留显式地址适配和真实拒绝/后备断言。

## 最终候选统一检查

各域实现稳定后，M 冻结准确源码并统一检查，开发期间不在本地反复全量。全量仅由 M 在云端最终候选阶段排队发起；以下是资格标准，不是每个 agent 的开工命令。

- workspace check、严格 workspace/all-target/all-feature Clippy：`cargo +1.99.0 clippy --workspace --all-targets --all-features --no-deps -- -D warnings`。
- 同一特性图的完整 workspace：`RUSTUP_TOOLCHAIN=1.99.0 RUST_MIN_STACK=16777216 bash scripts/test-workspace.sh`；记录各 target、真实结束码、失败和忽略，不把 --no-run 或部分成功当通过。
- 现有 CI 的代表性独立/最小 feature、traits/core no_std、validation-only 及依赖 DAG；协议按各自既定 upstream tag/immutable commit 互通，不升级基线逃避失败。
- 首波/第二波组合后的失败恢复、无变化 reload、网络换代、容量拒绝和最终退出。检查共享原件顺序和是否出现第二宿主/第二池。
- 最终旧链归零、public Rust 迁移账本、版本化工程规则和 feature 一致性。原 25 分节/17 卡设计不改写成已完成，逐 G0–G10 对账。

本轮 `.github/workflows/ci.yml` 已允许 PR base `codex/v003-cloud-r2-integration`；push 仍只对原 develop/main 触发。基线 push/空分支不代表 CI 通过。专门 workflow 若未包含本分支，按实际 workflow_dispatch 能力和精确 ref 运行；不能触发 release/publish/nightly 或将不存在的 check 填为通过。

## PR 交付表

标题围绕最终职责与行为，不写“完成全部重构”。每份 PR 填：

| 项目 | 要求 |
| --- | --- |
| 源码与依赖 | 起点、head 完整 SHA、共享前置/其他 PR、实际输入摘要；base 为 r2-integration |
| 实现与删除 | 真实调用者清单、标准角色/原 owner、普通装配/派发/计量、旧链删除点 |
| 生命周期 | 原始 provider、Scope/driver、用户/控制归还、停旧后启新、失败候选和恢复 |
| 验证 | 精确命令/工具/feature、产物摘要、exit code、通过/失败/忽略/未执行及原始 artifact |
| 兼容 | V1 配置/API/events、CLI、FFI、ACK/revision/结算、客户端面板语义；无消费者改动 |
| 剩余 | 本域实现缺口、环境待验及 TEMP 债务分列；不同源码的历史结果不合并为当前通过 |

原始日志上传 CI artifact 或仓库外；仓库只存简洁索引和必要结论，不提交 gzip、缓存、测试二进制或几百份临时快照。基线已有本地原始日志 SHA 只作来源索引，不要求云端具备用户机器的路径或文件。

## 回本地的资格

云端交付精确候选后，本地再验同一新内核产物与冻结 GUI/面板契约；不修改消费者。真实特权 TUN、Packet/Flow、源地址/MTU/回程、系统 route/DNS 恢复、Windows/macOS 原件、完整适用互通及 1 CPU/1 GiB 性能/恢复/退出按相应环境执行。云端 Linux 通过不覆盖它们，历史消费者结果不覆盖新产物。

TEMP-WG-OUTER-UDP-01 单独保留未关闭，不得借验收记录关闭。最终交接清楚区分架构实现完成、云端资格、本地待验和临时债务；不自动合入 develop/main，不发布或部署。

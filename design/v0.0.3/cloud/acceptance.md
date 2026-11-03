# 云端与本地验收标准

本包沿用 [完整验收方案](../validation.md)，不以新任务卡降低门禁。基线是保存已有迁移，不代表本轮重新执行全量；A/B 提交、PR 通过、云端集成、本地验收和整体架构完成分别记录。

## 基线检查与历史证据

本轮不改变已有业务实现；对照上一 WireGuard 批次的 3618 项输入，3617 项保持一致，仅工作区架构守卫因扫描规则修正而更新。该批精确源码曾通过 96 项定向行为测试及 187 项架构守卫，2 项 live ICMP 用例明确忽略。此结果不是 workspace/all-features、全协议或当前远端 CI 通过。其余历史批次按 [实施记录](../implementation/README.md) 的实际范围引用，不跨源码套用。

本轮重新执行格式、依赖边界、测试布局、方案来源与链接检查、diff 检查，以及 CI 分支过滤的定向检查；实际状态写 baseline.json。重新运行的工作区架构守卫为 27/0；首轮曾因漏列同步标准 relay 准备模块、误扫测试夹具而出现 25/2，修正只允许该精确准备模块并继续禁止 await/执行/任务创建，生产扫描仍覆盖所有生产 adapter。源码提交按层分组，仅整个最终栈作为云端起点，各中间层提交未单独承诺可构建。任务包不依赖本机缓存或仓库外证据；云端应重新验证适用范围。

## 实现期间

先跑变更职责域的必要定向回归；失败先保留并诊断，禁止扩大 timeout、弱化断言或连续重跑全量掩盖问题。复用同一冻结源码的已构建测试；同一环境有正在运行的全量则接续，不启动重复全量。每个提交记录是否影响之前验证的输入；有新源码则旧结果不能冒充当前通过。

```bash
cargo fmt --all -- --check
python3 scripts/check-test-layout.py
python3 scripts/check-runtime-contracts.py
python3 design/v0.0.3/check-plan.py
git diff --check
```

原始 native controller 关闭凭据、转交 view 退休和实际 TaskScope join 分别核对。挂起取消测试在真实 await I/O 处建立 Pending；不能用 PID、延迟、Drop/abort、端口可重绑或模拟成功代替。新 owner 构建/启动时点必须在旧真实清理确认之后；只有 TEMP-WG-OUTER-UDP-01 的既有路径例外。

## 云端冻结候选

| 检查 | 责任 / 通过要求 |
| --- | --- |
| A 资源准备与事务 | 任务卡列出的真实行为、失败/恢复、持久/临时、所有入口 ACK，以及旧 I/O/编排调用链删除 |
| B Hysteria2 出站 | 实际共池 TCP/UDP/carrier 派发/计量、原件关闭/Scope/网络换代、旧桥删除及固定基线互通 |
| 编译与统一回归 | `cargo check --workspace`、严格 workspace/all-targets/all-features Clippy、`RUST_MIN_STACK=16777216 bash scripts/test-workspace.sh`，记录测试/忽略/失败和退出码 |
| 最小功能 / no_std | 复用现有 CI 的 no-default、代表性独立 feature 与 traits/core no_std 检查；A/B 各自新增影响的最小组合明确验证 |
| 平台与互通 | 现有 workflow 上适用的官方基线互通和平台任务；缺工具/权限明确待验，不能开启未资格登记代替 |
| 依赖与禁止项 | runtime 不生产依赖 proxy/协议，platform/transport 不依赖 config/engine/proxy；无恢复的 listener/handler/Any 正常执行链，无第二宿主/池 |
| 安全与兼容 | 不输出凭据；保留旧配置省略字段的导出形状、发送投递状态、错误来源、身份/撤权/限速/计量和控制 ACK 语义 |

严格 Clippy 命令为 `cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`。不使用只跑某一 test target 的命令冒充完整资格。云端无法取得特权/外部工具时保留缺项，候选可交接但整体资格不能标通过。

主 CI 的 pull_request base 过滤已包含 `codex/v003-cloud-integration`；push 仍仅对原 develop/main 触发，不自动启动每个中间分支的完整矩阵。其他专门 workflow 如尚未包含此 base，由 C 通过其 workflow_dispatch 在明确的候选 ref 执行，且核对运行记录的实际 SHA。文档 PR 的 scope 跳过不证明实现通过；不得手动触发发布工作流。

## 本地最终验收

- 使用云端冻结候选的准确 SHA，独立 checkout，不自动合入 develop，不修改运行中的用户内核或系统网络来迁就测试。
- 同一个新内核产物验证冻结 GUI/面板契约：配置、控制/订阅、revision/ACK、重载、TCP/UDP 和退出。记录消费者版本、二进制摘要及真实执行范围，消费者代码不改。
- 适用平台执行真实 TUN、Packet/Flow 模式、源地址/MTU/回程、拥有/借入设备关闭和系统路由/DNS 恢复。云端已完成的同 SHA 检查可引用，不因拉回再无条件重复完整编译矩阵。
- 1 CPU/1 GiB 同负载比较启动、稳态、突发、容量拒绝、空闲回收、网络/配置换代、恢复与退出的 RSS/FD/任务/吞吐/延迟；预算实现和压力资格分开报告。
- TEMP-WG-OUTER-UDP-01 保留意味着严格资源生命周期目标仍有明确例外，不能声明 G0–G10 全部完成。临时债务纠正须另立完整内核任务，不能以验收记录关闭。

## 首轮后的明确待办

A/B 只承担指定职责域。其余出站 MUX/Mieru/载体的真实宿主归属、System/detour/池化 DNS 与失败重试、非入站 Packet/设备/TUN、适用资源上限生产配置，仍是架构实施待办；G8 的内部控制/观测整体对账及 G9/G10 完整互通/平台/消费者/资源资格另列。不能把仍有实际实现缺口的项目全部标成“只待验收”，也不能把已删除 listener 等旧链重新列待办。

# 端点客户端契约验证记录

日期：2026-09-30。实施基线：develop / f7681499，该提交保存此前全部本地修改。
本次新增内容遵循既有 Engine 条件校验、Proxy 确认事务和注册能力边界。

## 本次实现范围

- 四项端点修改命令增加可选 expected_core_instance_id，与意图版本在同一
  apply 锁内核对；检查先于幂等返回、候选分配、文件写入和资源操作。
- 精确发布运行中出站收缩限制，资源操作级能力同时描述 set_directions 和
  clear_overrides 的收缩边界。
- 配置来源、基准 enabled/directions、源路径支持和最近实际写入事实进入
  资源快照；持久化模式按资源及操作声明，不解析不透明 ID。
- 详情响应增加实例和配置关联字段；未接入的统计、生命周期过渡及独立
  PacketRoute 管理通过精确限制和 null 保持真实能力边界。

客户端使用方式见 [对接说明](network-endpoint-client-integration-v1.md)。
本次没有开发完整 Packet/字节计数、完整生命周期、独立入站 peer 健康或
独立 PacketRoute 公共执行 API，也不把这些缺口伪装为已支持。

## 回归场景

1. 两个 Engine 使用相同配置、相同意图版本而实例 ID 不同：新实例拒绝旧实例
   的四项修改命令，文件字节、运行状态、意图版本和资源代际均不改变。
2. 当前实例及版本允许操作；相同实例上的陈旧版本仍冲突，省略新增条件的
   旧请求继续可用。
3. 资源源路径和规范/旧角色来源决定可用持久化模式；方向覆盖不改变配置
   基准事实；clear_overrides 也准确声明运行中收缩限制。
4. 源文件写入未尝试时权限未知；普通 I/O 失败不虚报权限拒绝，后续真实
   写入成功可恢复事实。源信息不包含宿主绝对路径或协议私钥。
5. HTTP 正确接受新条件，保持 Admin 权限，实例条件冲突返回结构化 409。
6. 既有资源启停、源文件持久化、入站撤权、出站回复和观测字段回归。
7. 首次源写入失败不重复写文件回滚、不虚报运行资源 failed；修复文件环境后
   可用原条件重试。成功写入后的协调失败先释放候选 DNS 阶段，再回滚源文件
   和资源；回滚结束后不遗留 DNS 阶段占用。

## 门禁

最终日志：/Volumes/tool/tmp/zero-endpoint-contract-gates-20260930.log。
阶段起止和退出码：/Volumes/tool/tmp/zero-endpoint-contract-gates-20260930.json。
源文件哈希：/Volumes/tool/tmp/zero-endpoint-contract-gates-20260930-sources.json。
完整测试结束后、Clippy 开始时曾核对完整输出为 328 个目标、2280 passed、
0 failed、155 ignored；随后观察到共享日志尾部部分被覆盖，原因未最终确定。
阶段 JSON 保留实际退出码和起止时间，读取汇总另存
/Volumes/tool/tmp/zero-endpoint-contract-test-summary-20260930.json。
不把当前被覆盖的日志重新解析结果当作完整测试清单。
每阶段完成时均核对源码哈希；超时、源变化和非零退出不得计为通过。
完整门禁于 2026-09-30 16:43:31 +08:00 以 exit=0 结束，冻结 2485 个源文件。
本地制品 SHA256：04af4259ace990ba98b251cb48a6ed2708e976db444b5ee761221660b31ca62f。
版本仍为 0.0.3-dev.202609281319；构建时 Git 为基线加本次未提交源码，
实际源码对应冻结哈希，而非仅凭二进制显示的基线 Git ID 证明内容。

| Gate | Result |
| --- | --- |
| cargo fmt --all -- --check | Passed, exit 0, 10.20 seconds |
| git diff --check | Passed, exit 0 |
| cargo check --workspace | Passed, exit 0, 55.12 seconds |
| RUST_MIN_STACK=16777216 cargo test --workspace --all-features | Passed, exit 0, 7562.03 seconds; 328 targets, 2280 passed, 0 failed, 155 ignored |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | Passed, exit 0, 407.79 seconds |
| cargo build --release --features full,status-api,wireguard | Passed, exit 0, 520.42 seconds |
| target/release/zero --version | Passed, exit 0, 1.49 seconds; wireguard compiled |

前置定向测试的首次尝试因新测试缺少 trait 导入而编译失败，已修正；失败日志
保存在 /Volumes/tool/tmp/zero-endpoint-contract-focused-20260930-attempt1.log。
后续定向测试通过；最终验收仍以以上冻结源码的完整工作区结果为准。
首次最终门禁在测试编译阶段因审查发现源文件回滚问题主动停止，日志保存在
/Volumes/tool/tmp/zero-endpoint-contract-gates-20260930-attempt1.log，不计为通过。
新增回滚测试首次运行暴露候选 DNS 阶段未释放的问题，已修正；失败日志
保存在 /Volumes/tool/tmp/zero-endpoint-rollback-focused-20260930-attempt1.log。

真实 A/B、TUN、外部故障、长期运行、跨平台及已安装客户端验收保持独立。
本地构建不代表新版本已发布、推送或安装；WireGuard 继续为 opt-in。

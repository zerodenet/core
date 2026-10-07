# 端点控制与监听 peer 验证记录

日期：2026-10-06。范围：Zero 内核、契约、测试及项目文档。

## 实现范围

- 具有监听绑定的 WireGuard peer 可省略初始 `endpoint`。协议校验区分
  监听端点与纯出站；纯出站和 `outer_udp_proxy` 仍要求初始地址。
  已认证报文才能更新学习地址；配置地址未知时详情返回 null。
- 资源声明 `supported.peer_address_learning`。方向操作通过
  `supported.operation_capabilities.*.live_direction_contraction` 声明实际能力。
- WireGuard 运行中可撤销出站角色。共享端点关闭被撤权的客户端栈及业务，
  保留监听、协议状态、认证学习地址和另一方向的业务；重新授权可恢复。
  无入站绑定的纯出站资源撤权后可停止实际设备，重新授权时重建。
- 中性 Packet pin 清理只采用已确认资源事实，防止失败候选提前销毁旧路径。
  确认执行同时等待被撤权的 Flow 和已计量 Packet 路径结束。
- 独立启停、restart 发布 starting/stopping 与最终事实。过渡状态保留当前
  generation；确认停止后重新启动才分配新代际。未分配代际返回 null。
- 端点状态事件加入独立随机身份，避免同一毫秒连续事件被投递去重误合并。

协议继续拥有 WireGuard 认证、peer 索引和密钥；中性运行时拥有 socket、
用户态栈、任务和返回路径；Engine 拥有意图、事实、计量及事件。
未修改客户端、系统 DNS、TUN 开关或系统路由配置。

## 关键行为验收

`endpoint_contracts::listening` 使用两个真实 Zero WireGuard 端点，验证：

1. 无初始远端地址的监听 peer 通过认证握手学习地址。
2. 双向 TCP 及 UDP 业务可达。
3. 外部端口占用导致候选配置失败时，原双向 TCP 业务继续工作。
4. 运行中撤销出站关闭该方向业务，原入站业务及监听继续工作。
5. 恢复出站复用学习地址，原入站业务及 generation 保持不变。

真实 IPC 测试验证资源能力、实时撤权确认、generation 保留、旧意图冲突及
重新授权。既有 IPC 权限、实例冲突、协调回滚和统计重置锁测试继续保留。
Engine 测试验证过渡事实、事件 ID 唯一性与确认后清理；Packet pin 测试验证
作用域隔离。WireGuard 既有目标覆盖原生 Packet、派生 Flow、DNS、统计重置、
认证漫游、外层代理链及网络恢复。

## 门禁

使用工作区命令，测试编译并发为 2，测试线程为 4，测试栈为 16 MiB：

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
ZERO_TEST_HOST_IPV4=192.168.50.138 RUST_TEST_THREADS=4 \
  bash scripts/test-workspace.sh --jobs 2
```

最终五项门禁均退出码 0。完整测试在 2026-10-06 22:10:35 至 22:37:37 CST
完成，耗时约 27 分钟：146 个目标结果，2368 通过、0 失败、155 忽略。
测试布局检查为 277 个源文件、88 个集成目标、0 错误。忽略项包含缺少固定
版本参考工具的外部互操作、特权/长期场景及文档示例，不计为通过。
日志目录：`/Volumes/tool/tmp/zero-endpoint-completion-gates-20261006/`。

此前一次完整运行在 2026-10-06 21:17:06 至 22:08:26 CST 结束：146 个目标，
2358 通过、10 失败、155 忽略，退出码 101。九个失败均因环境参数指定的
`192.168.0.102` 已不在本机；另一个是仍要求出站收缩必须停用的旧 IPC 断言。
地址改为经过实际绑定确认的 `192.168.50.138` 后，三个目标单独复测分别为
endpoint_contracts 19/0、proxy_wireguard 10/0、wireguard_inbound 5/0（1 忽略）。
旧 IPC 测试已改为验证实时撤权、资源保留及冲突检查；最终完整门禁重新执行。
该失败记录保留于 `zero-endpoint-completion-gates-20261006-attempt3/`。
更早的编译失败与编译阶段中断均不计为完整测试通过。

## 尚未由本轮完成

- 完整普通启动、配置 reload、进程退出及意外失败的生命周期事实；目前只
  补齐独立端点控制操作的过渡阶段，相关 limitation 保留。
- 单独 PacketRoute 查询/关闭公共操作和完整跨资源依赖停止协调。
- 通用宿主 Direct PacketSink 双向 L3 后端。
- 仅出站端点的通用安全原生 Packet 返回分类；auto/强制模式继续执行现有
  安全限制，不用观测关联冒充访问控制。
- 独立入站的完整逐 peer 健康；不可观测指标继续返回 null。
- 外部固定版本实现、真实 TUN、长期故障恢复、跨平台及 M5 安全/性能验收。
  完整工作区命令中的 ignored 外部/特权/长期项目需要单独运行。

客户端按 [端点对接说明](network-endpoint-client-integration-v1.md) 使用实际
资源能力；本轮不新增协议专属命令，也不以版本号替代 capabilities。

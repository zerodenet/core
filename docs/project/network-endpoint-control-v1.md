# 通用端点控制 V1（P2 独立启停切片）

日期：2026-09-29。实现已接线并通过本地门禁，结果见
[控制验证记录](network-endpoint-control-verification-20260929.md)。
2026-10-06 的地址学习、运行中出站撤权及独立控制过渡事实见
[本轮验证记录](network-endpoint-control-verification-20261006.md)。
本契约补充 [端点目录](network-endpoint-catalog-v1.md)，实施范围遵循
[管理规划](network-endpoint-management-plan.md)。当前注册执行协议为 WireGuard。

## 控制入口

使用已有 `CommandRequest` 与 acknowledged executor。HTTP 使用现有命令入口；
IPC/gRPC/Rust 采用同一请求信封，不新建配置 API 或协议专用管理服务器。
端点命令要求 Admin 权限；同步 `CommandService::execute` 明确返回 unsupported，
必须调用 `execute_acknowledged` 等待资源协调。

IPC 命令分发统一使用服务的确认执行入口，端点控制与 `stats.reset` 均参与
既有协调锁；同步命令由服务内部安排阻塞执行。出站探测仍使用已有异步探测
入口和有界并发队列。客户端请求字段不变，不能把内部方法名写入请求。
本地 IPC 通过操作系统访问控制授予管理员身份，请求中的身份或权限字段
不能降低或提升权限；无管理员权限的远程控制请求由原鉴权入口拒绝。

```json
{
  "method": "endpoints.set_state",
  "params": {
    "endpoint_id": "endpoint:wg-a",
    "enabled": false,
    "persistence": "runtime_only",
    "expected_core_instance_id": "<core_instance_id from the endpoint snapshot>",
    "expected_intent_revision": 7
  }
}
```

`endpoint_id` 为目录返回的不透明 ID。`expected_core_instance_id` 和
`expected_intent_revision` 均可省略；提供时，在与配置应用共用的锁内检查，
检查发生在幂等返回、候选分配、持久化和资源操作之前。任一不匹配返回
conflict，field_path 分别为 params.expected_core_instance_id 或
params.expected_intent_revision。实例不匹配优先于资源查找；失败不改变资源。
客户端应同时提供两个条件，避免重启后意图版本重用的竞态；旧请求仍兼容。

| 命令 | params 特有字段 | 执行语义 |
| --- | --- | --- |
| endpoints.set_state | enabled、persistence | 独立启停一个共享资源 |
| endpoints.set_directions | directions、persistence | 限制已配置可执行角色 |
| endpoints.restart | 无 | 停止并重建启用资源；停用资源返回 invalid_argument |
| endpoints.clear_overrides | 无 | 清除该资源的全部临时覆盖，恢复配置意图 |

四项命令均接受 endpoint_id 和上述两个可选条件。directions 形状为
`{"inbound":false,"outbound":true}`。set_state/set_directions 的 persistence 默认
runtime_only；restart/clear_overrides 不写源配置。

成功响应的 accepted、result.applied、result.reconciled 均为 true，result.endpoint
是协调后的通用快照，另含 persistence。失败返回既有错误信封，无成功标记。
同一意图且资源已处于目标状态时重复操作幂等，不增加 intent_revision。

## 意图与版本

- Engine 快照持有配置意图和 runtime override，所有准入入口读取同一策略。
- runtime_only 不修改源文件和基础配置；同一 ID 的普通 reload 与网络恢复保留
  覆盖；删除资源清除覆盖，进程重启从源配置读取。
- source_file 仅支持规范 endpoints 且 Engine 配有源路径。使用既有持久化和
  确认事务，完成后清除该端点覆盖；旧角色需先显式迁移到规范配置。
- source_file 回滚保存原基础配置，不能把临时开关误写到文件。
- state_source 为 config 或 runtime_override；intent_revision 实际生成并支持
  条件更新。配置 revision 与意图 revision 分开。
- 分配器在进程内共享，失败候选的意图版本不复用；回滚恢复旧意图，不把失败
  候选当成已提交结果。端点运行代际由已应用的资源事实记录；配置实例的
  generation 仍与端点代际分开。

## 生命周期与回滚

资源快照的 configuration 公开规范/旧角色来源和未覆盖的配置基准 enabled、
directions，以及 source_file.available。supported.operation_capabilities 按操作
声明 persistence 模式、preconditions 和运行中方向收缩范围，客户端无需解析 ID。
source_file.writable 是最近一次实际写入观察：未尝试或普通 I/O 错误为 null，
写入成功为 true，权限拒绝为 false；同时提供 writable_observed_at_unix_ms 和
reason。它不保证未来文件权限，查询不会创建探测文件，最终结果仍由确认事务决定。

控制命令、config.apply 和 runtime reload 复用 Proxy 的串行确认事务。每个待确认
操作匹配具体 Engine 快照；相同基础配置但意图不同的通知不能相互确认。
请求连接断开后，已启动的控制执行器继续完成或回滚事务。

stop 阻止新准入、取消属于该资源的 Flow，终止监听和 raw-IP 驱动；旧退休设备
也立即关闭，不继续发送 keepalive。执行器等待监听、主驱动及业务结束确认，
再返回 stopped。无关端点保留设备和监听。现有 PacketRoute 持有的关闭设备不能
重新发送；数据路径 Close 的原有语义仍只释放该路径，不修改资源意图。

候选准备、绑定、DNS 提交或应用服务协调失败时恢复原快照和资源；失败响应说明
回滚结果。监听/业务结束确认有界；超时不得虚报成功。未注册控制能力的协议
返回 unsupported。仅观察到 running 不证明远端业务可达。

## 方向限制与当前缺口

允许方向只能覆盖已经配置的执行角色；未配置监听的端点不能通过方向命令获得
入站能力。停用时可以修改方向，随后启用。运行中方向扩展可以协调发布。

运行中撤销入站方向会在已发布的候选策略阻止远端新业务后，取消原入站 Flow；
控制命令等待它们结束。原生 Packet 入站逐包检查新策略，不保留独立业务任务。
共享协议监听和出站客户端栈继续运行，已关联出站回包仍被接收。配置 reload
同样执行撤权；已准备失败的候选不提前取消原有 Flow。

运行中撤销出站方向通过注册能力执行：共享端点保留入站 socket、协议会话和
已认证 peer 地址，停止主动客户端栈并清除 Packet 返回关联，取消原出站 Flow。
已排队的旧栈报文携带关闭令牌，恢复出站后也不能复活；TUN/raw-IP 会清理被
撤权的 Packet 路径引用。共享端点重新授权时构造新的客户端栈，不重启其入站资源。
没有监听角色的独立出站在撤权后释放设备/载体；enabled 意图保持不变，实际
state 可为 stopped。重新授权时重建设备并产生新 generation。
set_directions / clear_overrides 的 live_direction_contraction 按资源支持的角色
声明；WireGuard 同时支持已配置的入站和出站收缩。新内核不再发布旧的出站
requires_stop 限制。其他注册协议默认不声明该能力，必须自行实现确认撤权。

独立启停和 restart 在配置/服务验证后发布 starting/stopping 状态事实，
协调成功后发布 running/stopped；失败通过既有回滚恢复事实，并记录 last_error。
过渡观察本身不增加 generation；确认停止后的重启才分配新代际。
普通启动、reload、退出和失败的实际状态由已注册的生命周期能力发布；
设备替换和网络恢复的精确范围见下文及 Packet 路径控制契约。

旧配置中增加、移除或重新关联 inbound/outbound 角色属于配置拓扑变更，仍由
已有监听/设备协调流程处理；它可能重建设备，与保留角色时修改方向权限不同。

仅允许出站仍保持握手、keepalive 与已关联回复，拒绝远端新业务。原生 Packet
回程的安全限制与目录契约一致；auto TCP/UDP 使用已有 Flow 转换，Echo 使用
显式 stack 地址转换，不增加 ICMP Flow 平面。

## 分层及后续工作

EndpointControlCapability 单独注册控制支持，与观察、TCP/UDP/Packet 调度能力
分开。Engine 管意图、条件版本、准入和 Flow 归属；Proxy 管协调、确认和任务；
协议适配器投影配置并连接已有设备能力，WireGuard 协议内部状态仍由协议所有。

后续切片已实现运行中双向撤权、确认停止与失败协调、普通生命周期、真实
设备 generation/恢复事实、Flow/Inner/Outer 计量、规范仅监听 peer，以及
独立 PacketRoute 查询/关闭。调用方按 capabilities 使用相应能力，不能依据
本页的初始 P2 切片名称推断当前缺口。

仍保留明确边界：协议未提供的独立入站完整 peer 健康返回不可用；完整系统
丢包不能由本地边界计数推断；通用宿主 PacketSink 需要平台接入与部署条件；
第二种真实 L3 协议、长期恢复和生产安全/性能验收不由本契约自动完成。

既有 A/B、TUN 和外部 Echo 验证仅覆盖记录中的配置与操作；生产拓扑的
故障恢复、长期运行和各平台能力仍需独立验收，本地开关测试不自动关闭
WireGuard 生产门禁。WireGuard 继续为 opt-in。

客户端请求、错误处理与观测边界见
[对接说明](network-endpoint-client-integration-v1.md)。

## Orchestration lifecycle observations

`network_endpoint_orchestration_lifecycle_v1` extends the existing
`endpoint.state_changed` event and endpoint queries; it introduces no new control
command. Ordinary startup publishes `starting` then an observed `running`.
Configuration enable/disable and deletion publish their transition followed by
confirmed facts. Deletion emits a final `stopped` before forgetting the fact;
subsequent queries return not found. Recreating the ID receives a fresh runtime
generation. A rejected candidate restores observed previous resources and records
the error for changed endpoints; unaffected endpoints keep their generation.

Shutdown preserves configuration intent and statistics periods. `stopped` is
published after listener/service owners and outbound device completion receipts
finish. Startup/task failure follows the same cleanup path and publishes `failed`.
A cancellation/panic guard reports `failed` with an explicit unconfirmed-cleanup
error, rather than keeping a stale `running` fact. Grace expiry is also failure,
with owners aborted/joined. OS process termination cannot emit an event; use
`core_instance_id` and query recovery after restart.

`network_endpoint_device_incarnation_v1` covers actual observed component
replacement under the same ID; `network_endpoint_network_recovery_v1` projects
physical-egress preparation, retry, publication and supersession. Query the
reported capabilities and facts rather than assuming every future protocol
has implemented these operations. See [Packet route control](packet-route-host-control-v1.md).

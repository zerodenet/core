# 通用端点控制 V1（P2 独立启停切片）

日期：2026-09-29。实现已接线并通过本地门禁，结果见
[控制验证记录](network-endpoint-control-verification-20260929.md)。
本契约补充 [端点目录](network-endpoint-catalog-v1.md)，实施范围遵循
[管理规划](network-endpoint-management-plan.md)。当前注册执行协议为 WireGuard。

## 控制入口

使用已有 `CommandRequest` 与 acknowledged executor。HTTP 使用现有命令入口；
IPC/gRPC/Rust 采用同一请求信封，不新建配置 API 或协议专用管理服务器。
端点命令要求 Admin 权限；同步 `CommandService::execute` 明确返回 unsupported，
必须调用 `execute_acknowledged` 等待资源协调。

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

**运行中撤销出站方向仍不支持。** 共享客户端栈与 Packet 返回关联尚无独立
撤权边界，当前明确返回 unsupported，要求先停用、修改方向、再启用；
clear_overrides 和完整配置 reload 不能绕过该限制。
能力限制为 endpoint_live_outbound_direction_contraction_requires_stop；资源
set_directions 操作的 live_direction_contraction 为配置支持的 inbound 和 false
outbound。旧的全方向限制标识不再发布。
旧配置中增加、移除或重新关联 inbound/outbound 角色属于配置拓扑变更，仍由
已有监听/设备协调流程处理；它可能重建设备，与保留角色时修改方向权限不同。

仅允许出站仍保持握手、keepalive 与已关联回复，拒绝远端新业务。原生 Packet
回程的安全限制与目录契约一致；auto TCP/UDP 使用已有 Flow 转换，Echo 使用
显式 stack 地址转换，不增加 ICMP Flow 平面。

## 分层及后续工作

EndpointControlCapability 单独注册控制支持，与观察、TCP/UDP/Packet 调度能力
分开。Engine 管意图、条件版本、准入和 Flow 归属；Proxy 管协调、确认和任务；
协议适配器投影配置并连接已有设备能力，WireGuard 协议内部状态仍由协议所有。

本切片没有完成整个 P2/P3/P4/P5，剩余开发包括：

- 运行中出站方向的 Packet 返回关联和客户端栈撤权，保持入站业务连续。
- 依赖资源的完整停止顺序与失败状态协调。
- starting/stopping/failed 和失败恢复的完整生命周期事实；已应用资源已有
  generation、last_error、启动时间和状态变更事件。
- Packet/内外层计数、独立入站 peer 健康与完整统计；现有 Flow 数、低频批量
  endpoint.stats_sampled 事件和已认证来源仍属部分实现。
- 规范配置仅监听 peer、完整载体验收、schema 导出、端点诊断和第二种中性
  测试端点的执行能力。
- 单 PacketRoute 的独立公共管理 API（已有内部 Close 语义继续可用）。

真实 A/B、TUN、故障恢复、长期运行和跨平台属于后续验收，不由本地开关测试
自动关闭 WireGuard 生产门禁。WireGuard 继续为 opt-in。

客户端请求、错误处理与观测边界见
[对接说明](network-endpoint-client-integration-v1.md)。

# 通用网络端点客户端对接 V1

日期：2026-09-30，更新：2026-10-06。范围：Zero 的已注册端点目录、确认控制及真实观测。
这些接口适用于资源能力，当前执行协议为 WireGuard；不要求客户端解析其密钥、
peer 身份或配置 ID，也不新增协议专用启停命令。
历史门禁见 [2026-09-30 验证记录](network-endpoint-client-verification-20260930.md)；
本轮门禁与场景边界见 [2026-10-06 验证记录](network-endpoint-control-verification-20261006.md)。

## 能力发现与查询

先读取 GET /api/v1/capabilities。以下 features 表示当前接口支持：

| feature | 意义 |
| --- | --- |
| network_endpoint_catalog_v1 | list/get/details |
| network_endpoint_control_v1 | 已注册确认控制执行器 |
| network_endpoint_control_preconditions_v1 | 四项命令均接受实例和意图条件 |
| network_endpoint_operation_capabilities_v1 | 资源操作级能力和配置来源信息 |

GET /api/v1/endpoints?offset=0&limit=100 返回分页目录；GET
/api/v1/endpoints/{id} 查询单资源，整个 id 按 URI 路径段编码。权限为 Read。
endpoint_id 是不透明标识，同一资源跨重启可保持相同；core_instance_id 区分
Engine 实例。supported.operations 保留旧字符串列表，新增
supported.operation_capabilities 提供每个修改操作的具体条件。

例如配置包含规范端点、入站监听且 Engine 有源配置路径时，资源快照片段为：

```json
{
  "configuration": {
    "origin": "canonical",
    "enabled": true,
    "directions": {"inbound": true, "outbound": true},
    "source_file": {
      "available": true,
      "writable": null,
      "writable_observed_at_unix_ms": null,
      "reason": null
    }
  },
  "supported": {
    "peer_address_learning": true,
    "operation_capabilities": {
      "set_state": {
        "persistence": ["runtime_only", "source_file"],
        "preconditions": ["expected_core_instance_id", "expected_intent_revision"]
      },
      "set_directions": {
        "persistence": ["runtime_only", "source_file"],
        "preconditions": ["expected_core_instance_id", "expected_intent_revision"],
        "live_direction_contraction": {"inbound": true, "outbound": true}
      }
    }
  }
}
```

以上是具有监听绑定的 WireGuard 响应片段，不是必须由客户端重建的模型；
其他资源仍以实际返回的能力为准。configuration.enabled/directions
是当前基础配置值；allowed/enabled 是可能叠加运行时覆盖后的当前意图；
supported.directions 是已配置可执行角色的范围；effective 是当前运行资源可以
执行的方向。state_source 仅说明配置意图或运行时覆盖，不用于推断持久化来源。

## 发送命令与条件冲突

POST /api/v1/commands 要求 Admin 权限，等待同一 acknowledged executor 完成。
Rust 调用 ProxyHandle::execute_acknowledged；直接 Engine 同步 execute 不执行
资源生命周期。IPC/gRPC 等现有命令载体使用同一 CommandRequest 信封。

```json
{
  "method": "endpoints.set_state",
  "params": {
    "endpoint_id": "<snapshot.endpoint_id>",
    "enabled": false,
    "persistence": "runtime_only",
    "expected_core_instance_id": "<snapshot.core_instance_id>",
    "expected_intent_revision": 7
  }
}
```

意图版本必须取刚查询的快照，而非固定填写 7。四项命令为 set_state、
set_directions、restart、clear_overrides，均支持两个可选条件。set_state 和
set_directions 接受 persistence，默认 runtime_only；restart/clear_overrides
仅操作运行态，不接受 persistence 参数。

内核在与配置应用共用的锁内先检查实例，再检查该端点意图版本，随后才能
幂等返回、分配候选或触碰源文件/资源。实例变化时，即使新实例恰好重用旧版本，
也返回 HTTP 409 / error.code=conflict；field_path 标识冲突字段：

- params.expected_core_instance_id：重新读取实例、能力和端点快照。
- params.expected_intent_revision：重新读取该端点意图与配置基准。

客户端不要用新条件自动重放旧操作；由最新状态重新确认用户操作语义。
HTTP 成功信封的 result.accepted、result.result.applied、
result.result.reconciled 均为 true，result.result.endpoint 是确认后的快照。
原生 CommandResponse 使用 accepted 和 result；HTTP 多一层 ApiResponse 信封。
请求中未提供条件的旧调用仍兼容。
新客户端对旧内核先看 feature/操作能力，仅在支持时发送新增字段；旧内核会
拒绝未知请求字段。异步查询结果仍按 core_instance_id 丢弃陈旧响应。

## 方向收缩与持久化

set_directions 传 directions={inbound,outbound}；只能授权已配置的执行角色。
运行中撤销入站会结束已有入站 Flow、阻止远端新业务，保留共享监听、协议
控制报文和出站回复。WireGuard 运行中撤销出站会独立关闭主动客户端栈和
Packet 返回路径，保留已授权入站及协议会话。客户端按资源操作能力的
live_direction_contraction 判断所需收缩是否可直接执行；它不是目标允许方向。
clear_overrides 同样声明该范围，因为恢复配置方向也可能发生收缩。
旧内核如果仍声明 requires_stop 限制，继续执行既有停用/修改/恢复流程。

source_file 只有规范 canonical 配置且 Engine 有源路径时可用；legacy 配置或
无路径资源只声明 runtime_only。以操作的 persistence 列表为准，不能解析 ID
或只凭 origin 推断。成功持久化后清除该端点临时覆盖；clear_overrides 不写文件。

source_file.available 表示结构支持，不包含当前请求的 Admin 授权和 OS 权限。
writable 是最近一次真实写入结果：成功 true，权限拒绝 false，未尝试或其他
I/O 错误 null。writable_observed_at_unix_ms 给出观察时间，reason 使用
canonical_configuration_required、source_path_unavailable、
source_write_permission_denied 或 source_write_failed。查询不会写探测文件，
不存在用有路径或文件元数据伪造可写结论的情况。权限和磁盘状态可能变化，
持久化成功始终以当前事务结果为准；writable=null 应展示为未知。
首次写入失败时源文件未替换，回滚只恢复运行资源，不重复写入旧文件；写入
成功后资源协调失败，则同时回滚源文件和运行资源。错误后重新查询 state、
intent_revision 和 last_error；一次操作失败不等于端点资源已经 failed。

## 观测、peer 与路径边界

`counters.active_stream_flows` / `active_datagram_flows` 是活动业务连接数量，
`active_packet_routes` 是原生/转换 Packet 转发会话数。连接空闲且没有路径时
`0 / 0 / 0` 是真实状态；`null` 表示没有对应观测提供者，不能按零显示。
已完成的业务字节仍在 inner/outer 或 Flow 周期累计计数中，三项活动数不替代流量。
`traffic_packet_route_idle_observation_v1` 表示 Raw-IP 提供者已在准备时声明
路径观测，未产生原生 Packet 会话的设备也可返回已知零。仅派生 TCP/UDP Flow
业务不会增加 Packet 路由数。本机 Echo 也不是 Packet 转发路径。
公共 `packet_routes` 查询与端点计数使用现有 Packet 生命周期；共享资源按 ID
去重，关闭路径/停止入站/停用端点会释放对应活动数，`stats.reset` 不改变活动数。

GET /api/v1/endpoints/{id}/details 返回 schema_id/schema_version/details，另含
core_instance_id、config_revision、generation 和 observed_at_unix_ms。当前
WireGuard schema 为 zero.endpoint.wireguard.v1，版本 1；详情中的 peer ID、
公开密钥、AllowedIPs、配置 endpoint、认证 endpoint、来源已知状态和认证
报文年龄由协议观察器提供。现有出站健康包括握手年龄；独立入站的完整 peer
健康仍可为 null。不暴露私钥、PSK 或 payload。
supported.peer_address_learning=true 的监听端点可省略 peer endpoint；
configured_endpoint=null 与 authenticated_endpoint 分开显示，不能把未知地址
显示为 0.0.0.0 或已连通。无监听的纯出站及 outer_udp_proxy 仍要求配置地址。

通用快照已提供当前 Stream/Datagram Flow 数、已记录的运行代际、启动时间、
状态和错误。running 表示本地资源存在，不保证 peer 或业务可达。代际不能
替代实例和意图条件。具有 network_endpoint_orchestration_lifecycle_v1 的运行时
已接入普通启动、配置启停/删除、正常退出、任务失败和协调任务中断的状态事实。

显式端点的 Packet 路径数、内外层字节/报文数和本地已观察丢弃已有真实计量；
缺少提供者或不可观测的指标仍为 null。查询、批量采样、stats_epoch 和重置按
[流量观测 V1](traffic-observation-v1.md) 对接，客户端不能以 generation 代替统计周期。
具有 `packet_route_management_v1` 的内核提供分页 `packet_routes`、单项
`packet_route` 和管理员确认命令 `packet_routes.close`。只关闭实际持有的
Packet 会话，不改变端点意图或 Flow 用量；独立 Flow/Echo 生命周期保持原接口。
具体请求、错误和事件见 [Packet 路径与宿主 L3](packet-route-host-control-v1.md)。
具有 `network_endpoint_device_incarnation_v1` 的内核按真实设备组件替换更新
代际；相同 ID 的密钥更新/重建与物理出口替换也接入观测。策略或方向变化保留
设备时不改变代际。`network_endpoint_network_recovery_v1` 在既有端点快照和
事件的 `recovery` 中投影 preparing/retrying/recovered/superseded，恢复确认
表示本地设备发布，不代表远端握手或业务可达。
正常退出先发布 stopping，确认监听器和设备 I/O 任务结束后发布 stopped。
启动/后台任务失败经同一清理路径发布 failed 和 last_error；运行协调任务被
取消或 panic 的析构保护会报告未确认清理的 failed，不能据此断言资源已释放。
进程被强制终止后无法向旧事件日志写入最后事件；消费者以新的
core_instance_id 重建查询基线。

复用 GET /api/v1/events/stream 和已有回放入口。endpoint.state_changed 是状态
事实变更；endpoint.stats_sampled 每 10 秒按最多 64 个资源分页批量发出，
samples 内含 endpoint_id、config_revision、generation、采样时间和 counters。
使用事件信封的实例与序列恢复机制，缺失计数仍为 null。

本说明不宣称 A/B 实网、真实宿主转发、长期运行或跨平台生产验收已完成。
新增路径控制和恢复观测的实现范围及实际门禁结果见上述接入文档。

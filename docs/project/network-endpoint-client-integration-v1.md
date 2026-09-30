# 通用网络端点客户端对接 V1

日期：2026-09-30。范围：Zero 的已注册端点目录、确认控制及真实观测。
这些接口适用于资源能力，当前执行协议为 WireGuard；不要求客户端解析其密钥、
peer 身份或配置 ID，也不新增协议专用启停命令。
当前门禁与场景边界见 [验证记录](network-endpoint-client-verification-20260930.md)。

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
    "operation_capabilities": {
      "set_state": {
        "persistence": ["runtime_only", "source_file"],
        "preconditions": ["expected_core_instance_id", "expected_intent_revision"]
      },
      "set_directions": {
        "persistence": ["runtime_only", "source_file"],
        "preconditions": ["expected_core_instance_id", "expected_intent_revision"],
        "live_direction_contraction": {"inbound": true, "outbound": false}
      }
    }
  }
}
```

以上只是响应片段，不是必须由客户端重建的模型。configuration.enabled/directions
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
控制报文和出站回复。当前运行中撤销出站需先 set_state(false)，再修改方向，
随后按需要启用。global_limitations 的精确标识为
endpoint_live_outbound_direction_contraction_requires_stop。资源操作能力的
live_direction_contraction 是该操作当前支持的范围，不能视为目标允许方向。
clear_overrides 同样声明该范围，因为恢复配置方向也可能发生收缩；客户端
根据 configuration 的基准值判断该操作是否涉及撤销出站，不能绕过限制。

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

GET /api/v1/endpoints/{id}/details 返回 schema_id/schema_version/details，另含
core_instance_id、config_revision、generation 和 observed_at_unix_ms。当前
WireGuard schema 为 zero.endpoint.wireguard.v1，版本 1；详情中的 peer ID、
公开密钥、AllowedIPs、配置 endpoint、认证 endpoint、来源已知状态和认证
报文年龄由协议观察器提供。现有出站健康包括握手年龄；独立入站的完整 peer
健康仍可为 null。不暴露私钥、PSK 或 payload。

通用快照已提供当前 Stream/Datagram Flow 数、已记录的运行代际、启动时间、
状态和错误。running 表示本地资源存在，不保证 peer 或业务可达。代际不能
替代实例和意图条件，完整 starting/stopping 过程观察仍未完成。

Packet 路径数、内外层字节/报文数、丢包数未接入真实计数，保持 null；客户端
不得转成 0。单独 PacketRoute 公共管理尚无操作能力，不提供模拟协议命令。
当前全局限制明确公布：endpoint_packet_and_byte_counters_unavailable、
endpoint_transitional_lifecycle_facts_incomplete 和
endpoint_individual_packet_route_control_unavailable。

复用 GET /api/v1/events/stream 和已有回放入口。endpoint.state_changed 是状态
事实变更；endpoint.stats_sampled 每 10 秒按最多 64 个资源分页批量发出，
samples 内含 endpoint_id、config_revision、generation、采样时间和 counters。
使用事件信封的实例与序列恢复机制，缺失计数仍为 null。

本说明不宣称完整统计、完整生命周期、独立路径管理、A/B 实网、长期运行或
跨平台生产验收已完成；这些能力只有接入真实执行和观察后才能声明。

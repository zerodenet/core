# Connector

Connector 是可选的通用 Webhook 事件投递能力，不是面板协议，也不是另一套控制 API。它负责把内核事件转换为稳定的外部 envelope，并维持过滤、投递、重试和恢复循环；外部系统负责业务判断，内核负责执行 Zero API/gRPC 方法。

## 注册方式

中心通过现有 Zero API 提交 `config.apply`，在完整运行配置的 `api.event_sinks` 中注册或更新 Webhook。HTTP 使用 `POST /api/v1/commands`，gRPC 使用 `Control.Execute`；两者承载同一个 `CommandRequest`。

```json
{
  "method": "config.apply",
  "params": {
    "config": {
      "inbounds": [],
      "outbounds": [],
      "route": {
        "rules": [],
        "final": { "type": "direct" }
      },
      "api": {
        "event_sinks": [
          {
            "type": "webhook",
            "tag": "traffic-delivery",
            "url": "https://central.example/ingest/traffic",
            "events": ["flow.completed", "stats.sampled"],
            "source_id": "edge-west",
            "headers": {
              "authorization": "Bearer receiver-defined-token"
            }
          },
          {
            "type": "webhook",
            "tag": "operations-delivery",
            "url": "https://operations.example/ingest/zero",
            "events": ["engine.warning"],
            "source_id": "edge-west"
          }
        ],
        "outbox_path": "state/event-outbox.jsonl"
      }
    }
  }
}
```

`api.event_sinks` 是零到多个独立注册。注册关系如下：

- `tag` 是节点本地的投递通道标识，用于 sink 状态和 outbox 键；它不是节点 ID、协议 ID 或中心资源 ID。
- `url` 是接收方提供的完整地址。Zero 原样使用它，不拼接路径或解释资源名称。同一地址可被多个注册复用，也可被多个节点共同使用。
- `events` 是该注册的事件类型过滤器；空数组表示接收全部事件。一个注册可接收多个事件类型，也可用多个注册按事件能力分流。
- `source_id` 是可选的生产者元数据，只写入 envelope，不参与 URL 选择、路由或投递身份。
- `headers` 是不透明 HTTP headers；认证方案由调用方和接收方决定。

代理协议不是 Webhook 注册维度。若某类事件包含协议事实，它只出现在相应事件 payload 中，不决定投递地址。

`config.apply` 在持久化前完成配置验证和运行时重建。Webhook 变更通过 EventDispatcher 热应用，不要求重启节点。调用方必须基于当前配置构造候选配置；并发写入的事务和回滚语义见 [HTTP API](./http-api.md) 与 [配置](./configuration.md)。

## 推送格式

每次请求对已注册的完整 URL 执行一次 `POST`，请求体是一个 `zero.event.v1` JSON envelope：

```json
{
  "schema_id": "zero.event.v1",
  "event_id": "1730000000000-42",
  "event_type": "flow.completed",
  "occurred_at_unix_ms": 1730000000000,
  "source_id": "edge-west",
  "sequence": 42,
  "principal_key": "credential-value",
  "labels": {},
  "payload": {}
}
```

- `event_id` 是投递幂等键。同一事件重试或崩溃恢复后保持不变。
- `source_id` 由 sink 注册方设置，用于区分事件来源，但不表示 Webhook 与该来源绑定。
- `sequence` 是节点事件序列；接收方可用于检测缺口，但 ACK 仍以单次 HTTP 响应为准。
- `payload` 由 `event_type` 对应的 Zero 事件合同定义，详见 [事件目录](./events.md)。

## 确认语义

Connector 只解释 HTTP 结果，不解释响应正文：

| 结果 | 处理 |
|------|------|
| 任意 `2xx` | 已确认，完成该事件投递 |
| `429` | 未确认，可重试 |
| 任意 `5xx` | 未确认，可重试 |
| 网络、连接或超时错误 | 未确认，可重试 |
| 其他 HTTP 状态 | 未确认、不可重试，进入失败/死信处理 |

每个 sink 拥有独立投递 worker；某个接收端阻塞或超时不会串行阻塞其他 sink。启用 `api.outbox_path` 后，未确认事件在进程重启后继续投递。超时、退避、重试阈值与耗尽行为由 `api.dispatcher` 配置；默认 `retry_forever` 不会在固定次数后删除可重试事件。`dead_letter` 和 `discard` 必须由部署方显式选择。Connector 提供的是至少一次投递；接收方应按 `event_id` 幂等处理。

outbox 不使用固定容量上限。每次新增持久记录前，Connector 都会读取 outbox 所在文件系统的实时可用空间，并保留 `max(outbox_min_free_bytes, total_space × outbox_min_free_percent)`。默认保留 1 GiB 或 5%，取较大值。低于水位时新的 PUT 会 fail-closed：当前事件与尚未完成的 sink 集合留在 dispatcher 中，事件游标不前移，也不会被静默转成 dead letter；已经持久化的 delivery 仍可投递并写 ACK，从而允许积压自行回落。ACK 和压缩可使用正常保留空间，但仍保留有效水位 25%（至少 64 MiB、且不超过有效水位）的紧急维护空间，避免恢复日志反过来写满磁盘。`GET /api/v1/sinks` 通过 `outbox_storage.write_blocked`、`reserve_bytes`、`maintenance_reserve_bytes` 和容量字段暴露该状态。

这个保护优先避免写满节点磁盘，但它不是无限保留承诺。若磁盘压力持续到 engine event log 淘汰尚未持久化的后续事件，状态会记录 replay gap，外部系统仍需按业务账本执行对账或重放。

## 明确不属于 Connector 的内容

- 中心侧 URL、资源层级、节点注册和同步状态机；
- 用户、套餐、计费、凭据或 inbound 管理模型；
- `config.apply` 之外的私有配置/命令协议；
- 第三方面板适配和兼容 DTO；
- presence、traffic 等专用 HTTP 端点。

限流、停用、升级、通知等策略和工作流由外部系统决定。需要内核改变运行状态时，外部系统调用已有的 Zero HTTP/IPC/gRPC 通用方法或应用配置；程序升级由部署系统执行。Connector 不接收、不解释这些业务命令，也不维护面板状态。

Connector 只补充节点向已注册接收端可靠推送事件的能力。它的“保活”是节点内投递循环、重试/outbox 恢复和 sink 状态，不是要求中心实现固定心跳端点。外部系统需要活性信号时，可订阅适合的周期事件（例如启用统计采样后的 `stats.sampled`），并自行定义超时判断。

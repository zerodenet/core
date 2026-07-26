# Connector 接入指南

本指南面向需要接收 Zero 节点事件的外部系统。接收端不需要实现 Zero 规定的服务端路径；它只需准备自己的 Webhook URL，然后通过节点已有的 HTTP 或 gRPC 控制 API 注册该 URL。

## 1. 构建节点

```bash
cargo build --release --features full,status-api,connector
```

`connector` 启用 Webhook 投递，`event-dispatcher` 提供过滤、重试、outbox 和 sink 状态。需要本地 JSONL sink 时另加 `sink-jsonl`。

## 2. 注册 Webhook

先通过 `GET /api/v1/config` 取得当前配置，在该配置的 `api.event_sinks` 中增删 Webhook，然后通过 `POST /api/v1/commands` 提交：

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
            "url": "https://central.example/receivers/traffic",
            "events": ["flow.completed", "stats.sampled"],
            "source_id": "edge-west",
            "headers": {
              "x-central-token": "opaque-secret"
            }
          },
          {
            "type": "webhook",
            "tag": "operations-delivery",
            "url": "https://operations.example/receivers/zero",
            "events": ["engine.warning"],
            "source_id": "edge-west"
          }
        ],
        "outbox_path": "state/event-outbox.jsonl",
        "dispatcher": {
          "webhook_timeout_ms": 10000,
          "max_retry_attempts": 3,
          "retry_initial_delay_ms": 4000,
          "retry_max_delay_ms": 64000,
          "outbox_min_free_bytes": 1073741824,
          "outbox_min_free_percent": 5,
          "exhausted_delivery_policy": "retry_forever"
        }
      }
    }
  }
}
```

gRPC 调用 `Control.Execute`，payload 使用完全相同的 JSON，并通过 metadata `authorization: Bearer <api.control key>` 认证。内置 gRPC 是明文 HTTP/2；跨主机部署应在同机可信代理终止 TLS。Zero 原样使用 `url`，不会追加 `/register`、`/sync`、`/traffic`、`/presence` 或任何其他路径。

注册数量和拓扑不受节点或代理协议约束：

- 一个节点可注册一个或多个 Webhook；
- 一个注册可订阅一个或多个事件类型，`events: []` 表示全部；
- 多个节点可使用相同 URL；
- 同一 URL 也可用不同 `tag` 注册多次，分别维护事件过滤和投递状态；
- `tag` 只在节点本地标识投递通道，`source_id` 只作为 envelope 元数据；
- VMess、VLESS、Trojan 等代理协议不参与 Webhook 地址选择。

## 3. 实现接收端

接收端接受 `POST` 和 `zero.event.v1` JSON。成功持久化或幂等确认后返回任意 `2xx`。需要节点稍后重试时返回 `429` 或 `5xx`。其他状态表示不可重试拒绝。

接收端应使用 `event_id` 去重，因为断网、超时和崩溃恢复会产生至少一次投递。完整 envelope 和状态分类见 [Connector 合同](../control-plane-api/connector.md)。

每个 sink 独立投递。默认耗尽策略 `retry_forever` 会持续保留并重试可恢复故障；只有明确选择 `dead_letter` 或 `discard` 才会在达到阈值后结束可重试 delivery。

## 4. 观察与变更

- `GET /api/v1/sinks` 查看 pending、成功/失败计数和最近错误；
- 同一接口的 `outbox_storage` 查看实时可用空间、有效保留水位和 `write_blocked`；默认水位为 1 GiB 或文件系统容量的 5%，取较大值；
- 通过新的 `config.apply` 修改 URL、headers、事件过滤或移除 sink；
- 备份 `api.outbox_path` 前先停止节点或使用文件系统一致性快照；
- 不要让中心维护另一份旧配置后直接覆盖节点，应基于最新配置生成候选事务。

节点的 inbound、路由、凭据和协议配置仍由 Zero 配置合同管理。限流、停用、升级、通知等业务决策由外部系统完成；需要改变内核运行状态时调用已有的 Zero HTTP/IPC/gRPC 方法或应用配置，程序升级由部署系统执行。Connector 不拥有这些语义，也不是入站命令通道。

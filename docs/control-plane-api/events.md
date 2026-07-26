# 事件目录

所有事件以归一化信封格式输出，通过 SSE、IPC 流、CLI 或 Sink 投递消费。`flow.snapshot` 是实时订阅建立时生成的同步基线，不写入事件环，也不投递到 JSONL/Webhook sink；其余 flow 生命周期事件按正常事件路径投递。

## 事件信封

```json
{
  "schema_id": "zero.event.v1",
  "event_id": "9f20d4e11ac24cc590df30d90a47c821:flow.completed:42:1713500000000",
  "event_type": "flow.completed",
  "occurred_at_unix_ms": 1713500000000,
  "source_id": null,
  "sequence": 1024,
  "principal_key": "user-001",
  "labels": {},
  "payload": { }
}
```

| 字段 | 说明 |
|------|------|
| `schema_id` | 事件格式标识 |
| `event_id` | 跨引擎启动唯一且在重放中稳定的不透明标识；消费者只能用于去重，不得解析其格式 |
| `event_type` | 事件类型，用于过滤 |
| `occurred_at_unix_ms` | 事件时间戳（毫秒） |
| `source_id` | 节点标识（sink 投递时注入） |
| `sequence` | 单调递增序号，用于 SSE 断点续传 |
| `principal_key` | 关联的主体标识 |
| `payload` | 事件负载（类型相关） |

引擎生成的事件 ID 当前包含随机启动 epoch 和事件本地标识，用于避免进程重启后 flow ID、序号和毫秒时间戳复用造成消费者或 Sink 去重碰撞。该组成方式不是公共协议；所有消费者必须把完整字符串作为不透明幂等键。

## 事件类型总览

| 事件 | 触发时机 | 频率 |
|------|---------|------|
| `engine.started` | 进程启动 | 启动时 1 次 |
| `engine.stopped` | 进程停止 | 停止时 1 次 |
| `engine.warning` | 非致命异常 | 按需 |
| `config.changed` | 配置热重载完成 | 按需 |
| `flow.started` | flow 建立 | 每个新连接 |
| `flow.snapshot` | 实时订阅建立 | 每次包含 flow 类型的订阅 1 次 |
| `flow.routed` | 路由和实际出站建立 | 每个成功进入路由阶段的 flow |
| `flow.updated` | 活动 flow 流量快照 | 每 1s 检查，仅发送有变化的 flow |
| `flow.completed` | flow 结束/被关闭/被阻断 | 每个结束的 flow |
| `policy.selected` | selector 切换 | 按需 |
| `policy.probe.completed` | url_test 完成一轮探测 | 按探测间隔 |
| `stats.sampled` | 统计采样 | 每 1s |
| `ipc.connected` | IPC 客户端连接 | 按需 |
| `ipc.disconnected` | IPC 客户端断开 | 按需 |

---

## 统一连接记录 `FlowRecord`

`flow.started`、`flow.routed`、`flow.updated` 和 `flow.completed` 保留原有顶层兼容字段，并在 `payload.record` 中携带统一的连接记录。新消费者应优先解析 `record`：

```json
{
  "flow_id": "42",
  "revision": 8,
  "state": "completed",
  "network": "tcp",
  "inbound": { "tag": "socks5", "protocol": "socks5" },
  "source": {
    "ip": "192.168.1.10",
    "port": 52864,
    "process_id": 1234,
    "process_name": "curl",
    "process_path": "/usr/bin/curl"
  },
  "target": {
    "host": "example.com",
    "port": 443,
    "resolved_ip": "203.0.113.10",
    "sniffed_host": "example.com"
  },
  "route": {
    "mode": "rule",
    "action": "route",
    "target": "proxy",
    "matched_rule": { "index": 3, "condition": "domain: example.com" },
    "selection_chain": ["proxy", "server-a"]
  },
  "path": {
    "outbound": { "tag": "server-a", "protocol": "vless" },
    "remote": { "host": "198.51.100.8", "port": 443 },
    "relay_chain": []
  },
  "traffic": {
    "bytes_up": 1024000,
    "bytes_down": 5120000,
    "inbound_rx_bytes": 1024000,
    "inbound_tx_bytes": 5120000,
    "outbound_rx_bytes": 5120000,
    "outbound_tx_bytes": 1024000,
    "packets_up": null,
    "packets_down": null
  },
  "throughput": {
    "upload_bps": 8192,
    "download_bps": 32768,
    "sampled_at_unix_ms": 1713500000000
  },
  "timing": {
    "started_at_unix_ms": 1713499988000,
    "last_activity_at_unix_ms": 1713499999900,
    "ended_at_unix_ms": 1713500000000,
    "duration_ms": 12000
  },
  "result": {
    "outcome": "chained_relayed",
    "close_reason": null,
    "failure": null
  }
}
```

- `revision` 在同一 `flow_id` 内单调递增；消费者只应用不小于当前 revision 的记录。
- `state` 为 `opening`、`active` 或 `completed`。
- `traffic.bytes_up` / `bytes_down` 是用户方向汇总，同一个中继字节只计一次；四个边界计数用于传输诊断。
- `result` 仅在完成记录中出现。失败时 `result.failure` 提供 `stage`、稳定 `code`、`message` 和可选 `remote`。
- `source`、进程、解析 IP、嗅探域名等字段无法获取时会省略，消费者必须按可选字段处理。

---

## 负载规范

为避免重复，下面各生命周期事件示例主要展示既有顶层兼容字段；实际事件还会携带上文定义的完整 `payload.record`，不能把它当作增量片段解析。

### engine.started

```json
{
  "build_id": "<build-id>",
  "started_at_unix_ms": 1713500000000
}
```

### engine.stopped

```json
{
  "stopped_at_unix_ms": 1713503600000,
  "reason": "signal"
}
```

| reason | 说明 |
|--------|------|
| `signal` | 收到 SIGINT/SIGTERM 信号 |

### config.changed

```json
{
  "changed_at_unix_ms": 1713501000000
}
```

### engine.warning

```json
{
  "code": "ipc_hook_unreachable",
  "message": "ipc hook unreachable (Connection refused); allowing flow (fail-open)"
}
```

| code | 说明 |
|------|------|
| `ipc_hook_unreachable` | IPC hook 进程不可达，fail-open 放行 |

### flow.started

连接被接受并写入活动表后发送。`payload.record.state` 为 `opening`；路由和实际出站尚未确定的字段使用 pending/空值。

```json
{
  "flow_id": "42",
  "network": "tcp",
  "inbound": { "tag": "socks5", "protocol": "socks5" },
  "auth": { "scheme": "noauth", "principal_key": null, "attributes": {} },
  "target": { "host": "example.com", "port": 443 },
  "route": { "mode": "rule", "target": null },
  "policy": null,
  "outbound": { "tag": "server-a", "protocol": "vless" },
  "traffic": { "bytes_up": 0, "bytes_down": 0, "packets_up": null, "packets_down": null },
  "timing": { "started_at_unix_ms": 1713500000000, "ended_at_unix_ms": null, "duration_ms": null },
  "outcome": "direct_relayed"
}
```

### flow.snapshot

实时消费者订阅任一 flow 生命周期事件后收到的活动连接基线：

```json
{
  "watermark": 1024,
  "records": [
    {
      "flow_id": "42",
      "revision": 4,
      "state": "active",
      "network": "tcp",
      "inbound": { "tag": "socks5", "protocol": "socks5" },
      "target": { "host": "example.com", "port": 443, "resolved_ip": "203.0.113.10" },
      "route": {
        "mode": "rule",
        "action": "route",
        "target": "proxy",
        "selection_chain": ["proxy", "server-a"]
      },
      "path": {
        "outbound": { "tag": "server-a", "protocol": "vless" },
        "remote": { "host": "198.51.100.8", "port": 443 },
        "relay_chain": []
      },
      "traffic": {
        "bytes_up": 1024000,
        "bytes_down": 5120000,
        "inbound_rx_bytes": 1024000,
        "inbound_tx_bytes": 5120000,
        "outbound_rx_bytes": 5120000,
        "outbound_tx_bytes": 1024000,
        "packets_up": null,
        "packets_down": null
      },
      "throughput": {
        "upload_bps": 8192,
        "download_bps": 32768,
        "sampled_at_unix_ms": 1713500010000
      },
      "timing": {
        "started_at_unix_ms": 1713499988000,
        "last_activity_at_unix_ms": 1713500010000
      }
    }
  ]
}
```

`records` 中每项都是完整 `FlowRecord`。IPC 会先返回 subscribe ACK，再发送快照；SSE/CLI 实时订阅同样会收到快照。快照只用于重建当前活动态，不进入事件环、`GET /api/v1/events` 或外部 sink。

### flow.routed

路由决策和实际出站建立后发送。`payload.record.state` 为 `active`，此时 `record.route` 包含命中规则和选择链，`record.path` 包含最终出站、实际远端与中继链。

### flow.updated

内核每 1 秒检查一次活动 flow，只对 revision 已变化的连接发射。它是允许消费者合并或丢弃的流量/速率样本，不是历史记录。

```json
{
  "flow_id": "42",
  "network": "tcp",
  "inbound_tag": "socks5",
  "outbound_tag": "server-a",
  "bytes_up": 1024000,
  "bytes_down": 5120000,
  "inbound_rx_bytes": 1024000,
  "inbound_tx_bytes": 5120000,
  "outbound_rx_bytes": 5120000,
  "outbound_tx_bytes": 1024000,
  "throughput_up_bps": 8192,
  "throughput_down_bps": 32768,
  "snapshot_at_unix_ms": 1713500010000
}
```

### flow.completed

flow 终结事件，是完成流量统计的权威事实。`payload.record` 是自包含的最终事实，消费者收到后不需要再查询内核历史记录；消费者如何用于监控、审计或计费不属于 Zero 合同。

> **时间戳说明**：事件信封上层的 `occurred_at_unix_ms` 记录 flow 结束时间；payload 内 `timing.started_at_unix_ms` / `ended_at_unix_ms` / `duration_ms` 提供完整时间窗口。

```json
{
  "flow_id": "42",
  "network": "tcp",
  "inbound": { "tag": "socks5", "protocol": "socks5" },
  "auth": { "scheme": "socks5", "principal_key": "user-001", "attributes": {} },
  "target": { "host": "example.com", "port": 443 },
  "route": { "mode": "rule", "target": null },
  "policy": null,
  "outbound": { "tag": "server-a", "protocol": "vless" },
  "traffic": {
    "bytes_up": 1024000,
    "bytes_down": 5120000,
    "inbound_rx_bytes": 1024000,
    "inbound_tx_bytes": 5120000,
    "outbound_rx_bytes": 5120000,
    "outbound_tx_bytes": 1024000,
    "packets_up": null,
    "packets_down": null
  },
  "timing": {
    "started_at_unix_ms": 1713499988000,
    "ended_at_unix_ms": 1713500000000,
    "duration_ms": 12000
  },
  "outcome": "direct_relayed"
}
```

| 字段 | 说明 |
|------|------|
| `flow_id` | flow 唯一标识 |
| `network` | `tcp` 或 `udp` |
| `inbound.tag` | 入站 tag |
| `inbound.protocol` | 入站协议 |
| `auth.principal_key` | 稳定、非敏感的 Zero 主体标识 |
| `target.host` | 目标地址 |
| `target.port` | 目标端口 |
| `traffic.bytes_up` | 上行字节数（用户→代理） |
| `traffic.bytes_down` | 下行字节数（代理→用户） |
| `traffic.inbound_rx_bytes` | 入站方向接收字节 |
| `traffic.inbound_tx_bytes` | 入站方向发送字节 |
| `traffic.outbound_rx_bytes` | 出站方向接收字节 |
| `traffic.outbound_tx_bytes` | 出站方向发送字节 |
| `timing.started_at_unix_ms` | 连接建立时间（毫秒） |
| `timing.ended_at_unix_ms` | 连接结束时间（毫秒） |
| `timing.duration_ms` | 持续时长（毫秒） |
| `outcome` | 最终结果（见下表） |
| `record` | 自包含的最终 `FlowRecord`，其中 `state=completed` 且包含 `result` |

outcome 值：

| 值 | close_reason | 说明 |
|-----|-------------|------|
| `direct_relayed` | 通常无 | 直连成功 |
| `chained_relayed` | 通常无 | 链式转发成功 |
| `blocked` | — | 被路由规则拒绝 |
| `failed` | `upstream_error` | 上游/传输错误 |
| `cancelled` | `manual` | 被 `flows.close` 关闭 |
| (any) | `idle_timeout` | 空闲超时内核原语 |
| (any) | null (省略) | 正常结束 / 未指定 |

`close_reason` 为 `flow.completed` 负载上的可选字符串字段，用于区分终止原因（标准原因为 `"manual"`、`"idle_timeout"`、`"upstream_error"`）。对于常规对端关闭或 session 生命周期中手动处理之外的其他 finish 路径，会省略该字段。

GUI 或其他消费者应自行决定完成记录的内存上限、索引和持久化策略。需要长期可靠留存时配置 JSONL/Webhook sink；不要依赖内核的有限诊断窗口作为历史数据库。

### policy.selected

```json
{
  "policy_tag": "proxy",
  "policy_kind": "selector",
  "selected": "server-a",
  "previous": "direct"
}
```

### policy.probe.completed

url_test 探测完成后发射，包含每个成员的探测结果。

```json
{
  "policy_tag": "auto",
  "trigger": "scheduled",
  "url": "http://www.gstatic.com/generate_204",
  "started_at_unix_ms": 1710000000000,
  "completed_at_unix_ms": 1710000000320,
  "duration_ms": 320,
  "selected": "server-b",
  "members": [
    { "target_tag": "server-a", "healthy": true, "latency_ms": 120, "error": null },
    { "target_tag": "server-b", "healthy": true, "latency_ms": 85, "error": null },
    { "target_tag": "server-c", "healthy": false, "latency_ms": null, "error": "connection refused" }
  ]
}
```

`trigger` 的取值为 `startup`、`scheduled` 或 `manual`。事件 envelope 的时间戳表示发布时间；payload 中的时间戳表示完整探测周期，`duration_ms` 表示探测耗时。

### stats.sampled

本地控制面默认每 1 秒发射一次。该事件面向 GUI 和本地观测，远程控制器同步或批量上报应使用独立间隔，不应直接绑定本地采样频率。查询接口（HTTP `GET /api/v1/stats` / IPC `{"stats":{}}`）始终返回调用时的当前快照。

```json
{
  "active_sessions": 42,
  "total_started": 1234,
  "completed_sessions": 1192,
  "failed_sessions": 3,
  "blocked_sessions": 10,
  "direct_sessions": 800,
  "chained_sessions": 392,
  "bytes_up": 1024000000,
  "bytes_down": 5120000000,
  "udp_upstream": {
    "active_associations": 5,
    "created_associations": 50,
    "reused_associations": 45,
    "closed_associations": 48,
    "idle_timeouts": 1,
    "dropped_associations": 0,
    "failed_association_attempts": 0,
    "send_failures": 0,
    "recv_failures": 0,
    "packets_sent": 10000,
    "packets_received": 9500
  }
}
```

## 事件过滤

所有消费方式均支持 `event_type` 白名单过滤：

| 写法 | 含义 |
|------|------|
| `"events": ["flow.completed"]` | 仅接收 `flow.completed` |
| `"events": ["flow.completed", "flow.started"]` | 接收两个指定类型 |
| `"events": ["*"]` | 接收所有事件（等价于省略或传空数组） |
| `"events": null` / 省略 | 接收所有事件 |

内部 `EventFilter` 的 `event_types` 为空数组时即不过滤，`*` 作为特殊值等价于空数组。

### ipc.connected

```json
{
  "active": 1,
  "pipe": "\\\\.\\pipe\\zero-control"
}
```

| 字段 | 说明 |
|------|------|
| `active` | 当前活跃连接数（含本连接） |
| `pipe` | 管道名称（Windows）或对端地址（Unix） |

### ipc.disconnected

```json
{
  "active": 0,
  "pipe": "\\\\.\\pipe\\zero-control",
  "error": "BrokenPipe"
}
```

| 字段 | 说明 |
|------|------|
| `active` | 断开后的活跃连接数 |
| `pipe` | 管道名称或对端地址 |
| `error` | 异常断开时的错误信息（正常断开时为 null，不出现在 JSON 中） |

## 消费方式

IPC 和 SSE 的**事件 JSON 格式完全相同**（都是 `ApiEvent<P>` 信封），消费者只需一套解析代码。

GUI 的 IPC/HTTP/gRPC 连接和 EventDispatcher 都消费统一的 `EventSource` 语义，但前者面向交互式实时状态，后者面向 JSONL/Webhook 的持久投递。中心通过 Zero API/gRPC 的 `config.apply` 注册 Webhook，Connector 只负责把筛选后的 `zero.event.v1` envelope 投递到完整 URL 并处理 HTTP 确认。组件边界见 [Connector](./connector.md)。

| 方式 | 过滤 | 回放 | 格式 |
|------|------|------|------|
| SSE (`GET /api/v1/events/stream?types=...`) | event_type 白名单，`*` = 全部 | `?since=<seq>` / `Last-Event-ID`；实时阶段含 `flow.snapshot` 基线 | SSE frame: `id` + `event` + `data: <ApiEvent JSON>` |
| IPC (`{"type":"subscribe","events":[...]}`) | event_type 白名单，`*` = 全部 | 不回放历史事件；ACK 后发送 `flow.snapshot` 基线 | JSON line: `<ApiEvent JSON>\n` |
| CLI (`zero events`) | 无 | 不支持；启动时含 `flow.snapshot` 基线 | stdout: JSON line |
| Sink (`event_sinks[].events`) | event_type 白名单 | 持久投递生命周期增量；不接收 `flow.snapshot` | JSONL / Webhook |

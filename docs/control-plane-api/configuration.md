# 配置模型参考

控制面与事件投递配置位于 `api` 键下。本文档记录当前 API 配置字段。

完整的配置模型（inbounds、outbounds、route、runtime）请参阅 [config.md](../project/config.md)。

## 完整示例

```json
{
  "api": {
    "control": {
      "enabled": true,
      "listen": { "address": "127.0.0.1", "port": 9090 },
      "api_key": "sk-secret"
    },
    "hooks": [
      { "type": "ipc", "socket": "/run/billing/hook.sock", "timeout_ms": 100 }
    ],
    "event_sinks": [
      {
        "type": "jsonl",
        "tag": "audit",
        "path": "/var/log/zero/events.jsonl",
        "events": ["flow.completed", "engine.warning"]
      },
      {
        "type": "webhook",
        "tag": "billing",
        "url": "https://billing.example.com/events",
        "events": ["flow.completed"]
      }
    ],
    "outbox_path": "state/event-deliveries.jsonl",
    "dead_letter_path": "state/event-dead-letter.jsonl",
    "dispatcher": {
      "max_in_memory_deliveries": 4096,
      "replay_batch_size": 4096,
      "max_retry_attempts": 3,
      "retry_initial_delay_ms": 4000,
      "retry_max_delay_ms": 64000,
      "webhook_timeout_ms": 10000,
      "outbox_min_free_bytes": 1073741824,
      "outbox_min_free_percent": 5,
      "exhausted_delivery_policy": "retry_forever"
    }
  }
}
```

## `api.control`

本地 HTTP 控制接口。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|------|------|
| `enabled` | bool | `false` | 是否启动 HTTP 控制服务器 |
| `listen` | object | -- | 监听地址；`enabled=true` 时必填 |
| `listen.address` | string | -- | 绑定 IP，`127.0.0.1` 仅本地，`0.0.0.0` 公网 |
| `listen.port` | u16 | -- | 监听端口 |
| `api_key` | string | -- | Bearer token；启用控制面时与 `api_key_env` 二选一 |
| `api_key_env` | string | -- | 从环境变量读取 api_key；与 `api_key` 二选一 |
| `grpc` | object | -- | 可选 gRPC 传输与调用方认证策略；不改变 HTTP 控制端点 |
| `grpc.allow_insecure_remote` | bool | `false` | 允许 gRPC 在非 loopback 地址使用明文 HTTP/2 |
| `grpc.bearer_auth` | bool | `true` | gRPC 是否复用 `api_key` / `api_key_env` 进行 Bearer 认证 |
| `grpc.tls.cert_path` | string | -- | 原生 gRPC 服务端证书 PEM；相对路径以主配置目录为基准 |
| `grpc.tls.key_path` | string | -- | 原生 gRPC 服务端私钥 PEM；相对路径以主配置目录为基准 |
| `grpc.tls.client_ca_cert_path` | string | -- | 可选客户端 CA PEM；配置后启用 mTLS |

**CLI 覆盖**：`--status-listen 127.0.0.1:9090` 优先级高于配置文件。两者不能同时使用。

`config.apply` 不允许在线替换 `api.control` 的监听地址或鉴权来源。该命令正由现有控制面承载，在事务中自替换会产生失联和权限切换歧义，因此这类变更会在写入配置文件前被拒绝，需显式重启进程。其他可重建字段仍可热应用。

启用 `grpc-api` 时，gRPC 监听同一地址的下一端口。例如 HTTP 使用 `127.0.0.1:9090`，gRPC 使用 `127.0.0.1:9091`。控制端口为 `65535` 时无法分配伴随端口，启动会明确失败。

`api.control.grpc` 是可选配置，不会使仅使用 `status-api` 的 HTTP 部署承担 gRPC 约束。编译并实际启动 gRPC 时采用以下组合：

- 省略 `grpc`：loopback 明文 HTTP/2，并复用管理 Bearer；
- `grpc.tls`：原生服务端 TLS，可继续叠加 Bearer；
- `grpc.tls.client_ca_cert_path`：在 TLS 上要求客户端证书，即 mTLS；
- `grpc.bearer_auth: false`：关闭 gRPC Bearer；非 loopback 时必须由 mTLS 提供调用方认证；
- `grpc.allow_insecure_remote: true`：显式允许非 loopback 明文，适用于可信内网或外部 TLS 终止；不能与原生 `grpc.tls` 同时配置。

Bearer 与 TLS 解决的问题不同：Bearer 用于调用方认证，TLS 用于传输加密和服务端身份校验。Zero 支持明文、外部 TLS 终止、原生 TLS、原生 TLS + Bearer、mTLS，以及 mTLS + Bearer，不强制部署方选择其中某一种。除显式开启 `allow_insecure_remote` 外，非 loopback 明文会在配置或服务启动阶段 fail-closed。

原生 TLS 示例：

```json
{
  "enabled": true,
  "listen": { "address": "0.0.0.0", "port": 9090 },
  "api_key_env": "ZERO_NODE_API_KEY",
  "grpc": {
    "bearer_auth": true,
    "tls": {
      "cert_path": "managed/grpc/server.pem",
      "key_path": "managed/grpc/server-key.pem",
      "client_ca_cert_path": "managed/grpc/client-ca.pem"
    }
  }
}
```

`client_ca_cert_path` 可删除以使用普通服务端 TLS。若二进制同时启用 `status-api`，基础端口上的 HTTP 控制面仍是独立传输，不会自动继承 gRPC TLS；部署方应限制该端口的网络可达性，或在外部统一终止 TLS。这里保护的是控制面传输，证书、私钥等受管材料的内容级加密属于独立的材料事务边界。

### 限流

内置限流，无需配置：

| 类别 | 限制 | 响应 |
|------|------|------|
| 查询 (GET) | 100 req/s | 429 Too Many Requests |
| 命令 (POST) | 10 req/s | 429 Too Many Requests |
| SSE 并发 | 5 连接 | 429 Too Many Requests |

## `api.hooks`

Flow 生命周期钩子，按数组顺序执行。

```json
{ "type": "ipc", "socket": "/run/billing/hook.sock", "timeout_ms": 100 }
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|------|------|
| `type` | string | -- | 钩子类型，目前仅支持 `"ipc"` |
| `socket` | string | -- | IPC socket 路径 |
| `timeout_ms` | u64 | `100` | 请求超时（毫秒）；超时则 fail-open 放行 |

**CLI 覆盖**：`--ipc-hook-socket /run/billing/hook.sock` 优先级高于配置文件。

钩子协议详情：参见 [hooks.md](./hooks.md)。

## `api.event_sinks`

事件投递目标数组。

生产事件投递建议同时配置 `api.outbox_path`。每个 sink 使用独立 worker，一个接收端超时不会阻塞其他注册地址。Dispatcher 参数如下：

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `max_in_memory_deliveries` | `4096` | 活跃内存工作集；其余持久 delivery 留在 outbox |
| `replay_batch_size` | `4096` | 每轮从 engine event log 补偿 live queue 断档的最大事件数 |
| `max_retry_attempts` | `3` | 首次投递失败后最多重试次数 |
| `retry_initial_delay_ms` | `4000` | 首次退避 |
| `retry_max_delay_ms` | `64000` | 指数退避上限 |
| `webhook_timeout_ms` | `10000` | 单次 Webhook 请求超时 |
| `outbox_min_free_bytes` | `1073741824` | outbox 所在文件系统必须保留的绝对可用空间 |
| `outbox_min_free_percent` | `5` | outbox 所在文件系统必须保留的可用空间比例 |
| `exhausted_delivery_policy` | `retry_forever` | `retry_forever`、`dead_letter` 或 `discard` |

所有数值必须大于零，且初始退避不得大于退避上限；`outbox_min_free_percent` 必须在 1–50 之间。磁盘保护的有效水位取绝对值与文件系统总容量比例中的较大值。达到水位后，dispatcher 暂停新的 outbox PUT，不推进对应事件游标；已有 delivery 的投递、ACK 和压缩可以使用保留空间继续排空，但仍会保留有效水位 25%（至少 64 MiB、且不超过有效水位）的紧急维护空间。`GET /api/v1/sinks` 的 `outbox_storage` 会报告总容量、可用空间、有效保留水位、紧急维护水位和 `write_blocked` 状态。

`dead_letter` 策略要求同时配置 `api.dead_letter_path`。默认 `retry_forever` 会在达到阈值后继续按有界退避重试，不会隐式确认或删除可重试事件。

`/api/v1/sinks` 的 `pending` 是 outbox 中全部未 ACK delivery 数，不只是当前内存页。若 outbox 写入失败，dispatcher 不会把该 delivery 当作已持久化成功；replay cursor 保留在缺口之前，并通过 `last_error` 暴露故障。若 engine event log 在磁盘恢复前已淘汰缺口，状态会记录 replay gap，需要人工对账。

### JSON Lines 文件

```json
{
  "type": "jsonl",
  "tag": "audit",
  "path": "/var/log/zero/events.jsonl",
  "events": ["flow.completed"],
  "source_id": "node-001"
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|------|------|
| `type` | string | -- | `"jsonl"` |
| `tag` | string | -- | 唯一标识 |
| `path` | string | -- | 文件路径；相对路径相对于配置目录解析 |
| `events` | string[] | `[]` | 事件类型白名单；空 = 接收所有 |
| `source_id` | string | -- | 覆盖事件 source_id |

### Webhook

```json
{
  "type": "webhook",
  "tag": "billing",
  "url": "https://example.com/events",
  "events": ["flow.completed"],
  "source_id": "node-001",
  "headers": {
    "authorization": "Bearer receiver-defined-token"
  }
}
```

| 字段 | 类型 | 默认值 | 说明 |
|------|------|------|------|
| `type` | string | -- | `"webhook"` |
| `tag` | string | -- | 唯一标识 |
| `url` | string | -- | 接收方提供的完整 URL；Zero 不拼接路径 |
| `events` | string[] | `[]` | 事件类型白名单 |
| `source_id` | string | -- | 覆盖事件来源标识 |
| `headers` | object | `{}` | 接收方定义的不透明 HTTP headers |
| `allow_insecure` | bool | `false` | 允许明文 `http://`（仅测试用） |

中心可通过 HTTP `/api/v1/commands` 或 gRPC `Control.Execute` 提交 `config.apply` 热注册、更新或移除 sink。任意 `2xx` 表示确认，`429`、`5xx` 和网络错误可重试，其他状态不可重试；响应正文不参与协议。详见 [Connector](./connector.md)。

## `api.dead_letter_path`

死信队列文件路径。不可重试事件会写入此文件；可重试事件只有在 `exhausted_delivery_policy` 为 `dead_letter` 且达到阈值后才进入此文件。

| 字段 | 类型 | 说明 |
|------|------|------|
| `dead_letter_path` | string | 死信 JSON Lines 文件路径 |

死信文件格式：每行一个 JSON 对象，包含 `dead_lettered_at_unix_ms` 和 `original_event`。

## `api.outbox_path`

持久投递 journal。配置后，dispatcher 会在调用 Sink 之前同步写入每个 `(sink_tag, event_id)` delivery；Sink 返回已交付后写入 ACK。进程退出或异常重启时，未 ACK delivery 会从 journal 恢复并重投。

```json
{
  "api": {
    "outbox_path": "state/event-outbox.jsonl",
    "dead_letter_path": "state/event-dead-letter.jsonl"
  }
}
```

Webhook 接收端必须用 `event_id` 建立唯一约束并幂等返回 `2xx`。这样 ACK 写入前崩溃导致的 at-least-once 重投不会重复处理。可重试 delivery 达到阈值后的行为由 `exhausted_delivery_policy` 明确决定。

### 投递状态查询

```bash
zero status  # 包含 sink 投递统计
```

## 相关运行时字段

以下配置字段位于 `api` 部分之外，但可通过 `GET /api/v1/config` 获取，与控制面消费者相关。

| 字段 | 位置 | 说明 |
|------|------|------|
| `idle_timeout_secs` | `inbounds[*]` | TCP 中继空闲超时（秒，默认 300） |
| `url_rewrite` | `route.url_rewrite[]` | 路由前的域名重写规则（`from` / `from_regex` -> `to`） |
| `domain_regex` | `route.rules[*].condition` | 按正则表达式匹配域名的条件类型 |
| `up_bps` / `down_bps` | `inbounds[*].protocol`（Hysteria2、Shadowsocks、Trojan） | 每入站的 GCRA 速率限制 |

完整详情参见 [config.md](../project/config.md)。

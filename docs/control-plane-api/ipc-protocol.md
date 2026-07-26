# IPC 协议

本地进程间通信使用 JSON-line 帧协议，底层传输在 Unix 上为 Domain Socket，Windows 上为 Named Pipe。协议语义完全一致。

## 连接

| 平台 | 默认路径 | 传输 |
|------|---------|------|
| Linux/macOS | `~/.zero/control.sock` | Unix Domain Socket (0600) |
| Windows | `\\.\pipe\zero-control` | Named Pipe |
| CLI 覆盖 | `--control-socket /path/to/sock` | |

## 帧格式

每条帧是一个完整的 JSON 对象，以 `\n` 结尾。支持在同一条连接上多路复用多个请求（请求-响应用 `id` 字段配对），也可以只发一个请求后关闭连接（`id` 可省略）。

> **`id` 为不透明关联令牌**：接受数字或字符串（如 `1`、`"req-123"`、`"znet-sink-1"`、UUID），服务端原样回显，既不解析也不做数值运算——客户端可自由选择配对方案。下方示例统一用数字，仅作示意。

```
→ {"type":"query","id":1,"request":{"health":{}}}\n
← {"api_id":"zero.api.v1","ok":true,"id":1,"result":{"engine_build_id":"build-id",...}}\n
```

## 请求类型

### Ping

```
→ {"type":"ping"}
← {"api_id":"zero.api.v1","ok":true,"result":"pong"}
```

带 `id` 的多路复用形式：
```
→ {"type":"ping","id":42}
← {"api_id":"zero.api.v1","ok":true,"id":42,"result":"pong"}
```

### Query

`request` 字段是 `QueryRequest` 枚举，使用 **externally-tagged** 格式（serde 默认）。每个变体名是 snake_case，值是查询参数对象（无参数时为空对象 `{}`）。

```
→ {"type":"query","id":1,"request":{"health":{}}}
← {"api_id":"zero.api.v1","ok":true,"id":1,"result":{...}}
```

> **关键**：`request` 字段必须是一个 JSON 对象，包含一个变体名作为 key。不要用字符串（`"request":"runtime"` 是错的），不要用 `{"type":"runtime"}` 格式（那也是错的）。

| request | 说明 |
|---------|------|
| `{"capabilities":{}}` | 能力查询 |
| `{"health":{}}` | 健康检查 |
| `{"config":{}}` | 配置快照 |
| `{"runtime":{}}` | 运行时状态（含统计、日志配置、活动流） |
| `{"stats":{}}` | 统计摘要 |
| `{"active_flows":{"limit":100,"filter":{}}}` | 活动流列表 |
| `{"recent_flows":{"limit":100,"filter":{}}}` | 既有有限诊断窗口（兼容接口，不应作为 GUI 历史数据库） |
| `{"flow":{"flow_id":"42"}}` | 单流详情 |
| `{"policies":{}}` | 所有策略 |
| `{"policy":{"policy_tag":"proxy"}}` | 单个策略 |
| `{"diagnostics":{}}` | 诊断信息 |
| `{"sinks":{}}` | 事件接收器状态 |
| `{"tun_status":{}}` | TUN 虚拟网卡状态 |

连接页的新实现应订阅 flow 生命周期事件，以 `flow.snapshot` 重建活动连接，并在本地消费 `flow.completed` 维护历史。`recent_flows` 只保留为既有诊断兼容面，不用于断线重建或持久历史。

### Command

```
→ {"type":"command","id":1,"method":"policies.select","params":{"policy_tag":"proxy","target_tag":"direct"}}
← {"api_id":"zero.api.v1","ok":true,"id":1,"result":{"accepted":true,"result":{"policy_tag":"proxy","selected":"direct"}}}
```

`id` 可选，与 Query 一致。

支持的方法：

| method | params | 说明 |
|--------|--------|------|
| `policies.select` | `policy_tag`, `target_tag` | 切换 selector 出站 |
| `policies.probe` | `policy_tag` | 探测 url_test 组延迟（异步，结果经事件/查询取） |
| `flows.close` | `flow_id` | 关闭指定流 |
| `config.validate` | `config` (完整 JSON) | 验证配置 |
| `config.apply` | `config` (完整 JSON) | 持久化并等待 proxy 与进程级服务热重建；失败回滚 |
| `config.apply_runtime` | `config` (完整 JSON) | 不写回源文件，并等待 proxy 与进程级服务热重建；失败回滚 |
| `mode.set` | `mode`, `outbound?` | 设置全局模式 |
| `tun.start` | `name?`, `addr`, `mask?`, `mtu?`, `tag` | 启动 TUN |
| `tun.stop` | — | 停止 TUN |
| `diagnostics.probe_target` | `target_tag` | 直连 TCP 可达性（不走代理，仅本机→server:port RTT） |
| `diagnostics.probe_outbound` | `target_tag`, `url?` | 同步经代理单节点延迟；全局 `runtime.latency_test_url` 优先 |
| `diagnostics.dns_lookup` | `hostname` | DNS 查询 |
| `diagnostics.trace_route` | `target`, `port`, `protocol?`, `inbound_tag?` | 路由追踪 |

> **实现说明：** IPC Command 和 HTTP `POST /api/v1/commands` 共用同一条 serde 反序列化路径（`CommandRequest` 的 `#[serde(tag = "method", content = "params")]`）。新增 command 只需修改 `zero_api::CommandRequest`，传输层无需单独适配。

### Subscribe

```
→ {"type":"subscribe","id":1,"events":["flow.started","flow.routed","flow.updated","flow.completed"]}
← {"api_id":"zero.api.v1","ok":true,"id":1,"result":"subscribed"}
← {"schema_id":"zero.event.v1","event_type":"flow.snapshot","payload":{"watermark":1024,"records":[...]}}
← {"schema_id":"zero.event.v1","event_id":"...","event_type":"flow.completed","occurred_at_unix_ms":1713500000000,...}
← :\n
← ...持续推送...
```

`id` 可选，与 Query/Command 一致。

`events` 为可选的事件类型白名单，空或省略表示接收所有事件。

只要白名单包含任一 flow 生命周期事件，ACK 后的第一条 flow 事件就是 `flow.snapshot`。即使白名单没有显式列出 `flow.snapshot`，服务端也会发送该同步基线。客户端应先用 `records` 替换活动连接集合，再按 `flow_id` + `revision` 合并后续 `flow.started`、`flow.routed`、`flow.updated` 和 `flow.completed`。

Subscribe 的确认帧是 `ApiResponse`：一定包含 `api_id` 和 `ok`；`id` 只是请求关联 ID，只有请求带了 `id` 才会回显。后续事件帧是裸 `ApiEvent`：包含 `schema_id`、`event_id`、`event_type`，不包含 `api_id` 和 `ok`。客户端应使用顶层 `ok` 是否存在区分响应帧和事件帧，不应使用 `id` 区分。

**事件格式与 SSE 完全相同** — 都是 `zero_api::ApiEvent<serde_json::Value>` JSON。消费者只需要一套解析代码即可同时消费 IPC 和 HTTP/SSE 两个通道的事件。事件信封详见 [events.md](./events.md)。

连接保持期间服务端持续推送事件帧，同时可以继续发送 Query/Command/Ping 帧获取即时响应。心跳用 `:\n`（SSE 注释格式，客户端忽略）。

## 响应格式

IPC 响应使用统一信封格式（`zero_api::ApiResponse`），包含 `api_id` 字段用于协议标识。

```json
{
  "api_id": "zero.api.v1",
  "ok": true,
  "id": 1,
  "result": { }
}
```

- `api_id` — 协议标识，始终为 `"zero.api.v1"`
- `id` — 回显请求的 `id` 字段，用于多路复用时配对。请求不带 `id` 时此字段**省略**（JSON key 不存在）。请求 JSON 完全无法解析时此字段同样省略（服务端无法提取 `id`）
- `ok` — `true` 成功 / `false` 失败
- `result` — 成功时的响应数据（失败时省略）
- `error` — 失败时的错误详情（成功时省略）

### Query 响应的 result 格式

`result` 字段包含 **externally-tagged** 的 `QueryResponse` 枚举——一个变体名 key 包裹内部数据。每个变体名与请求变体名一致：

```json
{
  "api_id": "zero.api.v1",
  "ok": true,
  "id": 1,
  "result": {
    "health": {
      "engine_build_id": "build-id",
      "started_at_unix_ms": 1713500000000,
      "healthy": true
    }
  }
}
```

访问路径：`response.result.health.engine_build_id`

各变体名及内部数据结构：

| QueryResponse 变体 | result 内部 key | 内部数据 |
|---|---|---|
| `QueryRequest::Health` | `"health"` | `{engine_build_id, started_at_unix_ms, healthy}` |
| `QueryRequest::Config` | `"config"` | `{mode, rule_count, listeners, outbounds, outbound_groups}` |
| `QueryRequest::Runtime` | `"runtime"` | `{stats, log_level, active_sessions, ...}` |
| `QueryRequest::Stats` | `"stats"` | `{active_sessions, total_started, bytes_up, bytes_down, ...}` |
| `QueryRequest::ActiveFlows` | `"active_flows"` | `[flow, ...]` |
| `QueryRequest::Policies` | `"policies"` | `[policy, ...]` |
| `QueryRequest::Policy` | `"policy"` | `{tag, kind, outbounds, selected, ...}` |
| `QueryRequest::Diagnostics` | `"diagnostics"` | `{healthy, active_sessions, ...}` |
| `QueryRequest::Sinks` | `"sinks"` | `{sinks: [{name, pending, total_delivered, total_failed, replay_gaps, ...}]}` |
| `QueryRequest::TunStatus` | `"tun_status"` | `{running, name, addr, tag}` |

> **注意：** 这是 IPC 通道的格式。HTTP 通道的 `result` 字段**不包含**变体名 key——直接就是内部数据。例如 HTTP `GET /api/v1/health` 返回 `result: {"engine_build_id":"build-id",...}`，而 IPC 返回 `result: {"health":{"engine_build_id":"build-id",...}}`。

错误响应（`result` 和 `id` 在不存在时均省略，而非 `null`）：
```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "id": 1,
  "error": {
    "code": "not_found",
    "message": "flow `42` was not found",
    "field_path": "flow_id"
  }
}
```

> **注意：** 错误码使用 snake_case（如 `not_found`），与 HTTP 响应格式完全一致。

## 多路复用

同一条连接上发 subscribe 后**不关闭连接**，继续发送 Query/Command/Ping 帧：

```
→ {"type":"subscribe","id":1,"events":["flow.started","flow.routed","flow.updated","flow.completed"]}
← {"api_id":"zero.api.v1","ok":true,"id":1,"result":"subscribed"}
← {"schema_id":"zero.event.v1","event_type":"flow.snapshot","payload":{"watermark":1024,"records":[...]}}
← {"schema_id":"zero.event.v1","event_type":"flow.completed",...}
→ {"type":"query","id":2,"request":{"stats":{}}}
← {"api_id":"zero.api.v1","ok":true,"id":2,"result":{"stats":{"active_sessions":3,...}}}
→ {"type":"ping","id":3}
← {"api_id":"zero.api.v1","ok":true,"id":3,"result":"pong"}
← {"schema_id":"zero.event.v1","event_type":"flow.completed",...}
```

这样只需要一条持久连接即可承载所有通信，无需为 query/command 单独创建短期连接。

## 内核日志

IPC server 在以下事件输出结构化日志，每条日志携带 `active=N` 表示当前活跃连接数：

| 事件 | 级别 | 示例 |
|------|------|------|
| 客户端连接 | `info` | `ipc client connected active=1` |
| 客户端正常断开 | `info` | `ipc client disconnected cleanly active=0` |
| 客户端异常断开 | `warn` | `ipc client disconnected error=BrokenPipe active=0` |
| 连接处理失败 | `warn` | `ipc connection failed error=... active=0` |
| 连接 task panic | `error` | `ipc connection task panicked` |
| 服务端就绪 | `info` | `ipc server ready pipe=\\.\pipe\zero-control` |
| 服务端停止 | `info` | `ipc server stopped` |
| Pipe connect 失败 | `warn` | `named pipe connect failed error=...` |
| Pipe create 失败 | `error` | `failed to create named pipe error=...` |

通过设置 `RUST_LOG` 控制日志级别：
```bash
RUST_LOG=zero=debug zero run config.json     # 调试级别
RUST_LOG=zero=warn zero run config.json      # 仅警告和错误
```

诊断 pipe 实例耗尽问题时，观察 `active=N` 是否持续增长（无减）。正常情况下连接数和断开数应平衡。

## 客户端示例

### CLI

```bash
zero status
zero select proxy direct
zero flows
zero policies
zero events
```

### Python（Unix）

```python
import json, socket, os

SOCK = os.path.expanduser("~/.zero/control.sock")

def ipc_request(req):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(SOCK)
    s.sendall((json.dumps(req) + "\n").encode())
    resp = b""
    while b"\n" not in resp:
        resp += s.recv(4096)
    s.close()
    return json.loads(resp.split(b"\n")[0])

# 查询健康状态（注意 request 格式：externally-tagged）
health = ipc_request({"type": "query", "id": 1, "request": {"health": {}}})
print(f"Build: {health['result']['health']['engine_build_id']}")

# 查询策略列表
policies = ipc_request({"type": "query", "id": 2, "request": {"policies": {}}})
for p in policies['result']['policies']:
    print(f"  {p['tag']} ({p['kind']}) → {p.get('selected', '-')}")

# 切换 selector
ipc_request({
    "type": "command",
    "id": 3,
    "method": "policies.select",
    "params": {"policy_tag": "proxy", "target_tag": "direct"}
})
```

### Python（Windows）

```python
import json

PIPE = r"\\.\pipe\zero-control"

def ipc_request(req):
    # Windows Named Pipe 用普通文件操作即可
    with open(PIPE, "r+b") as f:
        f.write((json.dumps(req) + "\n").encode())
        f.flush()
        resp = b""
        while b"\n" not in resp:
            chunk = f.read(4096)
            if not chunk:
                break
            resp += chunk
        return json.loads(resp.split(b"\n")[0])

# 用法与 Unix 示例完全相同
health = ipc_request({"type": "query", "id": 1, "request": {"health": {}}})
print(f"Build: {health['result']['health']['engine_build_id']}")
```

### Go（Unix）

```go
package main

import (
    "bufio"
    "encoding/json"
    "net"
    "os"
)

func ipcRequest(req map[string]any) map[string]any {
    conn, _ := net.Dial("unix", os.Getenv("HOME")+"/.zero/control.sock")
    defer conn.Close()

    data, _ := json.Marshal(req)
    conn.Write(append(data, '\n'))

    line, _ := bufio.NewReader(conn).ReadString('\n')
    var resp map[string]any
    json.Unmarshal([]byte(line), &resp)
    return resp
}

func main() {
    health := ipcRequest(map[string]any{
        "type":    "query",
        "id":      1,
        "request": map[string]any{"health": map[string]any{}},
    })
    _ = health
}
```

### Node.js / Electron（Unix + Windows）

```javascript
const net = require('net');
const os = require('os');
const path = require('path');

// Unix 和 Windows 自动切换
const SOCK = process.platform === 'win32'
  ? '\\\\.\\pipe\\zero-control'
  : path.join(os.homedir(), '.zero', 'control.sock');

function ipcRequest(req) {
  return new Promise((resolve, reject) => {
    const client = net.createConnection(SOCK, () => {
      client.write(JSON.stringify(req) + '\n');
    });
    client.on('data', (data) => {
      client.destroy();
      const parsed = JSON.parse(data.toString().split('\n')[0]);
      resolve(parsed);
    });
    client.on('error', reject);
  });
}

// 查询运行时状态
const resp = await ipcRequest({ type: 'query', id: 1, request: { runtime: {} } });
console.log(`活跃连接: ${resp.result.runtime.stats.active_sessions}`);

// 查询策略列表
const policies = await ipcRequest({ type: 'query', id: 2, request: { policies: {} } });
for (const p of policies.result.policies) {
  console.log(`  ${p.tag} (${p.kind}) → ${p.selected ?? '-'}`);
}

// 切换 selector
await ipcRequest({
  type: 'command',
  id: 3,
  method: 'policies.select',
  params: { policy_tag: 'proxy', target_tag: 'direct' }
});
```

## 错误处理

### IPC 连接不存在

Zero 未启动时，socket/pipe 不存在，连接会失败：

```javascript
// Node.js
client.on('error', (err) => {
  if (err.code === 'ENOENT' || err.code === 'ECONNREFUSED') {
    console.log('Zero is not running');
  }
});
```

### Subscribe 后的帧顺序

Subscribe 请求的第一条响应是确认帧（`ok:true, result:"subscribed"`，包含在 `ApiResponse` 信封中）。之后才是事件帧（裸 `ApiEvent` JSON，无信封包裹）。消费者需要区分这两种帧：

```
← {"api_id":"zero.api.v1","ok":true,"id":1,"result":"subscribed"}   ← 确认帧（ApiResponse 信封）
← {"schema_id":"zero.event.v1","event_type":"flow.snapshot",...}     ← 活动连接基线（裸 ApiEvent）
← {"schema_id":"zero.event.v1","event_type":"flow.completed",...}    ← 生命周期事件（裸 ApiEvent）
```

如果订阅不包含任何 flow 类型，则不会生成 `flow.snapshot`。重新连接并订阅后会得到一份新的活动快照；已经完成的连接由 GUI/消费者自行保存，内核不会通过快照重放历史。

判断方法：检查顶层 `ok` 字段。存在且为 `true`/`false` → 响应帧；不存在 → 事件帧。`api_id` 标识响应信封，`schema_id` 标识事件信封。`id` 只是请求关联 ID，不是帧类型判别字段；如果 subscribe 请求没有传 `id`，确认帧也不会包含 `id`。

### 命令执行失败

命令失败时 `ok` 为 `false`，`error` 包含错误详情：

```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "id": 2,
  "error": {
    "code": "not_found",
    "message": "policy `nonexistent` was not found",
    "field_path": "policy_tag"
  }
}
```

错误码列表：`not_found`, `invalid_argument`, `permission_denied`, `feature_disabled`, `conflict`, `unsupported`, `internal`。

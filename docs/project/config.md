# 配置

Zero 使用 JSON。当前顶层结构如下：

```json
{
  "inbounds": [],
  "outbounds": [],
  "outbound_groups": [],
  "runtime": {
    "udp_upstream_idle_timeout_seconds": 30,
    "latency_test_url": "http://www.gstatic.com/generate_204",
    "principal_quota_state_path": "state/principal-quotas.json",
    "network": { "mtu": 1500 },
    "dns": {
      "servers": [{ "type": "system" }],
      "cache": { "max_entries": 256 },
      "routes": [],
      "fake_ip": null
    }
  },
  "api": {
    "event_sinks": [],
    "control": { "enabled": false },
    "hooks": []
  },
  "mode": { "type": "rule" },
  "route": {
    "rule_sets": [],
    "rules": [],
    "url_rewrite": [],
    "final": { "type": "direct" }
  }
}
```

这里仅记录当前已实现的配置。模式和节点组的长期设计见 [modes-and-groups.md](modes-and-groups.md)。

> Connector Webhook 作为 `api.event_sinks` 配置，不存在独立顶层 `push`。外部控制器通过 Zero API/gRPC 的 `config.apply` 注册完整 URL。详见 [Connector 通信边界](connector-architecture.md) 与 [配置模型参考](../control-plane-api/configuration.md#api-event-sinks)。

## runtime

`runtime.udp_upstream_idle_timeout_seconds` 控制上游 `SOCKS5` UDP 关联的空闲超时。

`runtime.event_log_capacity` 控制内核为事件回放保留的环形日志条数，默认 `1024`。
该值必须大于 `0`；运行时 reload 修改后会立即裁剪现有事件历史。

- 默认值：`30`
- 单位：秒
- 约束：必须大于 `0`

`runtime.latency_test_url` 是全局经代理延迟测速地址，仅支持 `http://`。未配置时默认使用
`http://www.gstatic.com/generate_204`。普通单节点测速统一使用该地址；`url_test` 组设置了
自己的 `url` 时优先使用组内地址，否则继承全局地址。

`runtime.principal_quota_state_path` 是可选的主体共享余额恢复快照。配置后，限额用户的当前 revision、初始余额与剩余字节会在独立后台线程中合并写入原子快照；relay I/O 不等待磁盘。正常停机强制写入最终快照，重启按相同 `principal_key + policy_revision + initial_bytes` 恢复。活动流量以约 50ms 合并窗口落盘，主机突然断电可能损失最近一个合并窗口；若后台持久化失败，后续限额用户 admission 会 fail closed。该路径不能在 live reload 中修改。

`runtime.network.mtu` 控制用户态网络栈的 MTU，并将同一数值传给 TUN 后端配置接口；默认
`1500`，最小 `576`。
启动 TUN 时若 `tun.start`/CLI 显式传入 `mtu`，该值覆盖全局配置；否则使用这里的值。
网络栈会按 MTU 扣除 IPv6 与 TCP 头部后计算通告 MSS，避免生成超过配置 MTU 的 TCP 包。
当前 Windows/Linux/macOS TUN 后端的系统网卡配置实现仍是空操作，因此如果还需要同步修改
操作系统网卡本身的 MTU，宿主侧仍需配置对应接口；本参数已经完整控制 Zero 用户态网络层。

### DNS

`runtime.dns` 是可选的 DNS 子系统配置。省略时使用系统解析器，行为不变。

```json
{
  "dns": {
    "servers": [
      { "type": "system" },
      { "type": "udp", "address": "8.8.8.8", "port": 53 }
    ],
    "cache": { "max_entries": 512, "max_ttl_seconds": 300 },
    "routes": [
      { "domain": "*.internal.local", "server": "system" },
      { "domain": "*.google.com", "server": "1" }
    ],
    "fake_ip": {
      "cidr": "198.18.0.0/15",
      "ttl_seconds": 86400,
      "exclude_domains": ["*.local"]
    }
  }
}
```

**servers** -- 有序 DNS 服务器列表。解析时并发查询所有服务器，最先返回的响应胜出。

| 类型 | 字段 | 描述 |
|------|------|------|
| `system` | -- | OS 解析器 (getaddrinfo) |
| `udp` | `address`, `port` | 纯 UDP DNS，默认端口 53 |
| `doh` | `url`, `server_name` | DNS-over-HTTPS (v2) |
| `dot` | `address`, `port`, `server_name` | DNS-over-TLS (v2)，默认端口 853 |

**cache** -- 基于 TTL 的 LRU 缓存。

| 字段 | 默认值 | 描述 |
|------|------|------|
| `max_entries` | `256` | 最大缓存条目数 |
| `max_ttl_seconds` | -- | TTL 上限；省略则使用 DNS 记录的 TTL |

**routes** -- 域名到服务器的路由。`domain` 支持精确匹配 (`example.com`) 和通配符 (`*.example.com`)。`server` 为 `"system"` 或服务器数组索引 (`"0"`, `"1"`)。

**fake_ip** -- 透明代理核心。对匹配的域名返回虚假 IP，维护域名到虚假 IP 的映射，在连接时反向解析为真实域名以进行路由。

| 字段 | 默认值 | 描述 |
|------|------|------|
| `cidr` | -- | 虚假 IP 池 CIDR，推荐 `198.18.0.0/15` |
| `ttl_seconds` | `86400` | 虚假 IP 分配生命周期 |
| `exclude_domains` | `[]` | 排除的域名，使用真实 DNS |

## api

`api` 是可选的管控面和可观测性配置。相关运行时能力由 Cargo features 控制；配置的存在不保证默认编译支持。

### event_sinks

`api.event_sinks` 描述归一化事件的投递目标。事件类型必须来自 [api.md](api.md) 中的事件目录。

本地 JSON Lines：

```json
{
  "tag": "local-events",
  "type": "jsonl",
  "path": "zero-events.jsonl",
  "events": ["flow.completed"],
  "source_id": "edge-local"
}
```

通用 Webhook：

```json
{
  "tag": "central-events",
  "type": "webhook",
  "url": "https://central.example/receivers/zero/node-17",
  "events": ["flow.completed", "engine.warning"],
  "source_id": "edge-shanghai-01",
  "headers": {
    "authorization": "Bearer receiver-defined-token"
  }
}
```

`url` 是注册方提供的完整地址，Zero 不拼接路径。`headers` 是不透明字段，Zero 不规定认证方案。`http://` webhook 需要显式 `allow_insecure: true`。

当 `event-dispatcher` feature 已编译且 `api.event_sinks` 不为空时，应用层启动一个 dispatcher owner 负责投递生命周期，并向管控面暴露只读 sink 状态。`GET /api/v1/sinks` 报告 `pending` 积压、投递计数器、最近成功/失败时间戳和最近错误文本。

生产计费 Sink 应同时配置 `api.outbox_path`。dispatcher 会先同步写入 delivery journal，再调用 Sink；成功后写 ACK，重启时恢复未 ACK 记录。每个 sink 使用独立 worker，单个接收端超时不会阻塞其他注册地址。`api.dispatcher` 配置活跃内存工作集、event-log replay 批量、Webhook timeout、重试阈值、初始/最大退避、耗尽策略，以及 outbox 文件系统的绝对/比例空闲保留水位；默认 `retry_forever` 不会隐式删除可重试 delivery。水位不足时新的持久投递 fail-closed 且不推进事件游标，已有 delivery 仍可投递和 ACK。`GET /api/v1/sinks` 的 `pending` 统计完整 durable backlog，`outbox_storage` 报告磁盘保护状态。不可重试 delivery 或显式 `dead_letter` 策略使用 `api.dead_letter_path`。接收端必须按 `event_id` 幂等。

### control

`api.control` 使面板能够主动查询节点和下发命令。默认关闭，启用时需要 API key：

```json
{
  "enabled": true,
  "listen": { "address": "127.0.0.1", "port": 9090 },
  "api_key_env": "ZERO_NODE_API_KEY"
}
```

当前管控面使用 `Authorization: Bearer <api-key>` 或 `X-Zero-Api-Key: <api-key>`。建议仅监听 localhost、内网或受防火墙保护的地址。

编译 `grpc-api` 时，gRPC 使用同一监听 IP 的下一端口。`control.grpc` 可选配置原生 TLS、客户端 CA（mTLS）、Bearer 开关或显式远程明文；Bearer 与 mTLS 可以单独使用或叠加。默认远程明文不会启动，外部 TLS 终止仍受支持。详细字段与示例见 [控制面配置](../control-plane-api/configuration.md#api-control)。

当前 HTTP 管控面支持：

```text
GET  /api/v1/capabilities
GET  /api/v1/health
GET  /api/v1/config
GET  /api/v1/runtime
GET  /api/v1/stats
GET  /api/v1/flows
GET  /api/v1/policies
GET  /api/v1/sinks
GET  /api/v1/events
POST /api/v1/commands
```

`POST /api/v1/commands` 使用统一的 command JSON，例如：

```json
{
  "method": "policies.select",
  "params": {
    "policy_tag": "proxy",
    "target_tag": "direct"
  }
}
```

## Inbounds

每个入站必须包含 `tag`、`listen` 和 `protocol`。可选的 `idle_timeout_secs` 字段控制 TCP 空闲超时。

```json
{
  "tag": "mixed-in",
  "listen": { "address": "127.0.0.1", "port": 7890 },
  "protocol": { "type": "mixed" },
  "idle_timeout_secs": 300
}
```

| 字段 | 类型 | 默认值 | 描述 |
|------|------|------|------|
| `tag` | string | (必填) | 唯一入站标识符 |
| `listen.address` | string | (必填) | 绑定地址 |
| `listen.port` | u16 | (必填) | 绑定端口 |
| `protocol` | object | (必填) | 协议特定配置 |
| `idle_timeout_secs` | u64 | `300` | TCP 中继空闲超时，单位秒 |

### idle_timeout_secs

内核将每个 TCP 中继包裹在 `tokio::time::timeout` 中。如果在 `idle_timeout_secs` 内任一方向没有数据传输，会话将被干净地终止。这是按入站配置的；不同的监听器可以有不同的超时时间。省略该字段时使用内核默认值 300 秒（5 分钟）。

### 当前支持的协议

- `socks5`
- `http`
- `mixed` -- 同一端口自动检测 SOCKS5 和 HTTP CONNECT；SOCKS5 分支支持 TCP CONNECT 和 UDP ASSOCIATE，HTTP CONNECT 分支仅支持 TCP
- `vless` -- TCP/TLS/WS/WSS, Reality, gRPC, H2, HTTPUpgrade, XHTTP（原 SplitHTTP，`mode` 默认 `stream-one` 单连接最终跳；`quic` 已被 XTLS 弃用）；MUX + Vision flow + UDP over TCP
- `hysteria2` -- QUIC，TCP 流和 UDP datagram 转发
- `shadowsocks` -- AEAD TCP 流和 UDP datagram 支持
- `trojan` -- TLS + SHA224 密码认证，TCP 流和 UDP 数据包中继
- `vmess` -- TCP 流、TCP/UDP MUX 和基于内置 VMess AEAD 实现的 UDP-over-stream；`cipher: auto` 被规范化为当前 AEAD 基线
- `mieru` -- TCP 流和 UDP 数据包中继，使用 XChaCha20-Poly1305 会话帧封装
- `direct` -- 固定目标 TCP 转发器；接受无握手原始 TCP，出站由常规路由规则确定
- `tun` -- 虚拟网络接口；通过 CLI/API 命令在运行时启动，流量经过常规规则匹配进行路由

`mixed` 不是外部协议，而是同端口入站多路复用的配置条目。它将 SOCKS5 TCP CONNECT 和 SOCKS5 UDP ASSOCIATE 分发到 SOCKS5 运行时路径，将 HTTP CONNECT 分发到 HTTP TCP 运行时路径。

`mieru` 已在协议清单中注册，单跳出站路径使用加密的 Mieru 流封装器。它暂不支持作为中间 `relay` 链跳跃节点，因为该路径必须在跳跃握手之后将活跃流替换为 Mieru 加密封装器。

`vmess` 仍处于实验阶段。来自 Xray/Clash 导出的 `cipher: auto` 被接受并规范化为当前 AEAD 基线。TCP/UDP MUX 已实现。外部 TCP 和 UDP 基线互操作性已覆盖：Xray 双向、Zero 出站到 sing-box 入站、Mihomo 出站到 Zero 入站。Xray WS/gRPC TCP 传输互操作性已覆盖双向。

### Direct 入站

`direct` 入站监听端口，接受无协议握手的原始 TCP 连接，并将所有流量通过常规路由规则转发。目标地址来自入站配置而非客户端。出站选择遵循标准路由管道 -- `mode`、`rules`、`rule_sets` 和 `final`。

```json
{
  "tag": "direct-in",
  "listen": { "address": "127.0.0.1", "port": 8080 },
  "protocol": {
    "type": "direct",
    "target": "example.com",
    "port": 443
  }
}
```

Direct 入站配置字段：
- `target` -- 可选，转发连接的目标地址（IP 或域名）；运行时必须存在（默认无）
- `port` -- 可选，目标端口，默认 `443`

### TUN 入站

`tun` 是虚拟网络接口入站。与其他入站不同，它不在静态 JSON 配置中声明，而是通过 CLI、IPC 或 HTTP 管控面命令在运行时启动和停止。

```bash
# 启动 TUN 设备
zero tun start --addr 10.0.0.1 --mask 255.255.255.0 --tag my-tun --name tun0

# 查看 TUN 状态
zero tun status

# 停止 TUN 设备
zero tun stop
```

HTTP 管控面等价命令（通过 `POST /api/v1/commands`）：

```json
{ "method": "tun.start", "params": { "addr": "10.0.0.1", "mask": "255.255.255.0", "tag": "my-tun", "name": "tun0", "mtu": 1500 } }
{ "method": "tun.stop" }
```

TUN 启动参数：
- `addr` -- 必填，分配给虚拟接口的 IP 地址
- `mask` -- 子网掩码，默认 `255.255.255.0`
- `tag` -- 必填，用于路由决策的入站标签；TUN 流量通过此标签匹配路由规则
- `name` -- 可选，OS 级别设备名称（如 `tun0`、`utun8`）；省略时自动分配
- `mtu` -- 可选，MTU 字节数，默认 `1500`

在内部，TUN 从虚拟接口读取原始 IP 数据包，解析 TCP 头部（当前支持 IPv4），维护最小 TCP 状态机，并将每个 TCP 连接通过 `serve_inbound()` 分发以进行统一路由和中继。实现位于 `crates/proxy/src/inbound/tun.rs`，平台后端位于 `crates/tun/`（Linux ioctl、macOS utun、Windows Wintun）。

SOCKS5 入站默认无认证。配置 `users` 启用 RFC 1929 用户名/密码认证：

```json
{
  "tag": "socks-in",
  "listen": { "address": "127.0.0.1", "port": 7890 },
  "protocol": {
    "type": "socks5",
    "users": [
      { "username": "alice", "password": "secret" }
    ]
  }
}
```

对于 SOCKS5 入站 users，`username` 可以省略。省略时内核使用 `password` 作为用户名。同时省略 `username` 和 `password` 的 user 对象会被忽略；空的 user 列表使入站保持无认证模式。只有 `username` 的 user 对象无效。

`mixed` 入站也可以为 SOCKS5 分支配置认证：

```json
{
  "tag": "mixed-in",
  "listen": { "address": "127.0.0.1", "port": 7890 },
  "protocol": {
    "type": "mixed",
    "socks5_users": [
      { "username": "alice", "password": "secret" }
    ]
  }
}
```

`mixed.socks5_users` 遵循与 SOCKS5 入站 users 相同的用户名/密码默认规则。

VLESS 入站必须配置 user UUID。`principal_key` 是稳定、非敏感的策略与计费归因键，会出现在 `flow.completed` 的 `auth` 和事件顶层；UUID 本身默认不会发送给面板：

```json
{
  "tag": "vless-in",
  "listen": { "address": "127.0.0.1", "port": 8443 },
  "protocol": {
    "type": "vless",
    "users": [
      {
        "id": "11111111-2222-3333-4444-555555555555",
        "principal_key": "user:10001"
      }
    ]
  }
}
```

VLESS 入站使用 TLS，在 protocol 内部添加 `tls`：

```json
{
  "tag": "vless-tls-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "vless",
    "users": [
      { "id": "11111111-2222-3333-4444-555555555555" }
    ],
    "tls": {
      "cert_path": "certs/fullchain.pem",
      "key_path": "certs/privkey.pem"
    }
  }
}
```

VLESS 入站支持 WebSocket 传输，通过 `ws` 启用：

```json
{
  "tag": "vless-ws-in",
  "listen": { "address": "0.0.0.0", "port": 80 },
  "protocol": {
    "type": "vless",
    "users": [
      { "id": "11111111-2222-3333-4444-555555555555" }
    ],
    "ws": {
      "path": "/vless"
    }
  }
}
```

WebSocket 可以与 TLS 结合使用 (WSS)：

```json
{
  "tag": "vless-wss-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "vless",
    "users": [
      { "id": "11111111-2222-3333-4444-555555555555" }
    ],
    "tls": {
      "cert_path": "certs/fullchain.pem",
      "key_path": "certs/privkey.pem"
    },
    "ws": {
      "path": "/vless"
    }
  }
}
```

### VMess 入站

VMess 入站处于实验阶段，需要 TLS。每个 user 必须提供 VMess UUID。`principal_key` 是稳定、非敏感的策略与计费归因键，`cipher` 省略时默认为 `aes-128-gcm`：

```json
{
  "tag": "vmess-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "vmess",
    "users": [
      {
        "id": "11111111-2222-3333-4444-555555555555",
        "cipher": "aes-128-gcm",
        "principal_key": "user:10001"
      }
    ],
    "tls": {
      "cert_path": "certs/fullchain.pem",
      "key_path": "certs/privkey.pem"
    }
  }
}
```

VMess 入站支持原始 TLS、WebSocket over TLS 和 gRPC over TLS。`ws` 和 `grpc` 互斥。支持的 cipher 值为 `auto`、`aes-128-gcm`、`chacha20-poly1305`、`none` 和 `zero`；`auto` 被规范化为当前 AEAD 基线。`none` 具有 Xray TCP 互操作性覆盖。`zero` 是 Zero 到 Zero 的能力，不应作为主流外部兼容性默认值暴露。

### Hysteria2 入站

Hysteria2 入站通过 QUIC 承载 TCP 流和 UDP datagram。服务器需要证书：

```json
{
  "tag": "hysteria2-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "hysteria2",
    "password": "your-secret-password",
    "cert_path": "certs/fullchain.pem",
    "key_path": "certs/privkey.pem"
  }
}
```

Hysteria2 配置字段：
- `password` -- 必填，客户端认证密码
- `cert_path` -- 可选，TLS 证书路径
- `key_path` -- 可选，TLS 私钥路径
- `up_bps` -- 可选，上传速率限制，单位 bytes/sec（内核 GCRA）
- `down_bps` -- 可选，下载速率限制，单位 bytes/sec（内核 GCRA）

### Shadowsocks 入站

Shadowsocks 入站使用 AEAD cipher 进行加密传输：

```json
{
  "tag": "ss-in",
  "listen": { "address": "127.0.0.1", "port": 8388 },
  "protocol": {
    "type": "shadowsocks",
    "password": "your-secret-password",
    "cipher": "chacha20-ietf-poly1305"
  }
}
```

Shadowsocks 配置字段：
- `password` -- legacy 或静态单用户入站密码；与 `users` / `identity_password` 互斥
- `identity_password` -- 可选，SIP023 AES 2022 多用户服务器 iPSK；配置后必须使用 `users`，热更新用户时保持不变
- `users` -- 可选，面板托管用户数组；AES 2022 模式下每个 password 必须是单个 uPSK
- `cipher` -- 可选，加密算法，默认 `chacha20-ietf-poly1305`；支持的值为 `aes-128-gcm`、`aes-256-gcm`、`chacha20-ietf-poly1305`、`2022-blake3-aes-128-gcm`、`2022-blake3-aes-256-gcm` 和 `2022-blake3-chacha20-poly1305`
- `up_bps` -- 可选，上传速率限制，单位 bytes/sec（内核 GCRA）
- `down_bps` -- 可选，下载速率限制，单位 bytes/sec（内核 GCRA）

对于 AEAD 2022 cipher 名称，每个 PSK 必须是标准 base64 密钥材料：`2022-blake3-aes-128-gcm` 需要 16 字节解码后密钥，`2022-blake3-aes-256-gcm` 和 `2022-blake3-chacha20-poly1305` 需要 32 字节解码后密钥。SIP023 多用户入站只支持 AES 2022，要求独立的 `identity_password`，且 iPSK 不得与任何 uPSK 相同。同步为空 `users` 时保持 fail-closed，不会把 iPSK 当作普通用户密码。

### Trojan 入站

Trojan 入站需要 TLS，在 TLS 隧道内进行密码认证，然后转发目标地址：

```json
{
  "tag": "trojan-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "trojan",
    "users": [{
      "password": "your-secret-password",
      "principal_key": "account:1001"
    }],
    "tls": {
      "cert_path": "certs/fullchain.pem",
      "key_path": "certs/privkey.pem"
    }
  }
}
```

Trojan 入站配置字段：
- `users` -- 多用户或面板管理配置；每项包含 `password`、可选稳定身份、用户限速和设备限制策略
- `password` -- 静态单用户兼容字段（SHA224 散列后比对），不能与 `users` 同时配置
- `sni` -- 可选，TLS SNI 值
- `tls` -- 必填，TLS 证书配置
  - `cert_path` -- 证书文件路径
  - `key_path` -- 私钥文件路径
- `up_bps` -- 可选，上传速率限制，单位 bytes/sec（内核 GCRA）
- `down_bps` -- 可选，下载速率限制，单位 bytes/sec（内核 GCRA）

### Mieru 入站

Mieru 入站可在配置中使用，接收来自内置客户端的加密 TCP 会话和 UDP 中继会话：

```json
{
  "tag": "mieru-in",
  "listen": { "address": "0.0.0.0", "port": 8964 },
  "protocol": {
    "type": "mieru",
    "users": [
      { "username": "alice", "password": "secret" }
    ]
  }
}
```

Mieru 入站配置字段：
- `users` -- 必填，非空用户名/密码对列表；`username` 可省略，默认为 `password`

Mieru 帧封装使用协议级加密段。代理在 Mieru 流封装器中保留 Mieru 特定帧处理，而不是直接使用通用原始 TCP 中继。当前兼容性工作聚焦于内置单跳行为；在与外部 Mieru 客户端和服务器有实际客户端覆盖之前，将互操作性视为实验性。

Mieru 没有无认证模式，因此 `password` 保持必填。

### 按入站速率限制 (rate_limits)

Hysteria2、Shadowsocks 和 Trojan 入站协议配置支持 `up_bps` 和 `down_bps` 字段，用于按入站 GCRA 速率限制。这些是 `InboundProtocolConfig::rate_limits()` 返回的值。

内核在 `serve_inbound()` 中通过 `apply_kernel_rate_limits()` 将它们作为默认值应用。如果协议的 accept 处理程序已经设置了按用户限制（例如 SOCKS5 `AuthHandler::rate_limit_for()`），则不应用按入站默认值——按用户限制始终优先。

SOCKS5、HTTP CONNECT、Mixed 和 VLESS 入站目前在其协议配置中不支持按入站速率限制（它们的 `rate_limits()` 返回 `(None, None)`）。

## Outbounds

```json
{
  "tag": "chain",
  "protocol": {
    "type": "socks5",
    "server": "127.0.0.1",
    "port": 2080
  }
}
```

当前支持：

- `direct`
- `block`
- `socks5`
- `vless`
- `hysteria2`
- `shadowsocks`
- `trojan`
- `vmess`
- `mieru`

SOCKS5 出站默认无认证。连接需要认证的上游时，配置 `password`，或同时配置 `username` 和 `password`。如果省略 `username`，内核使用 `password` 作为用户名。如果两者都省略，出站使用 SOCKS5 无认证。只配置 `username` 无效：

```json
{
  "tag": "chain",
  "protocol": {
    "type": "socks5",
    "server": "127.0.0.1",
    "port": 2080,
    "username": "upstream",
    "password": "secret"
  }
}
```

VLESS 出站，用于连接到上游 VLESS TCP 节点：

```json
{
  "tag": "vless-chain",
  "protocol": {
    "type": "vless",
    "server": "203.0.113.10",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555"
  }
}
```

连接到 TLS VLESS 上游，配置 `tls`。`server_name` 默认为 `server`。自签名或私有 CA 可使用 `ca_cert_path`。当上游不依赖 SNI 或需要隐藏目标域名时，设置 `disable_sni: true`：

```json
{
  "tag": "vless-tls-chain",
  "protocol": {
    "type": "vless",
    "server": "edge.example.com",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555",
    "tls": {
      "server_name": "edge.example.com",
      "ca_cert_path": "certs/ca.pem",
      "disable_sni": false,
      "insecure": false
    }
  }
}
```

TLS 配置字段：
- `server_name` -- 可选，SNI 和证书验证域名，默认为 `server`
- `ca_cert_path` -- 可选，自定义 CA 证书路径
- `disable_sni` -- 可选，不发送 SNI 扩展，默认 `false`
- `insecure` -- 可选，跳过证书验证，默认 `false`
- `alpn` -- 可选，ALPN 协议列表

连接到 VLESS Reality 上游，配置 `reality`。Reality 是 VLESS TLS 风格的安全层，不能与 `tls` 或 `ws` 组合；当前支持原始 TCP 出站 Reality：

```json
{
  "tag": "vless-reality-chain",
  "protocol": {
    "type": "vless",
    "server": "edge.example.com",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555",
    "reality": {
      "public_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      "short_id": "0123456789abcdef",
      "server_name": "www.cloudflare.com"
    }
  }
}
```

Reality 配置字段：
- `public_key` -- 必填，上游 Reality X25519 公钥，base64url 无填充编码，必须解码为 32 字节
- `short_id` -- 可选，0 到 16 个十六进制字符，默认为空
- `server_name` -- 可选，Reality ClientHello 中使用的 SNI，默认为 `server`
- `cipher_suites` -- 可选，TLS 1.3 cipher suite 名称列表；支持 `TLS_AES_128_GCM_SHA256`、`TLS_AES_256_GCM_SHA384`、`TLS_CHACHA20_POLY1305_SHA256`

VLESS 出站支持 WebSocket 传输，通过 `ws` 启用：

```json
{
  "tag": "vless-ws-chain",
  "protocol": {
    "type": "vless",
    "server": "edge.example.com",
    "port": 80,
    "id": "11111111-2222-3333-4444-555555555555",
    "ws": {
      "path": "/vless",
      "headers": {
        "User-Agent": "zero-proxy"
      }
    }
  }
}
```

WebSocket 可以与 TLS 结合使用 (WSS)：

```json
{
  "tag": "vless-wss-chain",
  "protocol": {
    "type": "vless",
    "server": "edge.example.com",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555",
    "tls": {
      "server_name": "edge.example.com"
    },
    "ws": {
      "path": "/vless"
    }
  }
}
```

WebSocket 配置字段：
- `path` -- WebSocket 握手路径，不能为空
- `headers` -- 可选，自定义 HTTP headers；不得包含 `Host`、`Connection`、`Upgrade`、`Sec-WebSocket-*` 和其他必需的握手 headers

### Hysteria2 出站

连接到上游 Hysteria2 节点，通过 QUIC 承载 TCP 和 UDP：

```json
{
  "tag": "hysteria2-chain",
  "protocol": {
    "type": "hysteria2",
    "server": "example.com",
    "port": 443,
    "password": "your-secret-password",
    "insecure": true
  }
}
```

Hysteria2 出站配置字段：
- `server` -- 必填，上游服务器地址
- `port` -- 必填，上游端口，必须大于 0
- `password` -- 必填，认证密码
- `insecure` -- 可选，跳过证书验证，默认 `false`
- `client_fingerprint` -- 可选，TLS 客户端指纹预设：`chrome`、`firefox`、`safari`、`ios`、`edge`、`randomized`；省略则使用 rustls 默认值

### Shadowsocks 出站

连接到上游 Shadowsocks 节点：

```json
{
  "tag": "ss-chain",
  "protocol": {
    "type": "shadowsocks",
    "server": "example.com",
    "port": 8388,
    "password": "your-secret-password",
    "cipher": "chacha20-ietf-poly1305"
  }
}
```

Shadowsocks 出站配置字段：
- `server` -- 必填，上游服务器地址
- `port` -- 必填，上游端口，必须大于 0
- `password` -- 必填，加密密码
- `cipher` -- 可选，加密算法，默认 `chacha20-ietf-poly1305`；支持的值为 `aes-128-gcm`、`aes-256-gcm`、`chacha20-ietf-poly1305`、`2022-blake3-aes-128-gcm`、`2022-blake3-aes-256-gcm` 和 `2022-blake3-chacha20-poly1305`

对于 AEAD 2022 cipher 名称，`password` 必须是标准 base64 密钥材料：`2022-blake3-aes-128-gcm` 需要 16 字节解码后密钥，`2022-blake3-aes-256-gcm` 和 `2022-blake3-chacha20-poly1305` 需要 32 字节解码后密钥。AES 2022 密码可以是冒号分隔的身份密钥链；Zero 验证并使用最后一段作为用户 PSK。

### Trojan 出站

连接到上游 Trojan 节点，在 TLS 隧道内通过密码认证，然后转发：

```json
{
  "tag": "trojan-chain",
  "protocol": {
    "type": "trojan",
    "server": "example.com",
    "port": 443,
    "password": "your-secret-password",
    "sni": "example.com",
    "insecure": false
  }
}
```

Trojan 出站配置字段：
- `server` -- 必填，上游服务器地址
- `port` -- 必填，上游端口，必须大于 0
- `password` -- 必填，认证密码（发送前进行 SHA224 散列）
- `sni` -- 可选，TLS SNI，默认为 `server`
- `insecure` -- 可选，跳过证书验证，默认 `false`
- `client_fingerprint` -- 可选，TLS 客户端指纹预设：`chrome`、`firefox`、`safari`、`ios`、`edge`、`randomized`；省略则使用 rustls 默认值

### VMess 出站

VMess 出站当前支持内置 AEAD TCP 和 UDP-over-stream 实现以及显式 cipher 名称：

```json
{
  "tag": "vmess-chain",
  "protocol": {
    "type": "vmess",
    "server": "example.com",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555",
    "cipher": "aes-128-gcm"
  }
}
```

VMess 出站配置字段：
- `server` -- 必填，上游服务器地址
- `port` -- 必填，上游端口，必须大于 0
- `id` -- 必填，VMess UUID
- `cipher` -- 可选，默认 `aes-128-gcm`；支持的值为 `auto`、`aes-128-gcm`、`chacha20-poly1305`、`none` 和 `zero`；`auto` 被规范化为当前 AEAD 基线
- `tls` -- 可选，TLS 传输封装
- `ws` -- 可选，WebSocket 传输封装
- `grpc` -- 可选，gRPC 传输封装

`ws` 和 `grpc` 互斥。如果设置了 `tls.server_name` 或 `tls.ca_cert_path`，它们不能为空。

兼容性说明：Xray/Clash 导出通常使用 `cipher: auto`；Zero 接受该别名。外部 TCP 和 UDP 基线互操作性已覆盖：Xray 双向、Zero 出站到 sing-box 入站、Mihomo 出站到 Zero 入站。Xray WS/gRPC TCP 传输互操作性已覆盖双向。`cipher: none` 具有 Xray TCP 互操作性覆盖。`cipher: zero` 不声称具有主流 Xray/sing-box/Clash 兼容性。

### Mieru 出站

连接到上游 Mieru 节点：

```json
{
  "tag": "mieru-chain",
  "protocol": {
    "type": "mieru",
    "server": "example.com",
    "port": 8964,
    "username": "alice",
    "password": "secret"
  }
}
```

Mieru 出站配置字段：
- `server` -- 必填，上游服务器地址
- `port` -- 必填，上游端口，必须大于 0
- `username` -- 可选，上游用户名；省略时内核使用 `password` 作为用户名
- `password` -- 必填，上游密码

Mieru 没有无认证模式，因此 `password` 保持必填。

Mieru 出站支持直接单跳 TCP 路由、TCP 中继链组合以及通过加密 Mieru 流封装器的 UDP 数据包中继。

UDP 出站选择由内核 UDP 分发路径处理。当前 TCP、UDP、MUX、传输和限制事实通过 `capabilities.protocols` 暴露，并记录在 [protocol-capabilities.md](protocol-capabilities.md) 中。

### 出站熔断器

`zero-engine` 为每个链式出站标签维护健康状态。在每次连接尝试之前，TCP 管道的候选建立路径会调用 `check_outbound_health()`。如果在 30 秒窗口内累积 5 次失败，该出站将被隔离 60 秒。隔离期满后，允许一个探测连接；成功则清除不健康状态，失败则重置冷却期。

这是内核原语——无需配置。它自动应用于除 `direct` 和 `block` 之外的所有出站连接路径。

## Outbound Groups

当前实现了五种出站组类型：

- `selector`
- `fallback`
- `url_test`
- `relay`
- `load_balance`

组成员可以是具体出站或其他出站组。配置验证时拒绝循环引用。

### selector

```json
{
  "tag": "proxy",
  "type": "selector",
  "outbounds": ["node-a", "node-b"],
  "selected": "node-a"
}
```

`selector` 支持通过 `POST /api/v1/commands` 使用 `method: "policies.select"` 进行运行时切换。

```json
{
  "method": "policies.select",
  "params": {
    "policy_tag": "proxy",
    "target_tag": "direct"
  }
}
```

成功切换后，`/api/v1/config` 和 `/api/v1/runtime` 中的 `outbound_groups[*].selected` 立即反映新的选择。

### fallback

```json
{
  "tag": "proxy",
  "type": "fallback",
  "outbounds": ["node-a", "direct"]
}
```

语义：

- 按配置顺序尝试成员
- 连接失败时自动回退到下一个成员
- 一旦连接成功，该会话将固定在该成员上
- 熔断器在连接尝试之前隔离不健康的成员，导致自动回退

### url_test

```json
{
  "tag": "proxy",
  "type": "url_test",
  "outbounds": ["node-a", "node-b", "direct"],
  "url": "http://example.com/",
  "interval_seconds": 300
}
```

语义：

- 按 `interval_seconds` 间隔进行探测
- 当前仅支持 `http://` 探测 URL
- `url` 可省略；省略时使用 `runtime.latency_test_url`
- 选择探测成功且延迟最低的成员
- 如果本轮所有探测都失败，保持当前选择；首次探测前默认使用第一个成员

### relay

```json
{
  "tag": "hk-us",
  "type": "relay",
  "proxies": ["hk-vless", "us-socks5"]
}
```

语义：

- 按顺序链式连接成员：流量按顺序流经每个代理
- 第一个成员是入口，最后一个成员是出口
- 任一跳的连接失败都会终止整条链
- 熔断器分别应用于每个链式成员

### load_balance

```json
{
  "tag": "lb",
  "type": "load_balance",
  "outbounds": ["node-a", "node-b", "node-c"],
  "strategy": "round_robin"
}
```

负载均衡配置字段：
- `outbounds` -- 必填，要均衡的出站标签列表
- `default` -- 可选，初始出站选择；未设置时回退到 `outbounds[0]`
- `strategy` -- 可选，分配策略，默认 `round_robin`
  - `round_robin` -- 按顺序在成员间分配连接
  - `random` -- 为每个连接随机选择一个成员

组成员可以是具体出站或其他出站组。配置验证时拒绝循环引用。

## Mode

当前支持：

- `rule`
- `global`
- `direct`

`global` 需要引用一个出站或出站组：

```json
{
  "type": "global",
  "outbound": "proxy"
}
```

## Route

规则由 `condition + action` 组成：

```json
{
  "condition": { "type": "domain", "values": ["blocked.example"] },
  "action": { "type": "route", "outbound": "block" }
}
```

当前支持的 conditions：

- `inbound` -- 按入站 `tag` 匹配，可用于把不同监听入口路由到不同 outbound group
- `domain` -- 域名匹配，支持 `example.com` 精确匹配和 `*.example.com` 通配符匹配
- `domain_keyword` -- 域名包含关键字时匹配
- `domain_regex` -- 根据一个或多个正则表达式模式匹配域名
- `ip` -- CIDR 匹配
- `rule_set` -- 引用外部规则集文件
- `geoip` -- MaxMind GeoLite2-Country mmdb 国家代码匹配
- `sni` -- TLS ClientHello SNI 域名匹配（语法同 domain）
- `and` -- 所有子条件必须匹配
- `or` -- 任一子条件匹配即可

当前支持的 actions：

- `direct`
- `reject`
- `route`

### domain_regex 条件

`domain_regex` 条件根据一个或多个正则表达式模式匹配目标域名。模式在启动时编译。匹配从会话中提取的目标域名。支持与 `and` / `or` 组合。

```json
{
  "condition": { "type": "domain_regex", "values": ["^.*\\.google\\..*$", "^.*\\.youtube\\..*$"] },
  "action": { "type": "route", "outbound": "proxy" }
}
```

注意：`domain_regex` 模式中的捕获组不用于路由上下文。如需基于捕获的域名替换，请改用 `url_rewrite.from_regex`。

### url_rewrite

`route.url_rewrite` 是在路由之前应用的域名重写规则数组。规则按首次匹配优先的方式执行：第一个 `from` 或 `from_regex` 匹配目标域名的规则胜出，不再评估后续规则。

每个 `UrlRewriteRule`：

| 字段 | 类型 | 默认值 | 描述 |
|------|------|------|------|
| `from` | string | -- | 精确匹配的域名 |
| `from_regex` | string | -- | 匹配域名的正则表达式模式 |
| `to` | string | (必填) | 替换域名；支持 `$1`、`$2` 等正则捕获引用 |
| `status_code` | u16 | -- | 如果设置，返回 HTTP 重定向（如 `302`）；仅限基于 HTTP 的协议 |

必须至少设置 `from` 或 `from_regex` 之一。

`status_code` 触发协议级 HTTP 重定向（用于 HTTP CONNECT）。非 HTTP 协议（SOCKS5、Shadowsocks 等）静默忽略 `status_code`，始终重写目标域名。

```json
{
  "route": {
    "url_rewrite": [
      { "from": "old.example.com", "to": "new.example.com" },
      { "from_regex": "^(.+)\\.mirror\\.example\\.com$", "to": "$1.example.com" },
      { "from": "temp.example.com", "to": "permanent.example.com", "status_code": 301 }
    ],
    "rules": [],
    "final": { "type": "direct" }
  }
}
```

## 外部规则集

匹配数据可以放在外部文件中，通过主配置中的 `tag` 引用。

当前支持：

- `type = file`
- `type = url`（远程获取并使用本地缓存）
- `format = domain_list`
- `format = cidr_list`
- `format = zero_rule_ir`（Zero Rule IR v1 JSON）
- `format = zrs`（ZRS 0.1 二进制 MatcherSet）

```json
{
  "route": {
    "rule_sets": [
      {
        "tag": "ads",
        "type": "file",
        "path": "rules/ads.txt",
        "format": "domain_list"
      },
      {
        "tag": "lan",
        "type": "file",
        "path": "rules/lan.txt",
        "format": "cidr_list"
      },
      {
        "tag": "geo-cn",
        "type": "file",
        "path": "rules/geo-cn.zrs",
        "format": "zrs"
      }
    ],
    "rules": [
      {
        "condition": { "type": "rule_set", "tag": "ads" },
        "action": { "type": "reject" }
      },
      {
        "condition": { "type": "rule_set", "tag": "lan" },
        "action": { "type": "route", "outbound": "direct" }
      }
    ],
    "final": { "type": "route", "outbound": "proxy" }
  }
}
```

注意事项：

- `path` 支持相对路径，默认相对于配置文件所在目录解析
- `domain_list` 作为域名列表加载
- `cidr_list` 作为 CIDR 列表加载
- `zero_rule_ir` 按 [Zero Rule IR v1](./zero-rule-ir-v1.md) 解码并编译为内存 matcher；兼容别名为 `zero_ir`
- `zrs` 按 [ZRS 0.1](./zrs-0.1.md) 执行完整 checksum 校验后只读 mmap，并预热索引根页
- 空行被忽略
- 以 `#` 或 `//` 开头的行被忽略
- 规则文件仅包含匹配数据，不包含动作
- 四种格式最终都通过同一个 `zero-rule` matcher 接口参与 TCP/UDP 的实际 route 首条命中决策；`RouteRule` 的顺序、action 和 final 仍由主配置拥有
- `domain_list` 中每一项保持原有语义：同时匹配该域名本身及其子域名；`cidr_list` 同时支持 IPv4 与 IPv6
- `type = url` 额外需要 `url` 字段；`path` 指定的文件用作本地缓存
- `type = url` 时 `update_interval_seconds`（默认 `86400`，即 24 小时）控制远程规则集重新拉取间隔

## 状态字段语义

`status --json` 与会话相关的当前字段语义：

- `bytes_up` / `bytes_down`
  - 从流角度看的累计应用层链路字节数
  - 包括 SOCKS5 / HTTP CONNECT 握手、SOCKS5 UDP 数据包头和转发载荷
  - 不包括 TCP/IP 头部、TCP 三次握手和其他内核网络栈开销
  - TCP 统计按连接计算，SOCKS5 UDP 统计按目标流计算
- `inbound_rx_bytes` / `inbound_tx_bytes`
  - 入站端实际读/写的应用层字节数
- `outbound_rx_bytes` / `outbound_tx_bytes`
  - 出站端实际读/写的应用层字节数
- `throughput_up_bps` / `throughput_down_bps`
  - 1 秒采样吞吐量
- `recent_completed_sessions`
  - 最近完成的会话结算记录
  - TCP 连接和 SOCKS5 UDP 流使用相同的字段结构
- `outbound_groups[*].selected`
  - 该组当前选中的成员
- `outbound_groups[*].latency_ms`
  - `url_test` 最近一次成功探测的延迟
- `outbound_groups[*].last_checked_unix_ms`
  - `url_test` 最近一次探测完成时间

## 约束

- `tag` 不能为空
- SOCKS5 用户名/密码在配置规范化后不能为空，各最多 255 字节
- SOCKS5 入站和 mixed SOCKS5 users 可以省略 `username`；默认为 `password`
- SOCKS5 出站可以省略两个认证字段以使用无认证模式，或仅省略 `username` 使其默认为 `password`；仅配置 `username` 无效
- VLESS 入站必须至少有一个用户，`id` 必须是 UUID；启用 TLS 时 `cert_path` 和 `key_path` 不能为空；启用 WebSocket 时 `ws.path` 不能为空
- VLESS 出站 `server` 不能为空，`port` 必须大于 `0`，`id` 必须是 UUID；`tls.server_name`、`tls.ca_cert_path` 和 `reality.server_name` 如果配置则不能为空
- VLESS 出站 `reality.public_key` 必须是 32 字节 base64url 无填充值；`reality.short_id` 最多 16 个十六进制字符；`reality` 不能与 `tls` 或 `ws` 组合
- 同一对象类型内的 tag 不能重复
- 相同的 `address:port` 只能有一个入站
- 同一端口需要 SOCKS5 TCP/UDP 和 HTTP CONNECT TCP 时，使用 `mixed`
- `route` 和 `global mode` 引用的目标必须存在
- 出站组的成员必须是已定义的出站或已定义的组
- 出站组不能有循环引用
- `runtime.udp_upstream_idle_timeout_seconds` 必须大于 `0`
- `runtime.event_log_capacity` 必须大于 `0`
- `runtime.latency_test_url` 如果设置，必须是非空的 `http://` URL
- `runtime.principal_quota_state_path` 如果设置，必须是非空路径
- `runtime.network.mtu` 不能小于 `576`
- `rule_sets[*].tag` 不能为空且不能重复
- `rule_set` 条件引用的 `tag` 必须存在
- `url_test.url` 可省略；如果设置，当前必须是 `http://`
- `url_test.interval_seconds` 必须大于 `0`
- Hysteria2 入站使用旧式单 `password` 或面板托管 `users`（空 `users` 表示拒绝全部新认证）；出站 `server` 不能为空，`port` 必须大于 `0`
- Shadowsocks 入站使用旧式单 `password` 或面板托管 `users`（空 `users` 表示拒绝全部认证），出站 `password` 不能为空；`cipher` 必须是支持的 Shadowsocks cipher 名称之一；AEAD 2022 PSK 必须解码为对应方法密钥长度；SIP023 托管多用户要求 AES 2022、独立 `identity_password` 和单段 uPSK，iPSK 与 uPSK 不得复用
- Trojan 入站必须配置 `tls`，`cert_path` 和 `key_path` 不能为空，并配置旧式 `password` 或 `users`（空 `users` 表示拒绝全部认证）；出站 `server` 不能为空，`port` 必须大于 `0`，`password` 不能为空
- `domain_regex` 条件要求在 `values` 中至少有一个模式
- `url_rewrite` 规则要求至少设置 `from` 或 `from_regex` 之一，且 `to` 不能为空
- `idle_timeout_secs` 如果设置则必须大于 `0`

## 运行时管理

### 模式切换

启动后的模式可以通过 CLI、IPC 或 HTTP API 进行热切换，无需重启：

```bash
zero mode rule              # 切换回规则匹配
zero mode direct            # 全部直连
zero mode global proxy      # 通过指定出站全局代理
```

IPC 等价命令：

```json
{ "method": "mode.set", "params": { "mode": "global", "outbound": "proxy" } }
```

### 热重载

`zero reload <config>` 重新加载配置文件。以下更改立即生效：

- 路由规则、模式、DNS 配置 -- 热交换
- outbound_groups 调整 -- 热交换
- inbound 添加/删除或监听形态修改 -- 运行时 reconcile；同 tag 变更会短暂重绑
- connector 确认式 reload 在监听绑定失败时恢复上一份配置；普通 `config.apply` 仍只表示命令已接受，自动化发布方应复核运行状态

### 配置验证

```bash
zero validate config.json
```

离线验证配置（不连接运行中的守护进程）。成功时打印摘要：

```
config valid: 2 inbounds, 3 outbounds, 1 groups, 5 rules
```

### 选择器切换

```bash
zero select <group-tag> <target-tag>
```

等效 HTTP API：`POST /api/v1/commands`，`method: "policies.select"`。

## 示例

`examples/` 包含可运行的配置样本，涵盖基本入站、链式出站、selector/fallback/url_test 组、规则集、VLESS、Hysteria2、Shadowsocks 和 Trojan。

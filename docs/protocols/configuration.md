# 配置速查

本页只列协议相关的常见配置形状。完整字段、校验规则、路由、模式和出站组见 [配置规范](../project/config.md)。

## Inbound

### SOCKS5

```json
{
  "tag": "socks-in",
  "listen": { "address": "127.0.0.1", "port": 1080 },
  "protocol": {
    "type": "socks5",
    "users": [
      { "username": "alice", "password": "secret" }
    ]
  }
}
```

`users` 可省略，省略时为 no-auth。用户项中 `username` 可省略，省略时内核使用 `password` 作为 username；`username` 和 `password` 都省略的用户项会被忽略。

### HTTP CONNECT

```json
{
  "tag": "http-in",
  "listen": { "address": "127.0.0.1", "port": 8080 },
  "protocol": { "type": "http" }
}
```

### Mixed

```json
{
  "tag": "mixed-in",
  "listen": { "address": "127.0.0.1", "port": 1080 },
  "protocol": { "type": "mixed" }
}
```

`mixed` 是入站复用器：SOCKS5 TCP、SOCKS5 UDP ASSOCIATE 和 HTTP CONNECT 共用同一个监听端口。

### VLESS

```json
{
  "tag": "vless-in",
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

`tls`、`reality`、`ws`、`grpc`、`h2`、`http_upgrade` 和 `split_http`（XHTTP，配置字段名沿用 `split_http`，支持 `mode`：`auto`/`stream-one` 单连接、`packet-up`/`stream-up` 双连接）是可选传输配置。`stream-one` 出站使用 H2/H2C，入站按首包同时接受 H2/H2C 与 HTTP/1.1 chunked；path 会规范化为尾随 `/`，并使用 XHTTP 默认请求/响应 padding。`reality` 不能和这些非 raw TCP 传输组合。`quic` 字段保留以向后兼容，但 XTLS 已弃用 VLESS 独立 QUIC 传输（继任者为 XHTTP `stream-one` H3）。

### Shadowsocks

```json
{
  "tag": "ss-in",
  "listen": { "address": "0.0.0.0", "port": 8388 },
  "protocol": {
    "type": "shadowsocks",
    "cipher": "chacha20-ietf-poly1305",
    "users": [{
      "password": "your-secret-password",
      "principal_key": "account:1001"
    }]
  }
}
```

`users` 用于面板管理的多用户入站，并支持在线原子替换；旧式单用户 `password` 仍兼容。legacy AEAD 直接使用各用户的 password。Shadowsocks 2022 单端口多用户使用 SIP023 EIH，AES 2022 入站必须另外配置固定的 `identity_password`（服务器 iPSK），每个 `users[*].password` 是独立的 uPSK：

```json
{
  "type": "shadowsocks",
  "cipher": "2022-blake3-aes-128-gcm",
  "identity_password": "MDEyMzQ1Njc4OWFiY2RlZg==",
  "users": [{
    "password": "ZmVkY2JhOTg3NjU0MzIxMA==",
    "principal_key": "account:1001"
  }]
}
```

`identity_password` 不参与面板用户替换；同步为空用户集时仍保留 iPSK，但拒绝全部认证，避免退化成静态单用户节点。iPSK 与所有 uPSK 必须不同。SIP023 EIH 只适用于两个 AES 2022 方法，`2022-blake3-chacha20-poly1305` 不允许配置多用户 EIH。

支持 cipher：

- `aes-128-gcm`
- `aes-256-gcm`
- `chacha20-ietf-poly1305`
- `2022-blake3-aes-128-gcm`
- `2022-blake3-aes-256-gcm`
- `2022-blake3-chacha20-poly1305`

AEAD 2022 的 PSK 必须是标准 base64 key material：AES-128 为 16 字节，AES-256 和 chacha20 为 32 字节。出站 `password` 可使用 SIP023 的 `iPSK[:iPSK...]:uPSK` 身份链；Zero 为 TCP/UDP 生成 EIH，并以最后一段 uPSK 加密负载。

### Trojan

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

静态单用户节点仍可使用旧的顶层 `password`。面板管理节点使用 `users`，两种形式不能同时配置；`users` 可为空以表示暂时拒绝所有认证。

### Hysteria2

```json
{
  "tag": "hysteria2-in",
  "listen": { "address": "0.0.0.0", "port": 8443 },
  "protocol": {
    "type": "hysteria2",
    "users": [{
      "password": "your-secret-password",
      "principal_key": "account:1001"
    }],
    "cert_path": "certs/fullchain.pem",
    "key_path": "certs/privkey.pem"
  }
}
```

面板托管节点使用 `users`，认证身份会同时投影到该 QUIC 连接承载的 TCP stream 与 UDP datagram；旧式单用户 `password` 继续兼容。空 `users` 表示拒绝全部新认证。

### Mieru

```json
{
  "tag": "mieru-in",
  "listen": { "address": "0.0.0.0", "port": 2999 },
  "protocol": {
    "type": "mieru",
    "users": [
      { "username": "alice", "password": "secret" }
    ]
  }
}
```

`username` 可省略，省略时内核使用 `password` 作为 username。Mieru 协议没有 no-auth 模式，`password` 仍必须配置。

### VMess

```json
{
  "tag": "vmess-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "vmess",
    "users": [
      {
        "id": "11111111-2222-3333-4444-555555555555",
        "cipher": "aes-128-gcm"
      }
    ],
    "tls": {
      "cert_path": "certs/fullchain.pem",
      "key_path": "certs/privkey.pem"
    }
  }
}
```

VMess inbound 当前要求 `tls`。可选传输为 raw TLS、WebSocket over TLS、gRPC over TLS，且 `ws` 和 `grpc` 互斥。`users[].cipher` 可选，默认 `aes-128-gcm`；可配置 `auto`、`aes-128-gcm`、`chacha20-poly1305`、`none`、`zero`。`auto` 会被归一化为当前 AEAD 基线。`none` 已通过 Xray TCP 双向互通；`zero` 仅作为 Zero 内部路径能力记录，不作为主流外部兼容选项展示。

## Outbound

### SOCKS5

```json
{
  "tag": "socks-out",
  "protocol": {
    "type": "socks5",
    "server": "127.0.0.1",
    "port": 1081,
    "username": "upstream",
    "password": "secret"
  }
}
```

`username` 可省略。只配置 `password` 时，内核使用 `password` 作为 username；两者都省略时使用 SOCKS5 no-auth；只配置 `username` 仍是无效配置。

### VLESS

```json
{
  "tag": "vless-out",
  "protocol": {
    "type": "vless",
    "server": "example.com",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555",
    "mux_concurrency": 8,
    "xudp_concurrency": 8,
    "mux_response_backlog_frames": 32,
    "mux_response_backlog_bytes": 1048576,
    "tls": {
      "server_name": "example.com"
    }
  }
}
```

`mux_concurrency` 与 `xudp_concurrency` 是 Zero 原生且彼此独立的能力开关：前者启用 TCP MUX 子流池，后者启用 XUDP 会话池；两者取值范围均为 `1..=65535`。`flow` 只表达 VLESS flow，不再隐式开启 MUX/XUDP。`mux_idle_timeout_secs` 是物理 MUX 载体的无帧活动超时：任一真实上行或下行 MUX 帧都会刷新期限，超过期限后读写两半同时关闭，后续逻辑流会新建载体；不同超时策略不会共用同一个池。

VLESS 与 VMess 入站、出站都可设置 Zero 原生容量策略 `mux_response_backlog_frames` 和 `mux_response_backlog_bytes`。前者控制每个逻辑流可积压的响应帧数，范围 `1..=4096`；后者控制每个物理载体可积压的响应总字节，范围 `16384..=67108864`。省略时分别使用安全默认值 32 帧和 1 MiB。慢消费者越界时只终止对应逻辑流并释放已保留字节；出站池键包含完整容量策略，因此不同配置不会错误复用同一物理载体。

### Shadowsocks

```json
{
  "tag": "ss-out",
  "protocol": {
    "type": "shadowsocks",
    "server": "example.com",
    "port": 8388,
    "password": "your-secret-password",
    "cipher": "chacha20-ietf-poly1305"
  }
}
```

AEAD 2022 password 规则与 inbound 相同。

### Trojan

```json
{
  "tag": "trojan-out",
  "protocol": {
    "type": "trojan",
    "server": "example.com",
    "port": 443,
    "password": "your-secret-password",
    "sni": "example.com"
  }
}
```

### Hysteria2

```json
{
  "tag": "hysteria2-out",
  "protocol": {
    "type": "hysteria2",
    "server": "example.com",
    "port": 443,
    "password": "your-secret-password"
  }
}
```

### Mieru

```json
{
  "tag": "mieru-out",
  "protocol": {
    "type": "mieru",
    "server": "example.com",
    "port": 2999,
    "password": "secret"
  }
}
```

`username` 可省略，省略时内核使用 `password` 作为 username。Mieru 协议没有 no-auth 模式，`password` 仍必须配置。

### VMess

```json
{
  "tag": "vmess-out",
  "protocol": {
    "type": "vmess",
    "server": "example.com",
    "port": 443,
    "id": "11111111-2222-3333-4444-555555555555",
    "cipher": "aes-128-gcm",
    "mux_concurrency": 8
  }
}
```

VMess 是 `partial` 能力。TCP 握手、TCP/UDP MUX、UDP-over-stream 和 body relay 使用 in-tree 实现；raw TLS、WSS、gRPC TCP 路径、本地 TCP MUX、本地 MUX UDP、本地 VMess UDP 单跳闭环，以及 `cipher: none` / `cipher: zero` 均有 Zero 内部覆盖。`cipher: auto` 会被归一化为当前 AEAD 基线。

已完成的外部互通覆盖包括：Zero outbound -> Xray inbound TCP/UDP、Xray outbound -> Zero inbound TCP/UDP、Zero outbound -> Xray inbound WS/gRPC TCP、Xray outbound -> Zero inbound WS/gRPC TCP、Zero outbound -> sing-box inbound TCP/UDP、Mihomo outbound -> Zero inbound TCP/UDP。`cipher: none` 已完成 Xray TCP 双向互通。`cipher: zero` 不作为主流外部兼容能力展示。

### Direct 和 Block

```json
{ "tag": "direct", "protocol": { "type": "direct" } }
```

```json
{ "tag": "block", "protocol": { "type": "block" } }
```

`direct` 和 `block` 是内核动作，不是外部协议。

# VMess Outbound

对应 `protocols/vmess/src/outbound.rs` — `VmessOutbound`、`VmessOutboundSession`、`VmessTcpSessionTarget`。

## VmessOutbound

实现 `TcpSessionProtocol` trait：

1. 建立 TLS（或 WS/gRPC over TLS）连接
2. 构造 Xray AEAD header（auth ID + 加密的 request body）
3. 写入 header → 等待 response header
4. 返回 `VmessOutboundSession { body_aead, response_body_aead }`

Proxy runtime 负责 transport setup、routing、metering、session lifecycle、stats 和 events。

## VmessOutboundSession

```rust
pub struct VmessOutboundSession {
    pub body_aead: BodyAead,           // 上传方向 AEAD state
    pub response_body_aead: BodyAead,  // 下载方向 AEAD state
}
```

用于构造 `VmessAeadStream` 进行双向 body relay。

## VmessTcpSessionTarget

```rust
pub struct VmessTcpSessionTarget {
    pub address: Address,
    pub port: u16,
    pub session: VmessOutboundSession,
}
```

## Outbound 配置示例

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

可选 transport 字段：`tls`、`ws`、`grpc`。`ws` 和 `grpc` 互斥。

- `mux_concurrency`: 启用 VMess TCP MUX 连接池，指定最大并发子连接数
- `mux_idle_timeout_secs`: 可选，连接池空闲超时
- `mux_response_backlog_frames`: 可选，每逻辑流响应积压帧数，范围 `1..=4096`，默认 32
- `mux_response_backlog_bytes`: 可选，每物理载体响应积压总字节，范围 16 KiB..=64 MiB，默认 1 MiB

## 已验证路径

内部验证：
- VMess TCP raw TLS、WSS、gRPC 出站路径
- 全部 cipher 双向 AEAD body relay
- 全部 cipher 关闭 termination chunk 投递
- SOCKS5 入口 → VMess MUX TCP → VMess 入站 → direct echo

外部验证（仅限本地，`#[ignore]`）：
- Xray 双向 TCP（`aes-128-gcm`/`none`）
- Xray 双向 WS+TLS TCP
- Xray 双向 gRPC+TLS TCP
- Xray 双向 UDP
- sing-box 入站 TCP + UDP
- Mihomo 出站 TCP（`auto`）+ UDP（`CMD_UDP` raw datagram）

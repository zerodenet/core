# VMess Inbound

对应 `protocols/vmess/src/inbound.rs` — `VmessInbound`、`VmessUser` 以及协议内部 accept state。

## VmessInbound

实现 `InboundProtocol` trait，通过 `serve_inbound()` 统一管线处理 raw TLS / WS / gRPC 三种传输路径。

### TCP 入站流程

1. TLS 握手完成（必须）
2. 读取 Xray AEAD header（16 字节 auth ID + 加密 payload）
3. KDF 派生 cmd_key → 验证 auth ID（时间窗口 ±30s）
4. 解密 header → 提取 cipher、command、target address、port
5. 返回 `VmessAccept { user, session, body_aead }`

### 多用户认证 (`accept_tcp_multi`)

```rust
pub async fn accept_tcp_multi(
    stream: T,
    users: &[VmessUser],
) -> Result<VmessAccept<T>, VmessError>
```

- `VmessReadBuffer` 一次读取 wire data
- 多个 `VmessUser` 的 uuid 依次尝试解密 header
- 认证失败：发送 rejection response（非静默断开）
- 支持 per-user `principal_key` 和 rate limits

### VmessUser

```rust
pub struct VmessUser {
    pub id: Uuid,
    pub cipher: VmessCipher,
    pub principal_key: Option<String>,
}
```

`cipher` 可选，默认 `aes-128-gcm`。

## Inbound 配置示例

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

- `tls`: **必需**，VMess inbound 必须使用 TLS
- `users[].cipher`: 可选，默认 `aes-128-gcm`
- `ws` 和 `grpc`: 互斥，不能同时启用
## 边界说明

`VmessAccept` and the inbound stream wrapper handoff stay crate-private.
Downstream glue should use `accept_tcp_stream()`, `accept_client()`, or the route helpers
instead of depending on raw accept/session crypto state.

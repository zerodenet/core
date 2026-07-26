# Shadowsocks Stream

对应 `protocols/shadowsocks/src/stream.rs` — `ShadowsocksAeadStream`。

## ShadowsocksAeadStream

实现 `AsyncRead + AsyncWrite`，为 Shadowsocks TCP relay 提供透明双向 AEAD 加解密。

### 结构

```rust
pub struct ShadowsocksAeadStream<S> {
    inner: S,
    cipher: CipherKind,
    read_key: Option<Vec<u8>>,      // 读方向密钥 (upload key for inbound, download key for outbound)
    read_password: Option<Vec<u8>>, // 读方向密码 (用于 response salt 派生 download key)
    read_nonce: u64,                // 读方向 nonce counter
    read_state: ReadState,          // 当前读状态 (Length or Payload)
    read_plain: Vec<u8>,            // 解密后待消费的 buffer
    read_plain_pos: usize,          // read_plain 的游标
    write_key: Vec<u8>,             // 写方向密钥
    write_nonce: u64,               // 写方向 nonce counter
    write_buf: Vec<u8>,             // 写方向待发送 buffer
    write_pos: usize,               // write_buf 的游标
    is_2022: bool,                  // 2022 edition stream
    request_salt: Vec<u8>,          // 入站: 请求 salt (回填到响应固定头); 出站: 发送的请求 salt (验证响应头)
    response_salt: Vec<u8>,         // 入站 2022: 响应 salt (与首个响应头 chunk 一起发出)
    write_response_header_pending: bool, // 入站 2022: 首次写入前需要先发送响应头
}
```

### 职责

- **TCP chunk 读取**：一次读取完整 AEAD chunk（2 字节长度 + payload + 16 字节 tag），解密后缓存供应用消费
- **TCP chunk 写入**：将要发送的数据切分成 AEAD chunk，加密后写入底层 stream
- **下载方向加解密**：使用 `read_key`/`read_nonce`
- **上传方向加解密**：使用 `write_key`/`write_nonce`
- **Response salt 生成**：inbound 侧为 server→client 方向生成独立的 response salt + download key
- **Chunk 边界处理**：正确处理跨 AEAD chunk 边界的大 payload

### Chunk 格式

```
[2-byte length (big-endian)][variable-length encrypted payload][16-byte AEAD tag]
```

Maximum chunk payload 为 0x3FFF (16383) 字节（legacy AEAD）；AEAD 2022 (blake3) 为 0xFFFF (65535) 字节。

### Inbound Stream 构造

Inbound accept 返回 `ShadowsocksAccept` 后，通过 `ShadowsocksAccept::into_aead_stream()` 构造 `ShadowsocksAeadStream`：

```rust
accept.into_aead_stream(stream, password)
// 或指定自定义 response salt:
accept.into_aead_stream_with_response_salt(stream, password, response_salt)
```

### Outbound Stream 构造

Outbound 通过 `ShadowsocksOutboundSession` 返回 session key 材料。通过 `ShadowsocksAeadStream::outbound()` 构造：

```rust
ShadowsocksAeadStream::outbound(stream, outbound_session, password)
```

## AEAD 2022 TCP（SIP022）

`CipherKind::is_blake3()` 派发到 SIP022 路径：`send_request_2022` 写入 `salt + fixed-header chunk (nonce 0) + variable-header chunk (nonce 1)`，body length/payload 对从 nonce 2 继续；inbound `accept_request_2022` 以**单次读取**接收 salt + fixed-header（SIP022 3.1.3 检测防御），失败时 drain 后再关闭。响应流的 fixed-header chunk（nonce 0，含 request-salt 回填）兼作首个 length chunk，首个 payload chunk 在 nonce 1。三个 blake3 cipher 均覆盖。

验证：TCP 入站已通过 `shadowsocks-rust` 参考客户端 `sslocal` 端到端互操作（HTTP 200）；SIP023 AES 2022 TCP/UDP 双向路径已通过 `shadowsocks-rust` 1.24.0 的 `sslocal` / `ssserver` 验证；常规 AEAD 与 2022 路径由 `is_blake3()` 在 `send_request` / `accept_request` / `ShadowsocksAeadStream` 中区分。

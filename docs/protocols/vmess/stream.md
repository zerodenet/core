# VMess Stream

对应 `protocols/vmess/src/stream.rs` — `VmessAeadStream`。

## VmessAeadStream

实现 `AsyncRead + AsyncWrite`，为 VMess TCP body relay 提供双向 AEAD 加解密。

```rust
pub struct VmessAeadStream<T> {
    inner: T,
    body_aead: BodyAead,            // 读方向 (上传)
    response_body_aead: BodyAead,   // 写方向 (下载)
    read_buf: Vec<u8>,
    // ...
}
```

### 职责

- **Body chunk 读取**: 解析 2 字节认证长度 → 读取 payload+tag → SHAKE128 unmask → AEAD decrypt → 缓存到 `read_buf`
- **Body chunk 写入**: 缓存写入数据 → 构造 chunk (认证长度 + AEAD encrypt + SHAKE128 mask) → 写入底层 stream
- **Shutdown 处理**: chunk 模式发送 termination chunk；标准 `zero` 直接关闭原始 body
- **Rekey**: 每 2^14 chunks 自动触发 BodyAead rekey

### Chunk 格式

```
[2-byte authenticated length][variable-length encrypted payload + 16-byte tag]
```

长度字段经过 SHAKE128 mask + AEAD 认证，防止长度篡改。

### Shutdown Sequence

1. `AsyncWrite::poll_shutdown()` → 写入 termination chunk
2. Termination chunk = 空 payload + `is_last=true` 标记
3. BodyAead 添加 global padding 到最后一个 chunk
4. 对端读到零长度 chunk = 流结束

### 与 ShadowsocksAeadStream 的区别

| 特性 | VMess | Shadowsocks |
|------|-------|-------------|
| Chunk 长度 | 2 字节认证长度 (masked) | 2 字节明文长度 |
| Masking | SHAKE128 XOR (all cipher modes) | 无 |
| Padding | Global padding (last chunk) | 无 |
| Rekey | 2^14 chunks | 无 |
| 非 AEAD cipher | `none`/`zero-plus` 保留 chunk；`zero` 为无 chunk 原始 body | 不适用 |

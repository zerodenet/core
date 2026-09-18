# VMess Crypto

对应 `protocols/vmess/src/crypto.rs` — Xray AEAD header seal/open、`BodyAead`、KDF 工具。

## KDF

### cmd_key 派生

```
cmd_key = KDF_SHA256(uuid, b"c48619fe-8f02-49e0-b9e9-edf763e17e21", key_len)
```

HMAC-SHA256 分层 KDF：
1. 第一轮：`HKDF_extract(salt=uuid, ikm=path)`
2. 后续轮：`HKDF_expand(prk, salt + path[i])`

### Auth ID 计算

```
auth_id = HMAC_SHA256(cmd_key, timestamp_be_bytes)[:16]
```

时间窗口：±30 秒，防止重放攻击。

## Xray AEAD Header

### seal_xray_aead_header

```rust
pub(crate) fn seal_xray_aead_header(
    cmd_key: &[u8],
    cipher: VmessCipher,
    command: u8,
    target: &Address,
    port: u16,
    timestamp: u64,
) -> Vec<u8>
```

1. 将 target、port、command、cipher 编码为 protobuf 格式
2. 添加随机 padding
3. 用派生 key 进行 AEAD 加密
4. 前面加上 16 字节 auth ID

### open_xray_aead_header

反向操作：验证 auth ID，解密 header，提取 target/port/command/cipher。

## BodyAead

```rust
pub struct BodyAead {
    key: Vec<u8>,
    nonce: Vec<u8>,
    cipher: VmessCipher,
    chunk_count: u64,
    // ...
}
```

### 核心特性

- **Authenticated length**: 每个 chunk 的 2 字节长度经过认证
- **SHAKE128 chunk masking**: SHAKE128 输出与 chunk payload/长度 XOR
- **Global padding**: 随机 padding 添加到最后一个 chunk
- **Periodic rekey**: 每 2^14 chunks 重新派生 key/nonce

### BodyAead 操作

```rust
impl BodyAead {
    pub fn seal_body_chunk(&mut self, data: &[u8], is_last: bool) -> Vec<u8>
    pub fn open_body_chunk(&mut self, chunk: &[u8]) -> Result<Vec<u8>>
    pub fn termination_chunk(&mut self) -> Vec<u8>  // 关闭 stream 的空 chunk
}
```

### Option Mode (非 AEAD cipher)

`none` 和 `zero-plus` cipher 同样走 chunk 格式（2 字节长度 + payload），但不加密和认证。`zero` 使用独立的原始 body 路径，不构造 chunk。

## Response Header

Xray response header 格式：
```
[1 byte status][optional padding]
```
- `0x00`: 成功
- 非零: 错误（连接拒绝）

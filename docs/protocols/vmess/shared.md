# VMess Shared

对应 `protocols/vmess/src/shared.rs` — `VmessCipher`、地址编码、UUID 解析、I/O 辅助函数。

## VmessCipher

定义位于 `protocols/vmess/src/validation.rs`：

```rust
pub enum VmessCipher {
    Aes128Gcm,        // "aes-128-gcm"
    Chacha20Poly1305, // "chacha20-poly1305"
    None,            // "none" — 明文 body，保留 chunk
    Zero,            // "zero" — Xray security 0x05，无 chunk 原始 body
    ZeroPlus,        // "zero-plus" — Zero 私有 security 0x06，保留 chunk
}
```

配置导入将 `auto` 归一化为 `aes-128-gcm`；它不是独立的 enum variant。

### zero 与 zero-plus

`zero-plus` 是原先名为 `zero` 的私有扩展，线路格式不变，仅承诺 Zero→Zero。
旧私有配置需要显式改为 `zero-plus`，不保留 `zero` 别名，避免继续混淆语义。

`zero` 使用 Xray 语义：security NONE（0x05），关闭 ChunkStream 和
ChunkMasking，body 不带 chunk。它有独立的原始 body 执行路径，不会静默映射为
仍保留 chunk 的 `none`。

固定参考：[Xray outbound](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/proxy/vmess/outbound/outbound.go#L107)。

## 地址编码

VMess address 格式遵循 VMess 规范：
- ATYP: 0x01 (IPv4), 0x02 (domain), 0x03 (IPv6)
- Domain 地址前缀 1 字节长度

## UUID 解析

标准 ASCII UUID 格式（带或不带破折号均可）；非 ASCII 输入在切片前拒绝，不会
触发 UTF-8 边界 panic。用于：
- KDF 派生 cmd_key（HMAC-SHA256 分层）
- Auth ID 计算
- Header encryption key 派生

## 命令类型

| Command | 值 | 用途 |
|---------|-----|------|
| `CMD_TCP` | 0x01 | TCP 代理 |
| `CMD_UDP` | 0x02 | UDP over stream |

## I/O 辅助

- `read_exact`: 带超时的精确读取
- VMess header 长度计算
- Response header 检查

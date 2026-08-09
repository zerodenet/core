# VMess

VMess 的主流传输基线（TLS / WS / gRPC）上的 TCP + UDP + MUX 已完整实现，外部互通覆盖 Xray、sing-box、Mihomo 三大实现族。模块结构与 `protocols/vmess/src/` 一一对应：

| 文档 | 对应源码 | 内容 |
|------|---------|------|
| [inbound.md](inbound.md) | `inbound.rs` | `VmessInbound`、`VmessAccept`、`VmessUser`、多用户认证 |
| [outbound.md](outbound.md) | `outbound.rs` | `VmessOutbound`、`VmessOutboundSession`、tunnel targets |
| [shared.md](shared.md) | `shared.rs` | `VmessCipher`、地址编码、UUID 解析 |
| [crypto.md](crypto.md) | `crypto.rs` | Xray AEAD header seal/open、`BodyAead`、KDF |
| [stream.md](stream.md) | `stream.rs` | `VmessAeadStream` 双向 AEAD body relay |
| [mux.md](mux.md) | `mux.rs` | `MuxFrame`、`VmessMuxStream`、Mux.Cool 连接池 |
| [udp.md](udp.md) | `udp.rs` | `VmessUdpPacket`、payload 模式、response bridge |
| [metadata.md](metadata.md) | `metadata.rs` | `VmessProtocol` 能力描述符、limitations |

## 能力分级总览

VMess 当前能力等级为 `supported`：主流传输基线（TLS / WS / gRPC）上的 TCP + UDP + MUX 已完整实现。`cipher: zero` 是明确标注的 Zero 扩展，不属于 `xray_core_vmess_aead` 兼容基线；主流部署应选择 `aes-128-gcm`、`chacha20-poly1305` 或 `none`。

### 基础能力

| 能力 | 状态 | 说明 |
|------|------|------|
| TCP inbound over TLS | ✅ 完成 | 单用户 + 多用户认证，`InboundProtocol` 接入 |
| TCP outbound over TLS | ✅ 完成 | `TcpSessionProtocol` 集成 |
| Xray AEAD header | ✅ 完成 | KDF (HMAC-SHA256 分层)、auth ID、header seal/open |
| Body AEAD chunk relay | ✅ 完成 | `VmessAeadStream` 双向加解密 |
| 4 个 cipher | ✅ 完成 | `aes-128-gcm`、`chacha20-poly1305`、`none`、`zero` |
| `auto` 归一化 | ✅ 完成 | 配置导入时 `auto` → `aes-128-gcm` |
| 配置校验 + feature gate | ✅ 完成 | UUID 解析、cipher 校验、TLS required for inbound |
| 统一 runtime 接入 | ✅ 完成 | InboundProtocol trait + `serve_inbound()` |
| 运行时可观测 | ✅ 完成 | session 生命周期、统计、事件 |

### 完整（supported 级，对标大众）

| 能力 | 状态 | 说明 |
|------|------|------|
| WebSocket over TLS 传输 | ✅ 完成 | 双向，Xray WS 互通已验证 |
| gRPC over TLS 传输 | ✅ 完成 | 双向，Xray gRPC 互通已验证 |
| UDP over stream (CMD_UDP) | ✅ 完成 | 两种 payload 模式 |
| Mux.Cool TCP sub-connection | ✅ 完成 | 连接池按 (server, port, uuid, cipher, transport) 分桶 |
| Mux.Cool UDP sub-connection | ✅ 完成 | 两种 payload 模式 |
| 多用户认证 | ✅ 完成 | 缓冲读取 + 多密钥尝试 |
| Body AEAD 高级特性 | ✅ 完成 | Authenticated length、SHAKE128 masking、global padding、2^14 rekey |
| `cipher: none` | ✅ 完成 | Xray TCP 互通已验证 |
| VMess→VMess UDP relay chain | ✅ 完成 | 同协议链路 |
| 外部互通 (Xray/sing-box/Mihomo) | ✅ 完成 | 三大家族全覆盖 |
| `cipher: zero` | Zero 扩展 | 只承诺 Zero→Zero，不作为 Xray 兼容能力 |

### 特级（Zero 特色）

| 能力 | 说明 |
|------|------|
| MUX 连接池 | 按 (server, port, uuid, cipher, transport) 分桶复用 |
| Broadcast UDP response bridge | 多个 UDP session 共享同一条 VMess UDP stream |
| UDP payload-mode 自动检测 | Inbound 自动检测 VMess packet vs raw datagram |
| 多用户缓冲读取 + early reject | 一次读取 wire data，多密钥依次尝试 |
| 统一 InboundProtocol 入口 | Raw TLS / WS / gRPC 共用同一管线 |

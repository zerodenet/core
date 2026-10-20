# VMess

VMess is the V2Ray/Xray-family proxy protocol implementation used by Zero.
This crate owns VMess protocol behavior: AEAD request/response headers, body
framing, TCP stream state, UDP packet framing, and Mux.Cool frame encoding.

## Current State

| Capability | State |
|------|------|
| TCP handshake | experimental |
| TCP body stream | experimental |
| TCP inbound | experimental |
| TCP outbound | experimental |
| UDP-over-stream | experimental |
| Mux.Cool TCP sub-connection | experimental |
| Mux.Cool UDP sub-connection | experimental |
| Body AEAD authenticated length | implemented |
| Body AEAD chunk masking (SHAKE128) | implemented |
| Body AEAD global padding | implemented |
| Body AEAD periodic rekey (2^14 chunks) | implemented |
| `cipher: auto` | normalized to the current AEAD baseline |
| `cipher: none` | implemented; Xray TCP interoperability is covered |
| `cipher: zero` | implemented with Xray's unchunked NONE body semantics |
| `cipher: zero-plus` | implemented for Zero-to-Zero paths; not claimed as mainstream external compatibility |

Supported explicit cipher names:

- `aes-128-gcm`
- `chacha20-poly1305`
- `none`
- `zero`
- `zero-plus`

`auto` is accepted as a config/import alias and maps to the current AEAD
baseline.

## Boundary

Protocol handshake, key derivation, response validation, body chunk
encryption/decryption, VMess UDP packet encoding, and MUX frame encoding stay in
this crate. The proxy runtime owns transport connection setup, routing,
session lifecycle, stats, events, and wrapping authenticated streams in
`VmessAeadStream` or `VmessMuxStream`.

In-tree validation covers bidirectional body relay for explicit ciphers,
shutdown termination chunks, UDP packet framing, raw TLS, WSS, gRPC, TCP MUX,
UDP-over-stream, MUX UDP, and same-protocol VMess UDP relay-chain paths.

External interoperability currently covers Xray TCP and UDP in both directions,
Xray WS/gRPC TCP in both directions, Zero outbound to sing-box inbound TCP/UDP,
and Mihomo outbound to Zero inbound TCP/UDP. `cipher: zero-plus` remains a
Zero-to-Zero capability, not a mainstream external compatibility claim.

## File Layout

```text
src/lib.rs       - crate root and re-exports
src/crypto.rs    - key derivation, AEAD header seal/open, body AEAD state (BodyAead), rekey, padding, chunk masking
src/inbound.rs   - inbound accept with single-user and multi-user auth, response header
src/outbound.rs  - outbound TCP session establishment, request encoding
src/stream.rs    - VmessAeadStream (AsyncRead + AsyncWrite) for bidirectional body relay
src/shared.rs    - cipher enum, address helpers, constants, read_exact, shared outbound session helpers
src/metadata.rs  - protocol capability descriptor
src/mux.rs       - Mux.Cool frame encode/decode, VmessMuxStream
src/udp.rs       - UDP packet encode/decode, UDP packet-session and UDP-over-stream establishment
```

`zero` now means Xray's unchunked NONE body semantics. Existing Zero-private
`zero` configurations must explicitly migrate to `zero-plus`; the private wire
format remains unchanged.

## 标准 Stream 接入

`runtime` 的 `component::Options` 在冷准备中解析 UUID/cipher、保留完整的普通
TLS/WS/gRPC profile；Factory 的 describe/build 不打开载体或启动任务。非 MUX
连接直接使用 Destination 和 OpenContext 身份，不构造代理 Session 占位对象。
单节点和全原生 relay 使用同一个真实 Stream role。

协议请求写入和 flush 后返回 `PeerConfirmation::Deferred`。原有
`VmessAeadStream` 继续认证加密响应头；只有实际验证成功才报告 PeerConfirmed，
认证失败报告 PeerRejected 并保留原始 I/O 错误来源。请求字节与 TLS/WS/gRPC
载体计量分开，现有 cipher、framing 和半关闭语义保持不变。

CA 文件现有独立库 Factory 入口 `Options::from_options_refs_at`：调用方必须显式
给出非零有限读取上限和可选配置目录。相对路径按原 `base_dir.join(path)` 处理，
不会 canonicalize 为额外权限。describe 声明精确 File 资源，start 通过真实
ResourceAccess 读取一次授权快照并准备 TLS 信任；connect 不再打开 CA 路径，
继续使用原 verifier、ALPN、fingerprint、resumption 及 WS/gRPC authority。
仅真实 Rustls 子集可用；OpenSSL/legacy TLS 明确拒绝。默认 Options 构造器没有
隐含上限或授权，仍拒绝 CA。生产资格仍排除 CA，等待 host 资源图和替换确认。

MUX、DNS 获取 ECH 和 browser_dialer 需要额外共享资源或宿主服务，仍走原有
真实执行路径；直接传入 Options 返回 Unsupported。当前中立 DNS 服务仅提供
A/AAAA，无法代替 ECH 的 HTTPS RR、材料缓存、force-query 和实际查询路由。
UDP 及包含其他旧协议的混合链路尚未迁移到此角色。

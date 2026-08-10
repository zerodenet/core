# VMess Metadata

`protocols/vmess/src/metadata.rs` 是运行时 `capabilities.protocols` 的权威来源。

| 字段 | 值 | 说明 |
|------|-----|------|
| `status` | `supported` | AEAD 基线的 TCP、UDP、MUX 与公开传输均已实现 |
| `inbound.tcp` / `outbound.tcp` | `supported` | Raw TLS、WS+TLS、gRPC+TLS |
| `inbound.udp` / `outbound.udp` | `supported` | CMD_UDP packet/raw 模式及同协议 relay chain |
| `transports` | `tcp`, `tls`, `ws`, `grpc` | 当前公开 VMess 传输面 |
| `mux` | `supported` | Mux.Cool TCP/UDP 子连接与连接池 |
| `limitations` | 空 | 验证覆盖不再与实现能力混为一谈 |

兼容基线是 `xray_core_vmess_aead`。`cipher: zero` 是 Zero 专用扩展，只承诺 Zero→Zero；它不是基线缺口，也不应作为 Xray/sing-box/Mihomo 兼容配置的默认值。

外部互操作入口保留在 `crates/proxy/tests/vmess_xray_interop.rs` 并默认忽略。CI 内置测试覆盖各 cipher、TCP/UDP、Mux.Cool 及 TLS/WS/gRPC 组合。

# Trojan

Trojan 是 `partial` 协议能力。基线 TCP 和 UDP-over-stream 路径存在。模块结构与 `protocols/trojan/src/` 一一对应：

| 文档 | 对应源码 | 内容 |
|------|---------|------|
| [inbound.md](inbound.md) | `inbound.rs` | `TrojanInbound`、`TrojanAccept`、TLS 入站 |
| [outbound.md](outbound.md) | `outbound.rs` | `TrojanOutbound`、`TrojanTcpTunnelTarget`、`TrojanUdpPacket` |
| [shared.md](shared.md) | `shared.rs` | 密码/请求/地址读写、CMD_TCP/CMD_UDP 常量 |
| [metadata.md](metadata.md) | `metadata.rs` | `TrojanProtocol` 能力描述符、limitations |

## 当前能力

| 能力 | 状态 | 说明 |
|------|------|------|
| TCP 入站 | `supported` | TLS 入口 + Trojan TCP 请求 |
| TCP 出站 | `supported` | Trojan TCP 上游 |
| UDP 入站 | `partial` | Trojan UDP-over-stream |
| UDP 出站 | `partial` | 单跳及 TCP relay-prefix final-hop 路径 |
| MUX | `unsupported` | Trojan MUX 未实现 |

## 剩余缺口

- 外部互操作覆盖不足
- MUX 未实现

中继链最终跳的 TLS 客户端指纹预设已支持：经 `connect_tls_stream` 在已建立的 TCP 中继流上应用与单跳一致的 rustls cipher/ALPN 预设（见 e2e 测试 `relays_udp_through_socks5_to_trojan_relay_chain_with_tls_fingerprint`）。该字段当前不承诺完整复刻浏览器 ClientHello 扩展顺序。

## 外部互操作

互操作测试文件：`crates/proxy/tests/trojan_xray_interop.rs`（8 个测试，Xray/sing-box/Mihomo，本地手动执行）。

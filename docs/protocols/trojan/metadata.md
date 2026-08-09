# Trojan Metadata

`protocols/trojan/src/metadata.rs` 是运行时 `capabilities.protocols` 的权威来源。

| 字段 | 值 | 说明 |
|------|-----|------|
| `status` | `supported` | 公开配置承诺的 TCP、UDP 和 MUX 均已接线 |
| `inbound.tcp` / `outbound.tcp` | `supported` | TLS + Trojan TCP |
| `inbound.udp` / `outbound.udp` | `supported` | CMD_UDP 与 Mux.Cool UDP |
| `transports` | `tcp`, `tls` | Trojan 必须运行在 TLS 上 |
| `mux` | `supported` | Mux.Cool TCP/UDP 子流 |
| `limitations` | 空 | 外部互通覆盖作为验证证据单独维护，不再冒充实现缺口 |

Zero→Zero 回归覆盖普通与 MUX 模式下的 TCP、UDP、连接复用、回程限额及配置校验。外部 Xray/sing-box/Mihomo 测试入口继续保留为默认忽略测试。

# Trojan Metadata

对应 `protocols/trojan/src/metadata.rs` — `TrojanProtocol` 实现 `ProtocolMetadata` trait。

## 能力描述符

```rust
impl ProtocolMetadata for TrojanProtocol {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ProtocolCapabilityDescriptor {
            protocol: "trojan",
            feature: "trojan",
            compatibility_baseline: "trojan_go",
            status: ProtocolCapabilityLevel::Partial,
            inbound: DirectionalCapability {
                tcp: ProtocolCapabilityLevel::Supported,
                udp: ProtocolCapabilityLevel::Partial,
            },
            outbound: DirectionalCapability {
                tcp: ProtocolCapabilityLevel::Supported,
                udp: ProtocolCapabilityLevel::Partial,
            },
            transports: vec!["tcp", "tls"],
            mux: ProtocolCapabilityLevel::Unsupported,
            limitations: vec!["external_interop_coverage_is_incomplete"],
        }
    }
}
```

## 状态解释

| 字段 | 值 | 理由 |
|------|-----|------|
| `status` | `partial` | TCP 基线完成，UDP partial，MUX unsupported |
| `inbound.tcp` | `supported` | TLS ingress + Trojan request |
| `inbound.udp` | `partial` | UDP-over-stream 入站 |
| `outbound.tcp` | `supported` | TLS + Trojan request 出站 |
| `outbound.udp` | `partial` | 单跳 + TCP relay-prefix final-hop |
| `transports` | `["tcp", "tls"]` | |
| `mux` | `unsupported` | Trojan MUX 不在范围内 |

## 剩余缺口

- `external_interop_coverage_is_incomplete`
- `mux_udp_is_not_implemented` (Trojan 无 MUX)

## 外部互操作

互操作测试文件：`crates/proxy/tests/trojan_xray_interop.rs`（Xray/sing-box/Mihomo 三大族，本地手动执行，`#[ignore]`）。

TLS 客户端指纹预设（`client_fingerprint`，chrome/firefox/safari/ios/edge/randomized）在 TCP 出站、UDP fresh-socket、以及 **relay-stream**（UDP relay-chain 末跳）三条路径上均已支持，当前应用 rustls cipher/ALPN 预设，不承诺完整浏览器 ClientHello 仿真。回归测试见 `socks5_udp/relays_udp_through_socks5_to_trojan_relay_chain_with_tls_fingerprint.rs`（CI 自跑）。

# Shadowsocks Metadata

对应 `protocols/shadowsocks/src/metadata.rs` — `ShadowsocksProtocol` 实现 `ProtocolMetadata` trait。

## 能力描述符

```rust
impl ProtocolMetadata for ShadowsocksProtocol {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        let unsupported = ProtocolCapabilityState::unsupported(&[]);
        let supported = ProtocolCapabilityState::supported();

        ProtocolCapabilityDescriptor {
            protocol: "shadowsocks",
            feature: "shadowsocks",
            compatibility_baseline: "shadowsocks_rust_sip022_sip023",
            status: ProtocolCapabilityLevel::Partial,
            inbound: ProtocolNetworkCapability::new(supported, supported),
            outbound: ProtocolNetworkCapability::new(supported, supported),
            transports: &["tcp", "udp"],
            mux: unsupported,
            limitations: &[
                "shadowsocks_2022_hardening_not_externally_validated",
            ],
        }
    }
}
```

## 状态解释

| 字段 | 值 | 理由 |
|------|-----|------|
| `status` | `partial` | SIP022 全部章节已实现并通过内置测试；剩余为安全加固外部验证 |
| `inbound.tcp` | `supported` | 常规 AEAD + AEAD 2022 TCP inbound accept（SIP022 头部+检测防御+重放） |
| `inbound.udp` | `supported` | UDP datagram framing for inbound（含 2022 UDP server response + 滑动窗口 + 按 session id 隔离） |
| `outbound.tcp` | `supported` | 全 cipher TCP outbound |
| `outbound.udp` | `supported` | 全 cipher UDP outbound (已外部验证) |
| `transports` | `["tcp", "udp"]` | Shadowsocks 不使用 transport 层抽象 |
| `mux` | `supported = false` | Shadowsocks 未实现 MUX |

## 限制

| Limitation 码 | 含义 |
|---------------|------|
| `shadowsocks_2022_hardening_not_externally_validated` | SIP022 全部章节及 SIP023 AES EIH 已实现，并完成 `shadowsocks-rust` 1.24.0 TCP/UDP 双向互操作；主动探测和重放攻击验证仍未完成 |

## 外部互操作状态

- UDP outbound 已完成与 `shadowsocks-rust ssserver -U` 的所有 6 个 cipher 外部互通
- AEAD 2022 TCP **入站**已通过 `shadowsocks-rust` 参考客户端 `sslocal` 端到端互操作验证（HTTP 200）
- SIP023 AES 2022 TCP/UDP **双向**路径已通过 `shadowsocks-rust` 1.24.0 端到端验证，覆盖 `sslocal` → Zero 与 Zero → `ssserver` 的两个 AES 方法
- AEAD 2022 **UDP server response** 已通过手动探针验证（DNS 往返 + 滑动窗口重放拒绝）：`protocols/shadowsocks/examples/udp_server_probe.rs`
- SIP022 3.2.4 的按客户端 session id 隔离 UDP 中继流已实现
- 新增的检测防御/drain 与滑动窗口对抗真实主动探测/重放攻击的外部验证尚未完成

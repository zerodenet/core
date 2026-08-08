# VLESS

> 参照 Xray-core | Crate: `vless`

VLESS 是 Xray 项目的核心入站/出站协议，无状态、轻量级，使用 UUID 身份验证，依赖外层 TLS 提供加密，自身不实现加密层。

## 协议来源

| 项目 | 来源 |
|------|------|
| 参照实现 | [Xray-core](https://github.com/XTLS/Xray-core) |
| 协议头 | `version` 字段恒为 `0x00` |
| 本实现 | `vless` crate |

## 协议帧

```
[version:1][uuid:16][addon:1+M][command:1][port:2][address_type:1][address:?]
```

- **version** — 始终 `0x00`
- **uuid** — 16 字节用户标识
- **addon** — 可选扩展（含 Flow），首字节决定是否存在
- **command** — `0x01` TCP、`0x02` UDP、`0x03` MUX
- **address** — IPv4 / IPv6 / 域名

## 功能对齐状态

### 命令分发

| 命令 | 状态 |
|------|------|
| TCP (`0x01`) | ✅ |
| UDP (`0x02`) | ✅ |
| MUX (`0x03`) | ✅ |

### Flow

| Flow | 状态 |
|------|------|
| `xtls-rprx-vision` | Xray-compatible outbound TCP over REALITY |
| `zero-aead-v1` | Zero-private request AEAD; not XTLS Vision |
| `xtls-rprx-vision-udp443` | Rejected as obsolete; migrate explicitly |

### UDP

| 特性 | 状态 |
|------|------|
| UDP v1/v2 包格式 | ✅ |
| 链式全传输 | ✅ |
| Per-target upstream + 异步轮询 | ✅ |
| 会话空闲超时 | ✅ |

### MUX

| 特性 | 状态 |
|------|------|
| Xray 兼容帧 + 连接池 + per-stream AES-128-GCM | ✅ |
| MUX UDP 子连接 | ❌ |

### 传输层

| 传输 | 入站 | 出站 |
|------|------|------|
| TCP / TLS / Reality / WS / gRPC / H2 / QUIC / HTTPUpgrade / SplitHTTP | ✅ | ✅ |
| DomainSocket | ❌ 不计划 |

### Fallback

| SNI/ALPN 探测 + 非 TLS 流量回落 | ✅ |

### TLS 指纹

Reality 客户端通过 `ztls` 构造可控 ClientHello。`reality.client_fingerprint` 支持 `chrome`、`firefox`、`safari` 和 `edge`，默认 `chrome`；短名称固定到版本化模板，控制 TLS 1.3 cipher suite、扩展顺序、supported groups 和 ALPN。不同指纹会形成不同的 MUX 连接池键，避免复用已有的不同线特征载体。

普通 TLS 传输继续使用 `zero-transport` 的 rustls profile；Reality 指纹配置只作用于 Reality 客户端，不改变入站 Reality 或普通 TLS 配置。

## 待实现

### MUX UDP
`handle_vless_mux_session` 将所有 MUX 子流创建为 `Network::Tcp`。Xray 支持 `network=0x02`（UDP）子连接。

### QUIC 0-RTT
Xray 支持 QUIC 0-RTT 握手加速，当前使用标准握手。

## 架构

```
protocols/vless/src/
├── lib.rs            # crate root, re-exports
├── inbound.rs        # VlessInbound: accept, auth, session dispatch
├── outbound.rs       # VlessOutbound: TCP request and MUX handshake
├── flow.rs           # Vision flow (AES-128-GCM encrypt/decrypt)
├── mux.rs            # MUX frame, client, server
├── shared.rs         # common: frame parsing, address, UUID, shared request helpers
├── udp.rs            # VLESS UDP tunnel, packet framing, flow/session state
├── mux_crypto.rs     # MUX per-stream AES-128-GCM crypto
├── mux_pool.rs       # MUX connection pool
├── metadata.rs       # protocol capability descriptor
├── deferred_response.rs  # DeferredVlessResponseStream (Reality flow)
└── reality/          # Reality TLS 1.3
    ├── mod.rs
    ├── reality_auth.rs
    ├── reality_client_connection.rs
    ├── reality_client_verify.rs
    ├── reality_server_connection.rs
    ├── reality_util.rs
    └── stream.rs
```

## 参考

- [Xray-core VLESS 出站配置](https://xtls.github.io/en/config/outbounds/vless.html)
- [VLESS 协议规范](https://github.com/XTLS/Xray-core/discussions/1967)

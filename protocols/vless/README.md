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

## 标准 Stream 接入范围

与现有 transport 相同的 `runtime` + `reality` 构建提供协议拥有的
`component::Options` / `Factory`。已接入普通无 flow 的 TCP、TLS、WS（含早期
数据）、gRPC，以及 TLS 1.3 直载的 Vision；单节点及全原生 relay 直接通过 Stream role 使用真实
Destination 和 Service/Business 身份，不创建 Session 占位对象。UUID 与自定义
ID 沿用本协议解析，普通请求帧复用原编码函数。build 不打开连接或启动任务。

提交请求并真实 flush 后返回 Deferred。现有 DeferredVlessResponseStream 在首次
读取中检查版本并完整消费 addons，完成后才上报 PeerConfirmed；非法版本上报
PeerRejected，截断仍保留 EOF 错误。Flow 只记录请求帧，Outer 保留实际载体字节。

暂未迁移的边界明确保留原有实际路径；直接传入这些项的 Options 返回错误：

- zero-aead flow 尚需独立认证流接入；Vision 目前只接受实际 TLS 1.3 直载，不假接 WS/gRPC 绕过控制。
- REALITY 需要完整认证、密钥 profile、拒绝及 decoy 导航语义的独立接入回归。
- VLESS Encryption，尤其 0RTT ticket cache，需要协议状态代际和共享所有者。
- MUX/XUDP、testpre、reverse、browser_dialer 需要相应资源和后台生命周期。
- QUIC、mKCP、Hysteria、FinalMask 需要真实 UDP/socket、驱动或 framing 接入。
- XHTTP 需要实际多载体前缀、download 配置及连接池/驱动生命周期；H2、HTTP Upgrade 尚未接入本 Factory。
- CA 文件和 DNS 获取的 ECH 配置等待 File grant 和原生 DNS 服务，不能用缺省 source_dir 或丢弃字段代替。

TLS Vision 保留原 flow protobuf（UDP443 后缀仍只影响 UDP 策略）、testseed
归一化和 VisionStream 的 padding/record-boundary 切换。实际 TLS 握手认证完成
后必须取得真实 switchable control；协商到 TLS 1.2 会在发送 VLESS 请求前返回
Unsupported。动态 Stream 擦除前把该句柄交给协议流，未用空控制器代替。

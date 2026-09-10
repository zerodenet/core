# Mieru

> 实现对标：官方 Mieru **v3.33.0** | Crate: `mieru` **3.33.0**

Mieru 是一种加密代理协议。它先与对端建立一条 XChaCha20-Poly1305 加密隧道（基于用户名/密码/系统时间派生密钥），**然后在隧道内用 socks5 协商代理目标并 relay**。这与 vless / trojan / shadowsocks 等"目标在握手时确定"的协议不同——mieru 的 mieru 会话本身不携带目标。

## 协议模型（socks5-in-tunnel）

```
应用 ── Zero socks5/HTTP/... 入站（解析出 target）
         │
         │ mieru 出站：建立加密隧道（openSession，仅 sessionID + 用户身份）
         │ 然后在隧道里直接发 socks5 请求：[05, CMD, 0, ATYP, addr, port]
         ↓
              mieru 加密会话（XChaCha20-Poly1305）
         ↓
              对端（mita）在隧道终点读 socks5 请求 → 拨目标 → relay
```

关键点：

- openSessionRequest 的控制元数据不包含目标；其可选 payload 可以携带隧道内 SOCKS5 请求。用户身份经密钥派生和 nonce user hint 体现。
- **隧道内的 socks5 不做 greeting / method / user-pass 认证**——mieru 会话本身即认证（对端 `ClientSideAuthentication`）。客户端直接发 socks5 请求，读 socks5 响应。
- 密钥派生：`key = PBKDF2-HMAC-SHA256(SHA-256(password ‖ 0x00 ‖ username), SHA-256(uint64_be(时间取整 2 分钟)), 64 iter, 32 bytes)`。
- segment 成帧：session segment **无前缀 padding0**（nonce 在偏移 0）；padding 是 suffix。

## 协议来源

| 项目 | 来源 |
|------|------|
| 参照实现 | [mieru v3.33.0](https://github.com/enfein/mieru/tree/v3.33.0) |
| 固定提交 | `48ddb69d5d343d76c9004c5054ee36609579ba13` |
| 本实现 | `mieru` crate `3.33.0`，整体 `partial` |

包版本标识唯一的实现对标版本。功能差距以该版本源码为准，额外版本互通不改变基线，也不代表功能已经完整对齐。基线检查入口为 `python3 scripts/prepare-mieru-interop.py --check-baseline`；详细对照见 [parity.md](../../docs/protocols/mieru/parity.md)。

## 功能对齐状态

| 特性 | 状态 |
|------|------|
| TCP 加密隧道（openSession 握手） | ✅ 已与 mita 互通验证 |
| TCP 出站：socks5-in-tunnel 目标协商 + relay | ✅ 已与 mita 端到端互通验证（httpbin.org） |
| TCP 入站：socks5-in-tunnel（对称于出站） | ✅ loopback 验证（对已验证出站） |
| UDP 出站：socks5 UDP ASSOCIATE | ✅ 已与 mita 互通验证（DNS relay） |
| UDP 入站：socks5 UDP ASSOCIATE | ✅ 已实现（对称设计） |
| 密钥派生（HashPassword）+ nonce user hint | ✅ 已与 mita 字节级对齐 |
| TCP 入站 MUX（同一 underlay 多会话） | 已实现；协议拥有 cipher/session 分发，内核拥有并发握手与路由任务 |
| 出站 underlay 连接池 | 已实现 TCP/UDP 载体共享、并发建连合并、身份隔离与重载退役 |
| 原生 UDP underlay | 已实现双向可靠分片、累计 ACK、窗口、重传及乱序去重；配置 `transport: "udp"` |

## 架构

```
src/lib.rs       — crate root, re-exports
src/inbound.rs   — MieruInbound（openSession 握手，不含目标）
src/outbound.rs  — MieruOutbound（建立加密隧道，不含目标）
src/tunnel.rs    — socks5-in-tunnel CONNECT / UDP ASSOCIATE 目标协商
src/segment.rs   — segment 成帧（build/parse，无前缀 padding0）
src/crypto.rs    — 密钥派生（HashPassword）+ XChaCha20-Poly1305 + nonce user hint
src/udp.rs       — UDP associate wrap/unwrap
src/protocol.rs  — ProtocolCapabilityDescriptor + TcpSessionProtocol
src/metadata.rs  — segment metadata 编解码
src/session.rs   — 会话状态（seq/window/timestamp）
```

协议私有的 socks5-in-tunnel 编排（TCP CONNECT / UDP ASSOCIATE 目标协商）由 `protocols/mieru/src/tunnel.rs` 负责；`crates/proxy/src/adapters/mieru/*` 只保留 carrier socket 生命周期、runtime pipe 调用和 trait bridge。

## 参考

- [mieru](https://github.com/enfein/mieru)

当前整体能力为 `partial`。本批功能与可复现验收见 [多会话、共享连接池和 UDP 载体](../../docs/protocols/mieru/multiplex.md)。

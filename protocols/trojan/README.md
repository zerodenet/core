# Trojan

> 参照 trojan-go | Crate: `trojan`

Trojan 是一种基于 TLS 的代理协议，通过将代理流量伪装为 HTTPS 来绕过检测。

## 协议来源

| 项目 | 来源 |
|------|------|
| 参照实现 | [trojan-go](https://github.com/p4gefau1t/trojan-go) |
| 本实现 | `trojan` crate |

## 功能对齐状态

| 特性 | 状态 |
|------|------|
| TCP: TLS 伪装 + 密码认证 + connect header | ✅ |
| TCP 链式出站 (TLS → upstream) | ✅ |
| UDP: CMD_UDP → TLS stream → framed relay | ✅ |
| 入站: accept + 密码验证 → route → relay | ✅ |
| 出站: TLS connect + send header → relay | ✅ |
| UDP 链式出站 | ✅ |
| WebSocket / gRPC 载体 | ✅ |

## 待实现

| 特性 | 状态 |
|------|------|
| Trojan-Go 多路复用 (smux) | ❌ 未计划 |

## 架构

`runtime` 提供协议拥有的 `component::Options` 和 `component::Factory`。
普通非 MUX 出站通过同一个标准 `StreamOutbound.connect` 建立单节点和 relay
连接；Options 在冷准备时复制 TLS/WS/gRPC profile 并预计算密码摘要，build
不执行网络 I/O。通用 transport 只负责载体，Trojan 自己根据请求的真实
Destination 写出协议帧。TLS 的验证、SNI、默认 chrome 指纹和 gRPC h2 ALPN
沿用原有 profile 语义。

Trojan 没有成功 CONNECT 应答，因此返回 `PeerConfirmation::Deferred`，本地
write/flush 只记录提交事实。协议握手计量只包含 Trojan 请求帧，Outer 仍记录
实际 TLS/WS/gRPC 字节。标准接口接受 pinned、非 Sync 载体，不产生额外转发任务。

MUX 需要共享连接资源的宿主生命周期，暂不进入此无共享资源 Factory；UDP、
MUX 及尚含其他旧协议节点的链路保留原有执行路径。该阶段不表示这些路径已迁移。

```
src/lib.rs       — crate root, re-exports
src/component/   — cold Options, standard Stream role, pinned carrier adapter
src/inbound.rs   — TrojanInbound (TLS accept, auth, route)
src/outbound.rs  — TrojanOutbound TCP request/header path
src/shared.rs    — shared: request/response format and framing helpers
src/udp.rs       — Trojan UDP request, framing, flow, responder state
```

## 参考

- [trojan-go](https://github.com/p4gefau1t/trojan-go)
- [Trojan 协议规范](https://trojan-gfw.github.io/trojan/protocol)

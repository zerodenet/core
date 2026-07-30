# VLESS

Zero 的 VLESS 实现覆盖 TCP 入站与出站，并通过通用传输层组合 TLS、REALITY、WebSocket、gRPC、HTTP/2、XHTTP 和 QUIC 等载体。UDP 与 MUX 能力按具体路径标记，不由“协议已支持”推导所有组合均可用。

## 能力摘要

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| TCP 入站 | `supported` | 鉴权后进入通用流路由 |
| TCP 出站 | `supported` | 支持单跳与中继最终跳 |
| UDP | `partial` | 包含 stream 和 MUX 路径，受传输组合约束 |
| MUX | `partial` | 按 TCP/UDP 子流分别验证 |

## 文档

- [入站](./inbound.md)
- [出站](./outbound.md)
- [公共约定](./shared.md)
- [公开配置参考](https://docs.zerodenet.org/projects/core/configuration/)

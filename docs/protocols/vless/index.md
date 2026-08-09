# VLESS

Zero 的 VLESS 实现覆盖 TCP 入站与出站，并通过通用传输层组合 TLS、REALITY、WebSocket、gRPC、HTTP/2、XHTTP 和 QUIC 等载体。UDP 与 MUX 能力按具体路径标记，不由“协议已支持”推导所有组合均可用。

## 能力摘要

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| TCP 入站 | `supported` | 鉴权后进入通用流路由 |
| TCP 出站 | `supported` | 支持单跳与中继最终跳 |
| UDP | `supported` | 包含标准 stream 与 MUX/XUDP 路径；非法传输组合在配置阶段拒绝 |
| MUX | `supported` | TCP 子流与多目标 XUDP 均接入统一运行时 |

## 文档

- [入站](./inbound.md)
- [出站](./outbound.md)
- [公共约定](./shared.md)
- [公开配置参考](https://docs.zerodenet.org/projects/core/configuration/)

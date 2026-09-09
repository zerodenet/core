# Mieru

Zero 的 Mieru 实现使用 Mieru 载体建立隧道，并在隧道内执行 SOCKS5 CONNECT 或 UDP ASSOCIATE 控制流程。隧道协商和数据阶段编解码属于 `protocols/mieru`，通用代理层只负责打开载体、路由和转发。

协议自己的 accepted session 实现内核 `InboundStreamRoute` 契约，适配器将它交给中立运行时，
不在 proxy 中拆解 TCP/UDP 协议状态。当前底层仍为逐次建立的 TCP 隧道；表中的 UDP 指业务
UDP ASSOCIATE，不代表官方 UDP underlay 已接通。底层多 session MUX/连接池尚未实现，
后续协议帧、nonce、session 分发及复用策略仍属于 `protocols/mieru`。

## 能力摘要

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| TCP 入站 | `supported` | 隧道内 CONNECT |
| TCP 出站 | `supported` | 通过协议所有的 tunnel session 连接目标 |
| UDP 入站 | `supported` | 隧道内 UDP ASSOCIATE |
| UDP 出站 | `supported` | 协议所有的 UDP 会话与数据帧 |

## 文档

- [入站](./inbound.md)
- [出站](./outbound.md)
- [会话流程](./flow.md)

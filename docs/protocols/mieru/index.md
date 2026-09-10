# Mieru

实现对标版本固定为官方 **v3.33.0**（提交 `48ddb69d5d343d76c9004c5054ee36609579ba13`），与协议包 `mieru 3.33.0` 一一对应。其他版本的互通记录属于补充兼容性证据，不改变能力对照基线。

Zero 的 Mieru 实现使用 Mieru 载体建立隧道，并在隧道内执行 SOCKS5 CONNECT 或 UDP ASSOCIATE 控制流程。隧道协商和数据阶段编解码属于 `protocols/mieru`，通用代理层只负责打开载体、路由和转发。

TCP/UDP 入站均支持同一底层连接承载多个独立逻辑会话，协议实现内核
`InboundRouteMultiplexer`，每条会话的 TCP/UDP 分类继续使用 `InboundStreamRoute`。
通用运行时拥有并发握手和路由任务，协议拥有 cipher、nonce、session ID 和帧分发。
出站共享载体池承载 TCP CONNECT 与 UDP ASSOCIATE。入站和出站协议配置均可选择
`"transport": "tcp"`（默认）或 `"transport": "udp"`。原生 UDP 载体使用协议自己的可靠传输。
MUX 能力为 `supported`；长期运行和网络切换尚未完成验证，整体仍为 `partial`。
TCP relay 末跳复用按整条前缀链隔离的 Mieru 载体；原生 UDP relay 可使用前一跳提供的
datagram carrier。仅提供字节流的 relay hop 仍不能承载原生 UDP，遇到这种组合会明确失败。

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
- [多会话、共享连接池与 UDP 载体](multiplex.md)
- [官方实现差距与验收边界](parity.md)

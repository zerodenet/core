# Mixed

`mixed` 是 Zero 的入站复用器，在同一个监听端点识别 SOCKS5 和 HTTP。它不定义新的线上协议，也没有对应的出站类型。

## 能力

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| SOCKS5 CONNECT | `supported` | 进入 SOCKS5 TCP 入站处理 |
| SOCKS5 UDP ASSOCIATE | `supported` | 进入 SOCKS5 UDP 关联处理 |
| HTTP CONNECT | `supported` | 进入 HTTP 固定目标隧道处理 |
| HTTP forward proxy | `supported` | 支持请求级路由、持久连接和 HTTP/1.1 消息边界 |
| 独立出站 | `unsupported` | 路由结果仍指向具体出站或出站组 |

Mixed 读取首字节完成协议识别后即委托给 SOCKS5 或 HTTP 实现；HTTP 分支与独立 `http` 入站具有完全相同的 redirect/rewrite、路由、Header 和持久连接语义。

## 文档

- [入站行为](./inbound.md)
- [实现边界](./architecture.md)

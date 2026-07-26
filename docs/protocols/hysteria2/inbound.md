# Hysteria2 入站

Hysteria2 入站准备带鉴权的 QUIC profile。客户端通过 `h3` ALPN 发送 `POST /auth`，节点验证 `Hysteria-Auth` 后以状态 `233` 确认。通用运行时负责绑定、QUIC 连接生命周期、关闭和任务回收；Hysteria2 模块负责 HTTP/3 鉴权、stream/datagram 协议语义和响应封装。

## 请求路径

| 请求 | 接受后路径 |
| --- | --- |
| TCP | 认证后的 QUIC stream 进入通用 stream route |
| UDP | QUIC datagram 解码后进入通用 UDP 流程 |

运行时通过中立 `AuthenticatedQuicInboundProfile` / `AuthenticatedQuicInboundConnection` 契约执行 QUIC 生命周期，不在通用模块中命名 Hysteria2 类型。

UDPMessage 按 `(session_id, packet_id)` 有界重组，单包最多接受 64 个分片，同时最多保留 64 个未完成包；不一致的目标、端口或分片总数会使该包失败。错误密码在创建 TCP/UDP 业务会话前拒绝。

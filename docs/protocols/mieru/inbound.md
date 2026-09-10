# Mieru 入站

Mieru 入站完成载体接受、协议鉴权和隧道控制协商。控制请求确定后续是 TCP CONNECT 还是 UDP ASSOCIATE，然后把协议所有的 stream 或 UDP relay 交给通用入站路由。

## 责任划分

| 责任 | 所有者 |
| --- | --- |
| Mieru 握手、加解密和帧编解码 | `protocols/mieru` |
| 隧道内 SOCKS5 CONNECT / UDP ASSOCIATE 协商 | `protocols/mieru::tunnel` |
| 监听与连接任务生命周期 | 通用入站运行时 |
| 路由、转发与计量 | 通用代理运行时 |

代理适配器不持有 Mieru 密码学状态，也不自行构建或解析协议帧。

TCP/UDP 载体入站均允许同一底层连接上的多个逻辑 session，并发处理各自的 CONNECT/UDP ASSOCIATE。
连接级 cipher 与有界分发属于协议，逻辑握手和路由任务属于内核。资源与验收边界见 [多会话与载体](multiplex.md)。

在 `protocol` 对象中选择 `"transport": "udp"` 可监听原生 UDP 载体；省略时保持 TCP。
两种载体都可承载 CONNECT 和 UDP ASSOCIATE。载体选择与业务 UDP 路由是不同配置维度。

`mtu`、`traffic_pattern` 和 `receive` 的配置及取舍见 [传输策略](policies.md)。

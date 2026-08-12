# Mixed 入站

Mixed 绑定一个 TCP 监听端点，并根据连接首字节选择 SOCKS5 或 HTTP 处理路径。识别完成后，所有协议行为都委托给对应协议实现。

| 客户端请求 | 执行路径 |
| --- | --- |
| SOCKS5 CONNECT | SOCKS5 TCP 隧道生命周期 |
| SOCKS5 UDP ASSOCIATE | SOCKS5 UDP 关联生命周期 |
| HTTP CONNECT | HTTP 固定目标隧道生命周期 |
| absolute-form HTTP | HTTP 请求级前向代理生命周期 |
| 无法识别或无效消息 | 返回协议错误，不进入路由 |

普通 HTTP 的每个请求都会单独解析、创建 Session 和执行路由；同一客户端连接中的后续请求不会继承第一个请求的目标或出站。

# HTTP 入站

Zero 的 `http` 入站同时支持 HTTP CONNECT 隧道和普通 HTTP/1.0、HTTP/1.1 前向代理。两种模式有意使用不同的生命周期。

## CONNECT 隧道

`CONNECT host:port` 建立一个固定目标的 TCP 隧道：解析和路由只发生一次，返回 `200 Connection Established` 后，后续字节作为不透明数据双向中继。TLS、HTTP/2 和隧道内 WebSocket 不会被再次解析。

## 普通 HTTP 前向代理

absolute-form 请求（例如 `GET http://example.com/path HTTP/1.1`）按 HTTP 消息处理。一个客户端 TCP 连接可以顺序承载多个请求，但每个请求都会独立执行：

1. 解析目标和消息边界；
2. 创建新的逻辑 Session；
3. 执行 Fake-IP、URL rewrite、路由和出站选择；
4. 建立独立的上游事务并转发一个请求/响应；
5. 在响应边界后继续解析下一请求。

当前实现不复用上游连接。该选择是性能策略，不影响客户端持久连接，也确保不同 authority 和不同路由决策不会共享上一个请求的上游。

## 消息与安全语义

- absolute-form 会转换为 origin-form；上游 `Host` 始终由有效请求目标重新生成。
- `Proxy-Connection`、`Proxy-Authorization`、`Proxy-Authenticate` 和逐跳字段不会直接泄露给源站；`Connection` 指定的扩展字段也会移除。
- 同时包含 `Transfer-Encoding` 和 `Content-Length`、冲突的 Content-Length、非法 chunk framing 会被拒绝。
- 支持固定长度和 chunked 请求/响应、HEAD、1xx、204、304，以及关闭定界响应。
- 普通 HTTP Upgrade 仅在源站返回有效 `101 Switching Protocols` 后切换到原始双向中继；非 101 响应继续按 HTTP 消息处理。
- 客户端流水线字节不会并发执行；请求会按顺序处理并保持响应顺序。

HTTP 和 `mixed` 的 HTTP 分支共用相同实现，因此解析、redirect/rewrite、路由和持久连接语义一致。

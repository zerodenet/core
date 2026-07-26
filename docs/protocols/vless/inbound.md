# VLESS 入站

VLESS 入站负责传输请求准备、协议接受、用户鉴权和目标解析。接受完成后，TCP、UDP 和 MUX 请求分别进入通用入站路由边界。

普通 UDP 会话在响应头之后按请求中的固定目标读写 `[2-byte payload length][payload]` 帧，并拒绝运行时试图把回包写向不同目标；MUX/XUDP 继续使用其独立的地址携带帧，不与普通 UDP 流混用。

## 责任划分

| 责任 | 所有者 |
| --- | --- |
| UUID、flow 与 VLESS 请求解析 | `protocols/vless` |
| TLS、REALITY、WebSocket、gRPC、HTTP/2、XHTTP、QUIC 载体 | `zero-transport` |
| 监听、接受循环、关闭与任务回收 | `zero-proxy` 通用入站运行时 |
| 接受后的 TCP、UDP 与 MUX 路由 | `zero-proxy` 通用路由管线 |

VLESS 适配器只准备协议所需的操作，不自行启动监听循环，也不保留完整 `Proxy` 对象。

XHTTP `stream-one` 入站在 `zero-transport` 首包识别明文线协议：H2/H2C preface 进入 HTTP/2 单流，`POST` 进入 HTTP/1.1 chunked 单流；两者随后都只向 VLESS 暴露中立双向字节流。HTTP/1.1 请求必须匹配配置 path、`POST`、chunked transfer encoding 和 `application/grpc`，响应带 `text/event-stream`、`no-store` 与默认 padding。该路径已通过 Xray v26.3.27 出站到 Zero 入站的 TCP、标准 VLESS UDP、TCP MUX、A → B → A 多目标 XUDP，以及并发 XUDP association 黑盒测试。Xray 在这里仅是外部线协议样本；入站仍通过 Zero 自有 MUX relay 契约进入通用运行时。

## 数据路径

- TCP 请求进入通用 stream route。
- UDP-over-stream 请求通过协议所有的 relay 封装交给通用 UDP 路由。
- MUX TCP/UDP 子流通过中立的 MUX relay 契约交给运行时。

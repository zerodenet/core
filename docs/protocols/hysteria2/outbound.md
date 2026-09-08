# Hysteria2 出站

Hysteria2 出站以显式 TCP 和 UDP 能力注册。适配器准备连接或 UDP 流操作，运行时决定单跳、中继最终跳或 packet-path 的执行顺序。

## 数据路径

- 出站先通过 HTTP/3 `POST /auth` 获得 `233`，并在业务 stream/datagram 存活期间保留 HTTP/3 控制连接。
- TCP 请求使用标准 TCPRequest/TCPResponse varint 帧，成功后交给通用 relay。
- UDP 请求使用标准 UDPMessage、连接协商出的 QUIC datagram MTU、分片和有界重组。
- 通用 packet-path 运行时只保存中立载体描述、缓存标识和计量状态，不解析 Hysteria2 私有字段。

连接失败、协议失败和 UDP 流失败在运行时边界归一化，不由适配器静默回退。

当前外部大包证据覆盖直接 managed UDP 流；packet-path/多跳链的大包仍须单独完成外部互通验收。

## TLS 与认证协商

`insecure` 默认为 `false`：使用公共根证书校验服务端证书链和服务器名称。
显式设置 `true` 才跳过证书信任与名称校验，TLS 握手签名仍须有效。
这项策略同时作用于 TCP、managed UDP 和 packet-path；UDP 缓存标识包含校验策略与指纹，
策略改变后不会复用不同策略的旧连接。当前服务器名称取自 `server`，自定义 SNI、私有 CA、证书固定仍待补齐。

HTTP/3 认证响应保存 `Hysteria-UDP` 与 `Hysteria-CC-RX`（数值或 `auto`）。
服务端未启用 UDP 或未协商 QUIC datagram 时，UDP 建连在发送业务包前失败，TCP 仍可使用。
客户端当前上报接收带宽 `0`，使用 QUIC 自适应拥塞控制；解析接收带宽不代表 Brutal 已实现。
认证取消、出错和最后一个连接持有者释放时，关闭 QUIC 会话并停止 HTTP/3 驱动任务。

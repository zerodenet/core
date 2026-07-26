# Hysteria2 出站

Hysteria2 出站以显式 TCP 和 UDP 能力注册。适配器准备连接或 UDP 流操作，运行时决定单跳、中继最终跳或 packet-path 的执行顺序。

## 数据路径

- 出站先通过 HTTP/3 `POST /auth` 获得 `233`，并在业务 stream/datagram 存活期间保留 HTTP/3 控制连接。
- TCP 请求使用标准 TCPRequest/TCPResponse varint 帧，成功后交给通用 relay。
- UDP 请求使用标准 UDPMessage、连接协商出的 QUIC datagram MTU、分片和有界重组。
- 通用 packet-path 运行时只保存中立载体描述、缓存标识和计量状态，不解析 Hysteria2 私有字段。

连接失败、协议失败和 UDP 流失败在运行时边界归一化，不由适配器静默回退。

当前外部大包证据覆盖直接 managed UDP 流；packet-path/多跳链的大包仍须单独完成外部互通验收。

# Hysteria2 公共约定

Hysteria2 入站和出站共享鉴权信息、TLS/QUIC 端点信息和协议级参数。公开互操作基线使用 `h3` ALPN、HTTP/3 `POST /auth`、状态 `233`、标准 TCPRequest/TCPResponse 与 UDPMessage。协议私有值由 Hysteria2 构造器和验证 API 解析，通用代理运行时不重建密钥、证书、鉴权状态或协议帧。

## 边界

| 数据 | 所有者 |
| --- | --- |
| 鉴权值与协议帧 | Hysteria2 协议模块 |
| 证书、密钥与 QUIC 载体构建 | 协议适配器与传输层 |
| 流生命周期、路由和计量 | 通用运行时 |

完整字段与校验规则见[公开配置参考](https://docs.zerodenet.org/projects/core/configuration/)。

## TCP 流读取边界

TCPRequest 按 QUIC varint、地址和 padding 逐字段精确读取，支持请求头分段到达，
并将与请求头一同到达的业务数据保留给后续转发。TCPResponse 使用相同的有界读取工具。
截断字段或提前 EOF 会使握手失败；padding 不进入业务数据流。

长度上限与 [Hysteria 参考实现](https://github.com/apernet/hysteria/blob/master/core/internal/protocol/proxy.go)
一致：地址为 1–2048 字节，响应消息为 0–2048 字节，padding 为 0–4096 字节。
超限长度在分配地址缓冲区或读取字段内容之前拒绝；padding 使用固定大小缓冲区丢弃。
这些是实现的资源边界，不是 QUIC varint 的取值上限。

回归测试位于 `protocols/hysteria2/tests/tcp_stream.rs`，覆盖合并到达的业务数据、
逐字节及各字段边界分段、全部 varint 宽度、大 padding、截断与异常长度。

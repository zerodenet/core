# Hysteria2 公共约定

Hysteria2 入站和出站共享鉴权信息、TLS/QUIC 端点信息和协议级参数。公开互操作基线使用 `h3` ALPN、HTTP/3 `POST /auth`、状态 `233`、标准 TCPRequest/TCPResponse 与 UDPMessage。协议私有值由 Hysteria2 构造器和验证 API 解析，通用代理运行时不重建密钥、证书、鉴权状态或协议帧。

## 边界

| 数据 | 所有者 |
| --- | --- |
| 鉴权值与协议帧 | Hysteria2 协议模块 |
| 证书、密钥与 QUIC 载体构建 | 协议适配器与传输层 |
| 流生命周期、路由和计量 | 通用运行时 |

完整字段与校验规则见[公开配置参考](https://docs.zerodenet.org/projects/core/configuration/)。

# Hysteria2

Zero 的 Hysteria2 实现使用标准 QUIC/HTTP/3 `POST /auth` 建立会话，以 QUIC stream 承载 TCPRequest/TCPResponse，以 QUIC datagram 承载 UDPMessage。鉴权、varint、TCP/UDP 帧和分片重组属于 Hysteria2 协议模块，QUIC 连接生命周期通过中立能力交给通用运行时。

## 能力摘要

| 能力 | 状态 | 承载方式 |
| --- | --- | --- |
| TCP 入站 | `supported` | QUIC stream |
| TCP 出站 | `supported` | QUIC stream |
| UDP 入站 | `partial` | QUIC datagram |
| UDP 出站 | `partial` | QUIC datagram 与 packet-path |
| MUX | `unsupported` | 不另行定义协议级 MUX |

## 外部互操作证据

- sing-box v1.13.14 → Zero：TCP、UDP、1600 字节 UDP 分片/重组通过。
- Zero → sing-box v1.13.14：TCP、UDP、1600 字节 UDP 分片/重组通过。
- sing-box 使用错误密码连接 Zero：HTTP/3 鉴权拒绝，目标连接未建立。
- 入口：`crates/proxy/tests/hysteria2_sing_box_interop.rs`。

UDP 仍标记为 `partial`，因为 packet-path/多跳大包、在线用户变更后的外部清退和长稳故障恢复尚未完成外部验收。

## 文档

- [入站](./inbound.md)
- [出站](./outbound.md)
- [公共约定](./shared.md)

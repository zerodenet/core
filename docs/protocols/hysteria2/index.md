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
- 凭据热更新：旧连接被清退、旧密码不能建立新连接，新密码访问成功。
- 入口：`crates/proxy/tests/hysteria2_sing_box_interop.rs`。

2026-09-08 使用从 v1.13.14 源码构建的 sing-box 复验，6 项外部测试全部通过。
同日新增官方 Hysteria app/v2.12.2 双向 TCP/UDP 验收，4 项全部通过，UDP 载荷为 1600 字节。
官方服务端保留默认 SNI guard；Zero 通过独立 `server_name` 指定证书名称。
[固定版本互通 CI](https://github.com/zerodenet/core/actions/runs/34245710843) 使用校验哈希后的 Linux 官方发布资产，10 项测试全部通过。
入口为 `crates/proxy/tests/hysteria2_official_interop.rs` 与原 sing-box 测试。
TCP 头部的分段、合并读取与长度边界另有 11 项确定性回归测试，见[公共约定](./shared.md)。

UDP 仍标记为 `partial`，因为 packet-path/多跳大包和长稳故障恢复尚未完成外部验收。

## 文档

- [入站](./inbound.md)
- [出站](./outbound.md)
- [公共约定](./shared.md)
- [官方实现对齐清单](./parity.md)

带宽协商、Brutal/BBR、QUIC 参数、连接复用与 HTTP/3 伪装见[传输与伪装配置](transport.md)。

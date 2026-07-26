# Hysteria2

> 参照实现：hysteria | Crate：`hysteria2`

Hysteria2 是基于 QUIC 和 HTTP/3 的代理协议。Zero 的公开互操作路径使用标准 `h3` ALPN、`POST /auth` 鉴权、TCPRequest/TCPResponse 和 UDPMessage 帧；旧的 Zero 私有 ALPN/鉴权仅作为同实现迁移兼容分支，不属于对外兼容基线。

## 协议来源

| 项目 | 来源 |
| --- | --- |
| 参照实现 | [apernet/hysteria](https://github.com/apernet/hysteria) |
| 协议规范 | [Hysteria2 Protocol](https://v2.hysteria.network/docs/developers/Protocol/) |
| 本实现 | `hysteria2` crate |

## 功能与证据

| 特性 | 状态 |
| --- | --- |
| QUIC + HTTP/3 `POST /auth`，成功状态 `233` | 已实现 |
| TCPRequest/TCPResponse 标准 varint 帧 | 已实现 |
| UDPMessage、MTU 分片与有界重组 | 已实现 |
| 入站多用户密码鉴权与在线用户同步 | 已实现 |
| TCP/UDP 入站、出站及 UDP chain | 已实现 |
| Zero ↔ sing-box v1.13.14 TCP/UDP 双向互通 | 已验证 |
| 1600 字节 UDP 双向分片/重组 | 已验证 |
| sing-box 错误密码访问 Zero 入站 | 已验证拒绝 |

真实外部二进制测试位于
`crates/proxy/tests/hysteria2_sing_box_interop.rs`，默认 `#[ignore]`，需要
`SING_BOX_BIN` 指向 sing-box 可执行文件。

## 尚未完成的生产门槛

- Hysteria2 UDP packet-path/多跳链的大包外部互通尚未覆盖。
- 用户热更新后的外部客户端清退、长连接断网恢复和长稳压测尚未完成。
- obfs、带宽协商和 Hysteria 风格拥塞控制仍未实现；面板映射必须对这些字段 fail closed。

## 架构

```text
src/lib.rs                 crate root
src/inbound.rs             协议所有的入站请求/用户投影
src/outbound.rs            协议所有的出站请求
src/shared.rs              TCP 帧、authority 与 QUIC varint
src/udp.rs                 UDPMessage、分片、重组和 managed UDP
src/transport/auth.rs      HTTP/3 鉴权客户端与服务端
src/transport/*.rs         QUIC 连接、stream 与运行时投影
```

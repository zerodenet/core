# Mieru 出站

Mieru 出站先从协议池取得载体上的独立逻辑会话，再调用 `protocols/mieru::tunnel` 完成隧道内控制协商。返回的 TCP stream 或 UDP session 是协议所有类型，代理层只做中立的转发与生命周期管理。

## TCP

1. 适配器准备 Mieru TCP 出站操作。
2. 协议池复用已有载体；需要建连时由运行时提供遵循出口策略的 socket。
3. Mieru 协议模块完成隧道握手和 CONNECT 协商。
4. 运行时中继客户端与协议 stream。

## UDP

UDP ASSOCIATE 请求、响应、数据阶段加解密和数据包编解码均属于 `protocols/mieru::udp`。通用 UDP 运行时不持有其 cipher 或 session 状态。

## 载体配置

下例是 outbound 的 `protocol` 对象；默认 `transport` 为 `tcp`。

```json
{
  "type": "mieru",
  "server": "proxy.example.com",
  "port": 8964,
  "username": "example-user",
  "password": "replace-me",
  "transport": "udp"
}
```

共享池隔离节点、凭据、载体、relay 前缀和出口代次；TCP relay 末跳在建新物理前缀连接前
先尝试复用池内 Mieru 载体，同一池既可打开业务 TCP CONNECT，也可打开 UDP ASSOCIATE
逻辑会话。重载后旧活跃会话继续完成，新请求使用新池。
原生 UDP 可通过前一跳的 packet-path/datagram carrier（当前验证为 SOCKS5 UDP ASSOCIATE）
到达 Mieru 末跳；仅提供字节流的 relay hop 不能满足这一载体契约。
连接上限、可靠性和验收范围见 [多会话与载体](multiplex.md)。

`mtu`、`traffic_pattern` 和 `receive` 的配置及取舍见 [传输策略](policies.md)。

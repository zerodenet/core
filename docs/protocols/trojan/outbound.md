# Trojan Outbound

对应 `protocols/trojan/src/outbound.rs` — `TrojanOutbound`、`TrojanTcpTunnelTarget`、`TrojanUdpPacket`、`TrojanUdpPacketTunnelTarget`。

## TrojanOutbound

实现 `TcpTunnelProtocol` trait：

```rust
impl TcpTunnelProtocol for TrojanOutbound {
    async fn establish_tcp_tunnel(
        &self,
        stream: T,
        target: &Address,
        port: u16,
    ) -> Result<TrojanTcpTunnelTarget>
}
```

1. 建立 TLS 连接
2. 写入 Trojan request：`[PASSWORD_HASH][CRLF][CMD_TCP][ATYP][ADDR][PORT][CRLF]`
3. 返回 `TrojanTcpTunnelTarget` — proxy 直接 relay TCP stream

## TrojanTcpTunnelTarget

```rust
pub struct TrojanTcpTunnelTarget {
    pub stream: T,
}
```

默认 relay：proxy 将 TLS stream 与原 TCP stream 做双向 copy。无 AEAD wrapper。

## UDP over Stream

`TrojanUdpPacketTunnelTarget` — 用于 UDP-over-TLS-stream 的 tunnel target。

`TrojanUdpPacket` — CMD_UDP packet 格式：
```

## Mux.Cool

配置 `mux_concurrency` 后，出站首先以普通 Trojan TCP 请求连接 `v1.mux.cool:666`，随后在该 TLS 连接上承载 Mux.Cool TCP/UDP 子流。连接池键包含端点、密码、TLS SNI/校验策略、客户端指纹、空闲超时及回程 backlog 策略，避免不兼容配置误共池。relay final-hop 不跨越已有中继流复用 MUX。

- `mux_concurrency`: 单条物理连接的最大并发子流数
- `mux_idle_timeout_secs`: 无帧活动时关闭物理连接
- `mux_response_backlog_frames`: 单子流回程帧数上限
- `mux_response_backlog_bytes`: 物理连接回程字节预算
[ATYP][ADDR][PORT][2-byte length][PAYLOAD]
```

## Outbound 配置

```json
{
  "tag": "trojan-out",
  "protocol": {
    "type": "trojan",
    "server": "example.com",
    "port": 443,
    "password": "your-password",
    "sni": "example.com",
    "insecure": false
  }
}
```

- `password`: 必需
- `sni`: 可选 TLS SNI
- `insecure`: 可选，跳过 TLS 证书验证
- `client_fingerprint`: 可选 TLS 客户端兼容配置；默认 `"chrome"`，使用现代混合密钥组生成浏览器尺寸级别的 ClientHello；显式设置 `"none"` 可关闭。原始 Trojan 不会因该选项自动声明 HTTP ALPN。

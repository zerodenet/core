# Trojan

Trojan 的 TCP、UDP-over-stream 与 Mux.Cool TCP/UDP 能力已完整接入统一运行时。TLS 是协议载体，认证、请求帧、MUX 状态和 UDP 封装均由 `protocols/trojan` 所有。

## 当前能力

| 能力 | 状态 | 说明 |
|------|------|------|
| TCP 入站/出站 | `supported` | TLS + Trojan TCP 请求，支持单跳与 relay final-hop |
| UDP 入站/出站 | `supported` | CMD_UDP 单流及 Mux.Cool 固定目标 UDP 子流 |
| MUX | `supported` | TCP/UDP 子流共享 TLS 物理连接；支持并发、空闲超时和回程 backlog 双限制 |
| 热更新 | `supported` | 入站凭据热替换；出站配置更新清空 MUX 连接池 |

`mux_concurrency` 启用 Mux.Cool；`mux_idle_timeout_secs` 控制物理连接空闲回收；`mux_response_backlog_frames` 和 `mux_response_backlog_bytes` 限制每条回程队列。未配置 MUX 时继续使用标准 Trojan TCP/CMD_UDP 单流。

TLS 客户端指纹预设在 TCP、UDP fresh-socket 和 relay-stream 路径中保持一致；它控制当前 rustls cipher/ALPN 预设，不承诺完整浏览器 ClientHello 仿真。

外部互操作测试仍保留在 `crates/proxy/tests/trojan_xray_interop.rs` 并默认忽略；能力完整性的 CI 门由 Zero→Zero TCP/UDP/MUX 端到端测试承担。

## 文档

- [入站](inbound.md)
- [出站](outbound.md)
- [公共格式](shared.md)
- [能力元数据](metadata.md)

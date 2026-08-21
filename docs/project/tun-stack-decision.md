# TUN 用户态网络栈决策

## 决策

Zero 保留 `zero-stack` 中的自研用户态网络栈，并将它作为独立、可确定性测试的子系统维护，而不是在当前阶段替换为第三方完整 TCP/IP 栈。

这段 TCP 只终止“本机应用 ↔ TUN 设备”的本地链路。互联网侧仍由操作系统 socket、既有协议实现和 `zero-proxy` 出站编排处理。当前实现已经具有稳定的 `zero-traits`、Session、Flow、路由和统计契约；整体替换会同时扩大异步适配、平台设备和观测契约的回归面，却不能替代 Zero 自身的 TUN 路由、Fake-IP 恢复和出口防环职责。

后续若重新评估成熟网络栈，只能在现有 `zero-traits` 边界后替换，不得改变 Zero API、配置、路由、Session 或 Flow 契约。

## TCP 策略

- SYN、数据和 FIN 都进入有界重传队列；RTO 使用 RTT 样本估计，采用上下界和指数退避，达到重试上限后确定性唤醒并失败读写端。
- 发送端使用 RFC 5681/6928 风格的初始拥塞窗口、慢启动、拥塞避免、三次重复 ACK 快速重传和超时回退。因为链路仅经过本机 TUN，RTO 下界可以低于互联网 TCP。
- 对端广告窗口和拥塞窗口共同限制未确认数据。对端零窗口时发送周期性探测，窗口更新会唤醒阻塞写端。
- 接收窗口绑定实际的 65,535 字节单连接缓冲；缓冲耗尽时广告零窗口，代理读取后立即发送窗口恢复 ACK。ACK 在本地链路上即时发送，不使用 delayed ACK。
- 默认最多保存 8,192 个 TCP 状态，其中最多 1,024 个处于半连接状态；accept 队列和单连接字节缓存均有固定上限。达到上限时显式拒绝新连接，不扩张内存。

这些值是内核安全默认值，不要求客户端提供新配置。未来如需开放覆盖，只能增加可选字段，并继续保留安全上限。

## IP、分片和 MTU 策略

- IPv4 与 IPv6 入站分片按源、目标、标识和上层协议重组，允许完全相同的重复片，拒绝任何有歧义的重叠片。
- 分片状态最多 256 组、总载荷最多 4 MiB、单组最多 65,535 字节，30 秒未完成即过期。
- TCP 使用由 TUN MTU 推导的保守 MSS。代理回写的超 MTU UDP 在源端生成 IPv4/IPv6 分片。
- 未分片且超过 TUN MTU 的 IPv4/IPv6 包分别收到 Fragmentation Needed 或 Packet Too Big，避免静默 PMTU 黑洞。
- Zero 不伪造远端 ICMP 可达性。ICMP echo 和其他未代理的 IP 协议会立即收到 administratively prohibited，而不是静默超时。

## 故障归因

- 本机应用复位：`close_reason=client_error`、`failure.stage=client_transport`。
- 本地 TUN ACK 超时或设备数据通道关闭：`close_reason=tun_error`、`failure.stage=tun_transport`。
- 真实路由、建连或上游转发故障继续使用 `upstream_error`。
- `client_error` 与 `tun_error` 对被动出站健康保持中性，不能降低节点健康度。

新增值属于现有字符串字段的语义扩展，不改变 API 结构。

## 发布验证

单元和集成测试必须覆盖丢 SYN-ACK、数据、ACK、FIN，重复与乱序，序号回绕，发送/接收窗口，零窗口恢复，队列压力，分片乱序、重复、重叠、过期和 MTU 错误。发布门禁仍需在 Windows、Linux、macOS 上执行特权 TUN IPv4、IPv6、双栈和持续流量测试；单个平台通过不能替代跨平台验证。

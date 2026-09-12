# Shadowsocks TCP 入站

`inbound.rs` 是入口 facade，握手、profile、授权索引、热更新与 acceptor
分布在 `inbound/`。UDP 入口和状态位于 `udp/inbound/`。

`ShadowsocksInboundTcpAcceptor` 接受 carrier socket，验证协议身份并返回
中立 `Session` 和协议拥有的 `ShadowsocksAeadStream`。Session 携带原生
principal/限速/额度策略；proxy runtime 再执行路由、取消和计量。

v1 支持 plain、单用户流密码和 AEAD；AEAD 的目标地址可跨多个认证 chunk。
2022 按 SIP022 接收固定头与可变头，固定前缀保持探测边界，可变头允许续读。
失败路径共用一次 2 秒、1 MiB 上限的 drain。AES EIH 按 SIP023 解密身份头并
使用索引选择 uPSK，随后验证头、时间戳和 salt 重放。响应绑定本次请求 salt。

`ShadowsocksInboundProfileStore` 原子替换用户快照，已有 listener 消费更新；
静态 iPSK 与动态 uPSK 分开。用户删除同时清理其 UDP 状态，不能终止共享 listener。
协议私有配置验证和密码规则见 [configuration.md](configuration.md)。
实现矩阵与固定版本互通证据见 [parity.md](parity.md)。

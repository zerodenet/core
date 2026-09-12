# Shadowsocks

固定对标 `shadowsocks-rust 1.21.2`，提交
`a03006a753486e64717d6e3afa91e0c6d043c557`，加密依赖 `shadowsocks-crypto 0.5.5`。
对齐范围为协议和线路，管理功能使用 Zero 原生配置、API、用户策略及可观测接口。
生产环境验证单列为最终步骤，不参与实现完成度计算。

| 文档 | 内容 |
| --- | --- |
| [实现对照](parity.md) | 固定基线、代码归属和测试证据 |
| [原生配置](configuration.md) | 密码、重放策略、状态限制、SIP003/SIP003u、管理映射 |
| [密码及共享原语](shared.md) | 完整算法目录、KDF、地址和 nonce |
| [TCP 入站](inbound.md) | 协议握手、SIP023 用户选择、认证结果 |
| [出站](outbound.md) | TCP 会话及 UDP 组合 |
| [流](stream.md) | 加密流、分块、取消与 EOF |
| [状态生命周期](../shadowsocks-udp-state.md) | 隔离、迁移、回收和重放 |
| [能力元数据](metadata.md) | 实现能力与生产验证的边界 |

算法涵盖 plain、历史流密码、常规与可选 AEAD、四种 AEAD 2022。
TCP、UDP 均双向实现。SIP023 支持 AES 2022 的 iPSK/uPSK 链与多用户选择。
SIP003/SIP003u 通过协议自有进程和载体计划接入中立 runtime。

Zero 额外提供 SS/SOCKS5 UDP packet-path 组合；外部插件自身建立连接，不能
包装已经存在的 relay stream，也不作为可嵌套的 UDP packet-path codec。
这些组合限制与官方 SS 协议功能分开描述。

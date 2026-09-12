# Shadowsocks 能力元数据

`protocols/shadowsocks/src/metadata.rs` 提供能力描述：

| 字段 | 值 |
| --- | --- |
| `status` | `supported` |
| `compatibility_baseline` | `shadowsocks_rust_sip022_sip023` |
| 入站 TCP / UDP | `supported` |
| 出站 TCP / UDP | `supported` |
| `transports` | `tcp`, `udp`（可通过 SIP003/SIP003u 外部载体） |
| MUX | `unsupported`，固定官方协议没有 MUX |
| `limitations` | 空列表 |

固定实现版本和证据见 [parity.md](parity.md)。`supported` 描述实现能力，
不表示当前机器已部署、所有外部插件已认证或生产环境已验证。
历史 `shadowsocks_2022_hardening_not_externally_validated` 将验证状态混入
实现能力，已移除；生产测试独立记录，不能用来降低实现完成度。

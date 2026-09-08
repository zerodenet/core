# Hysteria2 官方实现对齐

本清单以 2026-09-08 确认的官方稳定版
[app/v2.12.2](https://github.com/HyNetworks/hysteria/releases/tag/app/v2.12.2)
和[协议规范](https://v2.hysteria.network/docs/developers/Protocol/)为对照。
本地 crate 的 `2.6.1` 是历史包版本，不是完整兼容性声明。
每项能力分别记录实现和可复现的外部验收；单元测试通过不能替代官方二进制互通证据。

## 分层与验收

| 对象 | 实现状态 | 归属与后续验收 |
| --- | --- | --- |
| TCPRequest/TCPResponse、增量 varint 和长度边界 | 已实现 | `protocols/hysteria2`；确定性分段/粘包测试，sing-box 双向互通 |
| UDPMessage、分片/有界重组 | 已实现，部分外部验收 | 协议模块；补官方端双向、乱序、重复、超时、MTU 和多跳大包验收 |
| TLS 校验与 `insecure` | 本批修复 | 通用 `zero-transport::quic`；HY2 适配器传递策略，TCP/UDP/packet-path 一致 |
| 认证响应 UDP 能力、接收带宽解析 | 本批实现 | 协议拥有解析和 UDP 拒绝；缺失/异常头按参考实现默认值处理 |
| 认证取消与 HTTP/3 任务回收 | 本批修复 | 协议连接 guard；认证等待取消后对端可观察连接关闭 |
| SNI、私有 CA、证书固定、客户端证书、ECH | 待实现或逐项审计 | 配置 ADT 在 config；协议建 profile；TLS 机制在 transport |
| 带宽配置、Brutal、BBR 配置 | 待实现 | 协议处理收发带宽协商；通用 QUIC 执行拥塞控制，不在 proxy 模拟算法 |
| Salamander、Gecko | 待实现 | 协议拥有混淆编解码，transport 提供中立 datagram 包装；官方端双向和边界测试 |
| QUIC 窗口、保活、PMTU、连接复用/恢复 | 部分基础实现 | 通用 transport 与已有运行时生命周期；补参数配置和故障/长稳验收 |
| HTTP/3 伪装站点/代理 | 待实现 | 目前只有认证失败 404；需补持续 HTTP/3 服务和认证后的流分流，不能将 404 当完整伪装 |
| 端口跳跃、Fast Open、Mimic、Realm | 待逐项设计和实现 | 根据官方稳定版源码列明载体依赖、平台限制和可测行为 |
| 官方应用的 ACL、DNS、TUN、管理/统计 API | 映射已有 Zero 能力 | 复用 engine/router/dns/tun/api；不在 HY2 内建立第二套应用或控制面 |

## 推进顺序

1. 连接身份、认证能力协商、资源回收与缓存隔离。
2. 收发带宽与拥塞控制、QUIC 参数和连接恢复。
3. 混淆、伪装、其他载体能力及配置验证。
4. 官方稳定版与 sing-box 双向 TCP/UDP、packet-path、多跳和长稳故障验收。

所有开发提交在 `develop` 上线性追加，吸收 `main` 时先核对祖先关系，避免 merge commit。
协议或运行时行为变动执行工作区全量测试及分层回归；能力摘要保持 `partial`，直到对应外部验收闭环。

## 可重复外部验证

`Hysteria2 Interop` 工作流固定官方 Hysteria `app/v2.12.2` 和 sing-box `v1.13.14`，
下载后校验发布资产 SHA-256，再运行官方端四项和 sing-box 六项互通测试。
HY2、共享传输和代理相关改动自动触发，也可手动运行。
本地以 `HY2_BIN`、`SING_BOX_BIN` 指定同版本程序，执行该工作流中的同一 Cargo 命令。

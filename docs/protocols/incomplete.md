# 未完成项

本页只记录协议层**尚未完成**的能力缺口。已完成项已移除，实现与验证记录见各协议 `index.md`；公开能力事实见[协议能力参考](https://docs.zerodenet.org/projects/core/reference/protocol-capabilities)，运行时权威来源是 `capabilities.protocols`（各协议 metadata）。

## Shadowsocks

常规 AEAD Shadowsocks TCP/UDP 不受下列缺口影响；SIP022 全部 spec 章节已实现。

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| `shadowsocks_2022_hardening_not_externally_validated` | SIP023 TCP/UDP 已完成 `shadowsocks-rust` 1.24.0 双向互操作，但检测防御/drain 与滑动窗口未对抗真实主动探测/重放攻击完成验证 | 用真实 prober/重放工具验证单次读取+drain、salt 重放池与 UDP 滑动窗口行为 |

## VLESS

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| XUDP/Mux.Cool 生产硬化未完成 | 标准 Mux.Cool TCP、A → B → A 多目标 XUDP 与并发 association 已由 Zero 原生模型实现，并通过 Xray v26.3.27 双向黑盒测试；非零 GlobalID 被转换为有界、不透明的 Zero `UdpContinuityKey`。共享注册表按入站、协议和 principal 隔离，拒绝真实活动冲突并维护 generation；父载体先关闭协议 server、允许子任务优雅 detach，再中止悬挂任务。载体异常断开时原 `UdpDispatch` 连同上游 socket、flow 与计费上下文会转移给下一代载体，显式 END/清退立即结算，保留期到期则由定时任务结算。`xray_xudp_reattaches_zero_dispatch_after_carrier_reset` 已用 TCP 故障代理主动切断 Xray XHTTP 载体，证明同一 SOCKS UDP association 自动重连后保持同一个 engine session 且最终上下行计费无重复；协议无关回归也证明 detached dispatch 会在同一主体由其他连接耗尽共享配额时立即取消并按 `quota_exhausted` 结算。独立 `mux_idle_timeout_secs` 已进入 VLESS/VMess 物理载体运行时，按真实帧活动刷新并同时关闭读写两半；入站回程和出站下行响应使用 Zero 原生可配置的帧数/字节双重硬限制，默认每流 32 帧、每载体 1 MiB，溢出终止单流并精确释放预算，不同策略不会共池 | 完成时间型长稳压测 |

## Trojan

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| 外部互通覆盖不足 | 当前不能声明生产级完整兼容 | 使用基线实现进行 TCP 和 UDP 外部互通测试 |
| MUX 不支持 | 不提供 Trojan MUX 能力 | 明确实现 MUX 或保持 `unsupported` |

## Hysteria2

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| 扩展外部互通覆盖不足 | sing-box v1.13.14 双向 TCP/UDP、1600 字节分片和错误密码拒绝已通过；packet-path/多跳大包、在线用户变更清退和长稳故障恢复仍不能声明生产级完整兼容 | 使用外部实现完成 packet-path/多跳大包、热更新清退、断网恢复与长稳矩阵 |

## VMess

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| `cipher: zero` 非主流互通能力 | Zero 内部路径可用；Xray inbound 不接受 `zero` security，不能作为主流面板默认选项展示 | 只作为 Zero 内部兼容项保留，或在确认主流实现支持后补充外部互通测试 |

## 通用要求

协议从 `partial` 或 `experimental` 提升到 `supported` 需要同时满足：

- 配置解析和校验完整；
- 未编译 feature 时能早期失败；
- TCP/UDP 方向接入统一 runtime pipe；
- 运行时统计、事件、session 生命周期可观测；
- 协议细节留在协议 crate 内；
- 外部基线实现互通测试通过；
- docs 和 `capabilities.protocols` 同步更新。

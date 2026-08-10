# 未完成项

本页只记录协议层**尚未完成**的能力缺口。已完成项已移除，实现与验证记录见各协议 `index.md`；公开能力事实见[协议能力参考](https://docs.zerodenet.org/projects/core/reference/protocol-capabilities)，运行时权威来源是 `capabilities.protocols`（各协议 metadata）。

## Shadowsocks

常规 AEAD Shadowsocks TCP/UDP 不受下列缺口影响；SIP022 全部 spec 章节已实现。

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| `shadowsocks_2022_hardening_not_externally_validated` | SIP023 TCP/UDP 已完成 `shadowsocks-rust` 1.24.0 双向互操作，但检测防御/drain 与滑动窗口未对抗真实主动探测/重放攻击完成验证 | 用真实 prober/重放工具验证单次读取+drain、salt 重放池与 UDP 滑动窗口行为 |

## Hysteria2

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| 扩展外部互通覆盖不足 | sing-box v1.13.14 双向 TCP/UDP、1600 字节分片和错误密码拒绝已通过；packet-path/多跳大包、在线用户变更清退和长稳故障恢复仍不能声明生产级完整兼容 | 使用外部实现完成 packet-path/多跳大包、热更新清退、断网恢复与长稳矩阵 |

## 通用要求

协议从 `partial` 或 `experimental` 提升到 `supported` 需要同时满足：

- 配置解析和校验完整；
- 未编译 feature 时能早期失败；
- TCP/UDP 方向接入统一 runtime pipe；
- 运行时统计、事件、session 生命周期可观测；
- 协议细节留在协议 crate 内；
- 内置端到端测试覆盖公开能力；若外部基线互通验证暂缓，必须保留可执行测试入口并在协议文档中披露，不能把未验证描述成已验证；
- docs 和 `capabilities.protocols` 同步更新。

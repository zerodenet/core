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

## Mieru

TCP/UDP 载体均已实现入站多会话与共享出站连接池，范围见 [多会话与载体](mieru/multiplex.md)。

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| `long_running_recovery_is_not_verified` | 基础互通不能证明长期运行和网络切换恢复 | 固定配置与版本下完成长稳、故障与资源回收矩阵 |

## WireGuard（opt-in）

双向端点、原生 PacketSink、Flow 转换、双 peer 实网 payload 与固定 wireguard-go/Xray 双向互操作已有实现和定向验证，详见[实现计划](../project/wireguard-implementation-plan.md)。这些结果尚不足以将 `wireguard` 加入默认 `full`。

| 缺口 | 影响 | 完成标准 |
|------|------|----------|
| `engine_session_key_lifecycle_not_audited` | 仓库内固定 GotaTun `0.9.2` 补丁已将会话 AEAD 改为销毁时擦除 key 的 RustCrypto 类型，擦除持有中的握手链式密钥、限速器 secret 与预共享密钥，并遮蔽握手 Debug；临时栈副本、外部加密依赖内部状态及完整安全审计仍无覆盖；该后端的吞吐成本也未在 Zero 负载下测量 | 审查补丁及依赖的完整密钥持有链，测量 Packet/Flow 吞吐，完成故障和互操作门禁后再评估默认启用 |
| `full_platform_and_workspace_gate_not_verified` | 最终安全补丁通过本地定向及固定 wireguard-go/Xray 互操作；此前 macOS 双 peer 实网通过发生在补丁前，不能代替补丁后的实网、Linux/Windows、真实 TUN 或全工作区测试 | 三平台 CI、补丁后实网与真实 TUN、完整 workspace 门禁均有成功记录 |
| `multi_peer_fault_recovery_not_verified` | 已有本地漫游与外层代理重启恢复用例；真实多 peer 失联、网络切换和持续运行仍缺验收 | 固定配置下完成多 peer 故障注入、重协商与资源回收矩阵 |

## 通用要求

协议从 `partial` 或 `experimental` 提升到 `supported` 需要同时满足：

- 配置解析和校验完整；
- 未编译 feature 时能早期失败；
- TCP/UDP 方向接入统一 runtime pipe；
- 运行时统计、事件、session 生命周期可观测；
- 协议细节留在协议 crate 内；
- 内置端到端测试覆盖公开能力；若外部基线互通验证暂缓，必须保留可执行测试入口并在协议文档中披露，不能把未验证描述成已验证；
- docs 和 `capabilities.protocols` 同步更新。

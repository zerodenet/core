# B：剩余出站、共享池及载体职责域

分支 `codex/v003-cloud-r2-outbound`，PR base 为 `codex/v003-cloud-r2-integration`。对应 Z003-02/03/06 中尚未完成的既有生产路径，只写[入口](README.md#文件所有权)中 B 的出站范围。

## 继承与范围

上轮已迁 HY2 root TCP/UDP/既有 first-Datagram packet-path，同一个真实认证池和原始 QUIC/Metadata 所有者不能重做成另一套。其非空 prefix 原本 Unsupported，不是本轮要补的新能力。入站、TCP/UDP claim、handler/Any/resume 旧链已删除，不恢复、不重迁。

本域处理仍使用兼容执行入口的现有 Mieru、VLESS/VMess/Trojan MUX、gRPC/H2/QUIC 等共享载体，以及真实 TCP/UDP/packet-path 消费端。范围先从实际生产调用者冻结，覆盖当前已有合法配置/组合，不能通过逐个扩展组合创造里程碑。

## 先交依赖，再完成本域

1. 与 A/C 对齐所需原件转交清单。优先沿现有 Stream/Datagram/Packet 和显式 carrier borrowing 实现真实 donor/control handoff，给出可合入的前置提交；不要把握手、配置解析或池状态搬进 DNS/runtime。
2. 使用同一实际 adapter/protocol pool、endpoint、Scope 和原始 provider；完整凭据/profile、DNS/network/依赖代次进入复用边界。普通角色和兼容调用者在过渡期间不能同时各运行一个资源实例。
3. owner 必须在首次 await 前安装，driver 由真实 owning Scope 监督，转交原始控制保留实际 parent/slot/driver lineage。无独立 driver 的 owner 不虚构 witness；logical return 不是 donor native close。
4. 所有业务 Stream/Datagram/Packet 消费者接通现有标准派发与计量。保留空包/多目标、独占接收、顺序、半关闭、重连和安全历史。TLS/transport/framing、私有池键和凭据解析仍由协议拥有。
5. 资源替换先确认受影响消费者及传递依赖的原始清理，再构造/启动新资源；失败候选未清理不能恢复旧配置。历史句柄只保留弱代次事实，不保活旧私有池。
6. 生产登记和 inventory/Registry 总 leaf 投影的共享改动交 M 唯一整合。实际 qualified 普通路径完成后更新旧 gate 及规则，迁完该路径全部调用者再删除兼容 executor；不能只在受控测试 catalog 开启角色后结题。

## 文件交叉点

- A 独占 DNS crate/provider/旧 DNS executor。你提供真实标准角色和控制，不在协议内建 DNS 专用 connector。
- C 独占 WireGuard/Direct 设备和 Packet/TUN 运行；你拥有其他协议出站的 packet-path 载体部分。generic Packet runtime 或 grant 缺口交 M，不能双方重复改。
- 公共 platform/transport/runtime/catalog、`register.rs`、`inventory/service*`、`protocol_registry/`、tcp service 和 feature 由 M 唯一写入。提交具体变更建议，取得实际前置 SHA 后合入使用。
- 已迁入站模块默认不动；若通用 carrier 修复确实影响它，交 M 指定唯一负责人并补必要回归，不借机再迁一遍 listener。

## 必要验收和完成条件

- 每类现有执行形态的真实生产 TCP/UDP/packet-path、共享同池复用与计量；命令/普通 Proxy 装配不是仅私有 assembly。
- 认证/profile 隔离，候选失败及安全的既有后备，真实 pending I/O 取消，配置/网络换代及恢复。
- 原件 native close、转交用户/控制归还、真实 driver/Scope joins 分开；保留未完成原件时禁止替代构造。
- 生命周期内有界任务、pool/custody 元数据与按需额度，不驱逐活跃业务或有效 replay 历史。列需要接入生产预算的实际 owner 给预算后续任务，不引入新通用框架。
- 相应独立 feature 和固定 upstream tag/commit 互通；既有通过记录仅按相同输入复用。
- 交付生产调用者迁移/删除表，指出每个保留公开库 API 的真实职责。不能以 gate 继续关闭、裸流 Drop 或返回 Unsupported 作为整域完成证据。

主 agent 统一审查旧桥删除、普通登记、共享原件和依赖顺序，只有实际完整交付后才能关闭相关 Z003 子域。TEMP-WG-OUTER-UDP-01 保持独立。

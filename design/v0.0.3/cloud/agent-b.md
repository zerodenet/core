# Agent B：Hysteria2 既有出站共享池标准迁移

从 [云端任务入口](README.md) 指定的独立分支开工。第一轮固定一个完整职责域：Hysteria2 **既有出站** TCP、业务 UDP 及现有 packet-path 载体共同使用的真实认证池、QUIC endpoint、驱动和原始 UDP。入站已完成标准迁移，不重做。对应 Z003-02/03/06 的子域及原 PR04B / PR05。

## 初始调用者与迁移目标

- `crates/proxy/src/adapters/hysteria2.rs` 的实际 outbound_pool 和 claim 投影。
- `adapters/hysteria2/tcp.rs`、`udp.rs` 的真实 TCP/managed Datagram/packet-path 调用者。
- `protocols/hysteria2/src/transport/{pool,connection,managed_udp,...}` 的认证、endpoint、会话及协议状态。
- `crates/transport` 中实际 QUIC/UDP/驱动原始所有者；必须复用真实对象，不能另外创建演示 endpoint 或并行兼容池。

以当前支持配置/载体为能力清单，迁入已有 ComponentFactory、Stream、Datagram 和适用的 carrier 角色；协议继续拥有 QUIC 认证、协议 framing、会话/拥塞及私有池键，宿主消费标准角色并统一路由、计量与取消。不让通用 RuntimeHost 识别 Hysteria2 配置，不把协议库变成 proxy/runtime 的依赖方。

## 实施与关闭边界

1. 在 PR 开工记录列出该协议全部现有生产调用者和支持条件，给出它们与实际同一池/endpoint 的对应关系。范围不只挑 literal/TCP 成功样例，不新增原来不存在的组合。
2. 将实际原始 packet I/O、endpoint、协议 owner、关键 driver 和转交 Stream/Datagram 用户接入同一持久宿主和 TaskScope。安装原始 owner 在 await 前，保留实际 provider 的关闭凭据与中断状态。
3. 标准 Stream/Datagram 和仍需 carrier 的调用者共用同一实际资源代次；历史角色不得保活旧池或从新池取得业务。关闭逻辑流不等于 endpoint/native 关闭；协议报告、任务 join、用户归还和原始 provider 分别核对。
4. 替换按受影响消费者与依赖清理，旧原件全部确认后才构造/启动替代。网络切换、配置替换、失败重建和现有解析/拨号约束保持；不得偷偷用 system/direct 绕过原前缀。
5. 真实 Proxy 登记和 TCP/UDP/packet-path 派发接线完成，保持 Flow/Outer 计量、发送状态、单接收者、背压和原空包/多目标语义。迁完全部该域调用者后删除 Hysteria2 对应兼容 claim/prepare 桥，不删除其他协议仍使用的通用执行器。

共享通用契约若缺少必要表达，向 C 提交具体缺口，由唯一负责人先落实；不能以新 listener SPI、第二任务池、额外协议组合或降低原件关闭要求完成本卡。适用上限沿用既定配置机制、保守默认且按需分配；不把所有 native catalog 开关开启作为目标。

## 必须验证

- 实际 Proxy 的既有 TCP、UDP、多目标、空包及 packet-path 行为和计量；非该协议不受登记变更影响。
- 相同实际认证池的复用与身份隔离；共池 TCP/UDP 活动时的配置/网络换代、历史角色撤销和受影响消费者清理。
- 真实 driver/native/用户三类原始关闭证据；保留并检查转交视图退休，不能只测端口重新可用。
- 挂起 I/O 时取消、建立失败、清理超时/失败/Unknown、候选失败后恢复顺序；旧资源确认前无替代构建。
- 有限队列/任务/载体容量、耗尽拒绝、空闲回收；不驱逐活跃业务或有效安全历史。
- 仅启用 hysteria2 的构建、相关协议/载体定向测试，以及固定上游 tag/不可变 commit 的官方互通。取不到工具时明确待验，不升级上游规避。

完成条件是整个既有 Hysteria2 出站职责域的真实迁移、旧桥删除及必要回归。保留其他协议、System/detour DNS、完整 TUN 平台与 TEMP-WG-OUTER-UDP-01 的独立待办，不把本 PR 通过等同全部出站或 G0–G10 完成。

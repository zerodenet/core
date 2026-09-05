# TUN 与 Fake-IP 完成度路线

本文档定义 TUN 与 Fake-IP 后续实现的优先级、模块所有权和验收边界。它补充
[总体架构](./architecture.md)与 [TUN 用户态网络栈决策](./tun-stack-decision.md)，
不改变其中已经确定的依赖方向和职责划分。

## 不变约束

- `zero-stack` 只终止本机应用到 TUN 设备的链路；互联网侧继续使用操作系统 socket。
- 配置形状与结构校验属于 `zero-config`，路由决策属于 `zero-router` 和 `zero-engine`。
- TUN、DNS、Fake-IP 产生的连接都进入统一的 `Session`、Flow、路由、统计和事件生命周期。
- `Session.target` 表示逻辑路由目标，`direct_target` 表示直连 socket 目标，
  `original_target` 表示透明接管前的原始 IP；不得重新用单一字段承载三种语义。
- socket 拨号、UDP 流和生命周期编排属于 `zero-proxy`；平台绑定细节属于
  `zero-traits` 与 `zero-platform-tokio`。
- Fake-IP 分配、反查、缓存和持久化属于 `zero-dns`，不得在 TUN 或代理运行时复制映射状态。
- 原始 IP 包解析与构造保持在 `zero-stack::packet` 的纯函数边界内。
- 新配置、控制面字段和观测字段必须同时更新对应契约、验证、工程文档和公开文档。

## P0：生产可用闭环

P0 解决会造成正常流量失败、自环或泄露的正确性问题。

### 多地址连接

- A 与 AAAA 并行查询，保留各地址族内的 DNS 顺序。
- TCP 候选按地址族交错并延迟竞速；快速失败立即推进后续候选。
- 每个候选独立执行路由探测、物理出口选择、接口绑定和连接超时。
- 成功 Flow 记录实际远端、源地址和接口；失败记录可定位到具体连接阶段。
- UDP 使用按逻辑目标和地址族稳定选择的候选，不永久固定 DNS 返回的首地址。

### UDP 出口生命周期

- direct UDP 的 socket、目标和响应索引使用明确且互不混淆的身份模型。
- 出口拓扑变化发布单调递增 generation；新流使用新 generation，旧流按策略排空或重建。
- 无可用物理出口时 fail closed，不能让 socket 重新进入受管 TUN。
- 明确每目标/端点映射、空闲超时、容量和失败退避，并覆盖持续流量与拓扑切换测试。

### 严格路由与防泄露

- `strict_route` 同时约束路由事务和运行期泄露保护，不只检查启动安装结果。
- Windows 使用 WFP、Linux 使用 nftables，macOS 使用 pf 或等价平台机制实现 kill switch。
- 放行面仅包含受管物理出口、代理节点和显式 DNS bootstrap；热更新事务失败时保留旧保护。
- 正常停止、崩溃恢复、权限不足、多实例竞争和网络切换都有确定性回滚语义。

### DNS 出站策略

- 系统 DNS 出口保护独立于 DNS 拦截开关：`auto_route` 开启且未配置内置 DNS，或关闭拦截时，仍为系统解析服务器准备物理出口绕行地址。开启拦截仍要求配置内置 DNS；严格路由下无法发现必要系统 DNS 地址应在接管前失败。关闭 `auto_route` 时不安装这些受管路由。

- DNS 后端拥有显式超时、按配置声明的回退链、地址族策略和响应有效性检查。
- direct DNS、经节点 DNS 与默认 DNS 的执行路径使用中性计划模型，不在后端实现中读取代理配置联合类型。
- 污染或不可接受响应的判断只作用于当前查询，不隐式改变流量路由策略。
- bootstrap 和所有 DNS socket 服从同一出口 generation 与严格路由保护。

### IPv6 Fake-IP

- Fake-IP 配置可表达互不冲突的 IPv4 与 IPv6 地址池，并分别回答 A 与 AAAA。
- 两个地址族共享规范化域名身份、TTL、容量、LRU、持久化和冲突统计语义。
- 到期、淘汰或管理清除释放的地址先进入一个完整 TTL 的 `RETIRED` 隔离期；
  隔离地址不可反查或跨域名复用，池压力必须显式失败而不是回落真实 DNS。
- 状态日志版本化，旧的仅 IPv4 状态可以安全迁移或明确隔离，不允许错误复用。

## P1：通用成熟能力

P1 补齐复杂客户端、细粒度接管和长期运行场景。

- real-IP reverse mapping：保留真实 DNS 答案到域名的有界反向索引，为非 Fake-IP 透明流量恢复逻辑域名。
- 嗅探：在既有 TLS SNI 之外支持 HTTP Host 与 QUIC Initial/SNI，并为 ECH 定义不猜测域名的回退策略。
- 选择性接管：支持 CIDR、规则集、接口、源地址、端口、进程、UID、包名和 MAC 等条件；
  条件编译到路由计划，平台采集只提供中性元数据。
- UDP NAT 语义：显式定义 endpoint-independent/dependent mapping 与 filtering、端口保留、容量、超时和 ICMP 错误。
- 网络生命周期：覆盖休眠恢复、DHCP、前缀变化、VPN 叠加、 captive portal 和多实例资源所有权。

## P2：性能与高级协议能力

P2 在正确性闭环之上提高极端网络和高吞吐场景的能力。

- 本地 TCP 栈评估并按证据补充 SACK、window scale、timestamps、ECN 等扩展；
  任何实现继续服从有界状态和确定性测试要求。
- 在既有 traits 边界后评估 `system`、自研栈和可选成熟用户态栈的选择或混合模式，
  不改变 Session、Flow、路由和控制面契约。
- Linux `auto_redirect`、GSO/GRO、批处理、多队列和减少拷贝以独立能力落地，不侵入协议实现。
- 观测补齐完整拨号候选、每次失败、实际远端/本地源、原始 OS 错误、出口 generation、
  DNS 后端与防泄露状态，并保持控制面向后兼容。

## 交付与验收

每项能力独立提交并满足以下门禁：

1. 先确定所属 crate、运行时门面和不可跨越的依赖边界；若边界变化，先更新架构文档。
2. 配置与控制面变化先定义 ADT、默认值、校验、热更新和回滚语义，再接入执行路径。
3. 单元测试覆盖纯状态与失败分支，非特权集成测试覆盖跨模块生命周期。
4. TUN 行为在 Linux、macOS、Windows 的特权 E2E 中覆盖 IPv4、IPv6、双栈、网络切换和崩溃恢复。
5. 执行 `cargo fmt --all --check`、`cargo check --workspace`、
   `cargo test --workspace` 与 `cargo clippy --workspace --all-targets`。
6. 一个 PR 只交付一个可独立回滚的能力闭环；P0 未通过跨平台门禁前不把对应项目标记为完成。

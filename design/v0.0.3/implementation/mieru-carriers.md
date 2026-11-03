# Mieru 节点 DNS 与 UDP underlay

本地 develop / v0.0.3，接续 [完整 profile / 标准 UDP 角色](mieru-profiles.md)。Mieru 与 mieru-config 继续以 3.33.0 为基线。未提交、未推送。此批仍是 native 库及真实宿主 provider 接线，**不是全部迁移完成，也没有开启生产登记**。

## 当前实现

`PoolProfile::new` 保留 TCP / UDP underlay 选项，完整凭据集合的身份包含 underlay；`tcp` 便捷入口继续要求 TCP。标准 Stream 与 Datagram 角色均使用同一个实际 ClientPool、完整 profile 身份、宿主代际和一个总连接预算。TCP underlay 使用原 reader/writer，UDP underlay 使用原 packet reliability / ACK / window / framing 实现，不新建 UDP 池。

通用平台新增 `ControlledDatagramCarrier`，与 DNS 的 `ControlledOutboundDatagram` 查询授权分开。它绑定完整 endpoint / dial / network、真实 owner 和 TaskScope。原始 `ControlledDatagramOwner` 在同步原生创建前进入持久 slot；唯一 I/O view 和唯一原始 control 在交给协议前绑定同一 slot。Mieru 同步准备原 packet driver，以同 slot 的 tag 登记唯一真实任务，再等待 OPEN_SESSION。没有包装旧 detached JoinHandle，也没有增加辅助 worker。

关闭先取消业务，等待实际 driver 的 TaskScope join，再由原 datagram control 执行原始 OS close。原始 native/users 凭据、逻辑 TCP / UDP 视图和 pending users 都完成后才能建下一代。Drop、端口可绑定、cache 清空和计数归零都不能签发关闭凭据。整池关闭接收的报告在后续 await 前持久保存；拨号中的失败候选须先收取匹配原始凭据再继续，只保留当前未结清 slot，不累计已完成 native 历史。

`ResolvingCarrierOpener` 接受实际宿主 DnsService，以 Node 角色、调用方期限/取消、expected resolver generation 和实际 egress generation 解析。候选保持原始域名及完整 binding，不把域名改写为另一个配置 literal，不调用系统 DNS fallback。每个候选拥有独立原始 slot；失败后须取得该原始 slot 的关闭凭据，才可复用同一个协议连接预算尝试下一个候选。解析前已占用 pending pool reservation，但还没有 Mieru native acquisition；此时取消只归还未使用的协议预算，DNS 查询的实际 owner/TaskScope/native 仍由 DNS provider 证明。

候选准备和缓存复用都检查 resolver / network generation。resolver generation 是实际 DnsService 的代际，不能由相同的网络数字替代。实际 Mieru packet driver 的错误通过原 TaskJoinWitness 向逻辑流保留原 source；协议报告不重复签发任务退出。

## 验证边界

新增回归使用实际 ComponentCatalog / HostResources / TaskScope、原始 TCP/UDP 控制器、加密 Mieru peer 和真实 DNS 报文。DNS 使用已有 owned-UDP query/root 授权；DNS 查询 join 与原始关闭均独立验证。首个 IPv4 DNS 候选使用 TCP 无法连接的有限广播地址，随后连接真实 IPv6 Mieru peer。独立低层回归检查失败前已安装实际原始 TCP control、失败后仍占用容量，以及匹配的原始关闭凭据先于下一次连接。测试对真实生命周期报告只做观察，不修改或补发凭据。

UDP underlay 回归同时承载 TCP 与 UDP 业务，验证共用认证连接、空 UDP payload、未使用凭据触发整池替换、保留 RX 阻止后继构建，以及原始关闭报告先于新 owner 构建。唯一 UDP peer 地址数量不是 native close 证据，新的原始 socket 可能复用已释放端口。

固定源码结果及原始失败另记于 本批验证记录（历史材料见检查点 `8acd1cca`），旧批结果不覆盖本批源码。独立低层夹具曾漏消费 Scope join，关闭按预期保持 unfinished；补上真实 next_exit 后复验，不放宽断言或截止时间。首轮完整测试为 3781 通过、2 失败、175 跳过，失败源码指纹和完整日志保留：默认拒绝接口遗漏 NotSubmitted 标记；DNS 夹具原先假设已 bind 未 listen 会立即拒绝，但 Darwin 会等待拨号超时。前者修正错误元数据，后者改用确定不可连接的真实候选；原有负向断言、关闭要求与期限均保留。该失败结果不作为当前源码的通过证据。

DNS 夹具随后暴露了另一处断言错误：将重试阶段已收取的失败候选报告，要求在最终 lifecycle shutdown 再报告一次。最终报告现在明确只含最后一个成功 original，独立低层回归同时直接检查失败候选的实际原始 control、匹配关闭凭据及关后才能继续的单容量约束。既不放松失败候选的原始关闭条件，也不为满足累计数字增加无界报告历史或重复凭据。该诊断失败及其固定源码指纹一并保留。

DNS provider 的既有关闭报告按资源声明有界聚合，两次查询对应一条包含 `2 original closes` 的声明报告；夹具同时校验这个原始关闭数以及实际 provider 的 physical_confirmed（native、users、query joins 和 admission），不把报告条数误作 socket 数。该夹具的初始错误预期与修正前源码指纹保留在验证记录中。

## 尚未接通：relay prefix 与生产装配

新 native 契约下的非空 prefix 仍被明确拒绝，不能借新 DNS/UDP 接口退回空 prefix 的直接拨号。当前生产继续由原有兼容路径处理 relay 配置；这里的拒绝仅针对尚未登记的 native 契约。以下工作尚未完成：

1. 实际 prefix role / generation 的宿主授权与转交借用。当前 Service 原始授权要求同一个真实 scope；另一个 Mieru driver 的 witness 不能冒充 donor 的 caller。ServicePurpose 标签和虚构业务 SessionId 都不能补足授权。
2. donor 所拥有的根 native 与最终 Mieru pool 所拥有的逻辑载体/driver 的原始关闭链。逻辑借用归还不是 donor 原始 FD 关闭；最终池不能签发不属于自己的 native 凭据。共享 donor 的其他使用者也不能被错误关闭。
3. 同协议多跳及动态选择的实际所有权图。简单把整池依赖挂到其自身的某个角色，会产生 Pool → Role → Pool 的虚假静态环；不能为绕过它复制 ClientPool、保留 registry/整份旧配置或枚举失控的所有路径组合。需要按真实 carrier 借用关系接线，并让替换事务收取原始 owner 的证据。
4. Proxy 的实际 resolver / egress / prefix provider 装配、真实 endpoint / Outer accounting / health 观察，以及 legacy reload 与 native 全路径的统一切换。本批没有把测试 opener 注入当作生产装配，也没有把 literal TCP 子集登记上线。

后续借用契约还必须区分 actual driver、转交 I/O view、逻辑 channel control 和 donor 原始 native control。前缀初始化需要真实执行任务的宿主授权；不能在 inline 握手前放一个不做业务的占位任务，也不能事后将任意 witness 挂成 driver。若同一真实任务继续尝试下一个候选，失败候选的 native settlement 不得反过来等待该任务自身 join；只允许匹配原 owner 的、未转交业务载荷的关闭凭据授权重试，最终关闭仍要收齐该任务的真实 join。现有使用债务只能用于取消/等待，不能替代这些凭据。这些接线尚未实现，不能作为本批已完成项。

上述条件统一满足并通过真实关闭/故障/取消及消费者回归后，才允许修改生产 register.rs。当前 MieruAdapter 继续持有原来唯一的兼容池，native 初始认领仍拒绝任何曾使用 legacy driver 的池；没有 native / legacy 双池或第二内核实例。

## 客户端与面板

本批不改变配置 ADT、API/V1 命令/事件、Engine、FFI/gRPC、CLI 或消费者源码。外部协议 wire 与 UDP carrier 原有实现复用。兼容性按本批同一不可变二进制执行客户端 IPC 与面板原有发布回归；本地 shim/fixture 不等同于真实 GUI/TUN、生产部署、外部官方工具或 1 CPU/1 GiB 压力资格。

## 本批固定源码验证

431 项源码指纹在完整运行前后保持一致。Rust 1.99.0、全工作区、全部 feature、16 MiB 测试线程栈和正常测试并发：155 个目标，3784 通过、0 失败、175 跳过，完整结束标记 exit=0，耗时 2174 秒。六项新增回归均在这次完整运行中通过；外部工具/特权跳过项仍按原条件保留。Rust 1.98.0 的全工作区、全部 target/feature 严格 Clippy（no-deps，-D warnings）也通过。

同一不可变二进制 SHA-256 为 `2f1bd0e18702d7636bdba1e1f2e67364f2a38ef331d6a556430ad77d38bcdc8b`。客户端原有 IPC 回归通过（19.18 秒），实际 runtime 自然退出 0、无强制信号；面板原有本地发布回归通过（82.48 秒，原 120 秒期限），已有 TCP/UDP 跨三个 publisher 进程保持连通。消费者源码保持干净。完整命令、源码指纹、原始失败、同二进制消费者记录及校验值见本批 validation.json。

这些结果只给本批 native 库及真实宿主夹具资格。上面的 relay prefix 和实际 Proxy 装配仍未实现，生产登记保持关闭；本地 develop 未提交、未推送。

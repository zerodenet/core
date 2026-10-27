# Mieru 整池 profile 与标准 UDP 角色

本地 develop / v0.0.3，接续 [固定根载体阶段](mieru-pool.md)。不提交、不推送。冻结方案的验收目标与剩余任务保持不变。

后续节点 DNS / UDP underlay 接线见 [载体阶段](mieru-carriers.md)；下文保持本批历史源码与资格语境。

## 本批实现

`PoolOptions::with_profiles` 冻结同一实际 `ClientPool` 的完整 outbound profile 集合。每个 profile 包含 tag、endpoint、用户名、密码、TCP 载体选项和完整 upstream binding；集合按 tag 规范排序，拒绝重复或空集合。secret-free 的池身份包含整个集合的进程内带密钥摘要，未使用的凭据变化也改变整池身份。域名及非空 prefix 仍在冷验证时拒绝；这不是这些路径已获得原始关闭权限的声明。

不同 endpoint / dial / network binding 对应各自实际 `CarrierOwner`，按资源声明 ordinal 登记到同一个宿主组件和同一 `TaskScope`。协议的 pending/live/retiring/unknown record 仍受一个总预算限制，不按 endpoint 乘大可用载体数。`build_with_carriers` 要求冷装配提供与每个声明绑定逐项一致的真实 opener；它不创建第二个池、另一份 egress controller、兼容 fallback 或额外 worker。

冷装配通过 `ResourcePreparation::candidate` 只在本轮借用候选配置，装配桥可据此生成完整集合。recipe 不保留该借用或整份配置。client 模型持有 profile / carrier / 输入值，component 只消费这些值做装配，client 不反向依赖 component 或 transport 模块。绑定后的角色只保存自己的凭据、弱池引用和固定长度的 secret-free 身份；完整凭据集合由规范池 recipe / 实际 owner 持有。角色启动检查实际 owner 的整个 recipe 身份，而不只检查预算和自身密码。

标准 `StreamFactory` 同时暴露 Stream 和 Datagram 角色。Datagram 在同一个认证 TCP 载体上建立独立 UDP ASSOCIATE 逻辑流，继续使用协议原有 SOCKS 请求、响应与 UDP 帧编解码。TCP 与 UDP 可以复用同一认证连接和唯一真实 driver，没有另建 UDP 池。这里只覆盖 **TCP underlay 上的 UDP role**，不代表 UDP underlay 已迁移。

Datagram TX 保留部分写入和 flush 状态；部分发送后更换帧会隔离该逻辑会话并报告 Unknown。RX 保留取消后未消费的帧，支持空 payload；固定域名发送目标仍可接收代理返回的已解析源 IP，发送许可不因此放宽。没有新增每会话后台任务或发送队列；发送帧和接收帧均有明确长度上限。逻辑 Control 取消业务后等待实际 TX/RX 视图归还，再显式完成逻辑 stream shutdown 并归还池 user lease；它只报告逻辑资源归还，不能签发 native FD 关闭或共享 driver 退出凭据。

整池关闭逐个收取原 owner 的报告，收到后立即存入同一个持久关闭接收者，再等待下一个 owner。全部原始 native/users 凭据、实际 driver joins 和 pending / 已转交的逻辑用户齐全后，才能清缓存并开放下一代；中断关闭不能丢失此前报告。仍持有 UDP RX 或旧 TCP read half 都会阻止替换。

## 资格与生产边界

测试使用真实 ComponentCatalog、HostResources、TaskScope、原始 native provider 与协议加密 peer；测试 opener 注入仍不是生产登记。覆盖不同 endpoint / dial 共池预算、重排不替换、仅修改未使用凭据引发整池替换、历史 role fence、TCP/UDP 认证连接复用、UDP 逻辑关闭保留 TCP、保留 UDP RX 阻止新 owner，以及部分 UDP 帧的取消/Unknown 语义。具体执行结果另记于本批验证文件，历史批次结果不覆盖本批源码。

生产 MieruAdapter / register.rs 继续使用同一个原有兼容池，尚未开启 native 工厂。仍需完成：

1. 实际 node/bootstrap DNS 的候选选择、取消、失败重试和 network generation fence，绑定到宿主原有 resolver / egress provider。不能把域名直接替换成未经该 provider 选择的 literal 来通过声明验证。
2. relay prefix 的实际角色依赖、逻辑载体归还与根资源关闭证明。最终池不能将前缀拥有的 native FD 记为自己的原始 FD，依赖顺序也不能代替实际关闭凭据。
3. UDP underlay 的实际 datagram 原始 owner、唯一 packet driver 监督、用户归还及 native 关闭凭据，纳入同一个总预算和同一池代际；不得并行 native / legacy 双池。
4. 上述路径统一满足证据条件后，由真实 Proxy 装配注入 provider，再启用生产登记并运行完整协议/消费者资格。

本批没有修改配置 schema、API/V1 命令事件、FFI/gRPC、客户端或面板源码；兼容结论以本批实际测试为准，不等同于真实设备、生产部署或 1 CPU/1 GiB 压力资格。

## 本批固定源码验证

验证记录与逐文件源码摘要（历史材料见检查点 `8acd1cca`）对应 404 项本地源码/锁状态；HEAD 为 `b963c864`，源码仍未提交。Rust 1.99 完整 workspace / all-features / 16 MiB test stack 最终为 **3778 通过、0 失败、175 跳过**，155 个目标，499 秒。Rust 1.98 严格 workspace / all-targets / all-features Clippy（`-D warnings`）通过，未加 lint suppression；格式、314/95 测试布局、190/27 分层边界、21 项 CI 范围及冻结正文校验通过。

诊断阶段完整回归为 3777/0/175，当时继续整理了源码，不计为最终固定源码资格。固定源码首次完整回归为 3777/1/175：既有 observer_handoff 用例已取得实际原始关闭和 join 回执后，在末尾重新绑定 UDP 端口时遇到 `AddrInUse`。原测试与 native close 实现均未改；同一源码的单项复跑及原并发完整重跑通过，没有改断言、截止时间、并发或跳过条件。竞争端口复用是推断，未捕获竞争者身份；原始失败、源码及分析（历史材料见检查点 `8acd1cca`）完整保留。中间一次严格 lint 指出手写 hash_one，已改用标准库 `BuildHasher::hash_one` 后重新检查。

客户端 IPC 与面板使用同一个只读内核副本，SHA-256 为 `16b5582bca591cbe6da4a90f6b3c6122d0ba0dc18fbd0c99ba8d3b659f61423e`。客户端原有 IPC / apply / 拒绝与回滚检查通过，20.90 秒，内核随父输入关闭正常退出 0，无 terminate / kill。面板原发布回归通过，84.26 秒：PID 19304、core `80290bbb4d5eb7dafeca0513a5460bbf`、revision 2，TCP / UDP 跨三个发布进程保持可用。两个消费者仓库均保持原清洁 HEAD；API / config / engine / FFI / gRPC / CLI、依赖锁和上阶段其余源码均保持原状态，所列 Mieru / 通用冷装配修改除外。

以上仍是本地 fixture 与 native 库资格，生产 Mieru 工厂没有登记；真实 GUI / TUN 设备、生产部署、固定官方外部工具及 1 CPU / 1 GiB 压力资格另验。未暂存、提交或推送。

# 控制面兼容性与破坏性变更

## Unreleased: Native Stream component integration

- Rust component factories now declare `ComponentSpec::task_limit: Option<NonZeroUsize>`; `None` grants no internal tasks. Interrupted start retains the installed owner, protocol cleanup and registered driver shutdown progress together, and interrupted task cleanup retains already collected exit IDs and original failures for retry. Direct/SOCKS5 still declare no internal tasks; this does not change public wire configuration or the FFI ABI.
- Ordinary Direct and SOCKS5 business streams now use cold-built component roles while retaining V1 configuration, API/event envelopes and existing FFI ABI. SOCKS5 retains protocol-owned credential validation and framing; existing rejection messages, stages and endpoints are covered by direct comparison tests. Internal DNS/UDP and mixed legacy relay migrations remain in progress.
- Rust runtime integrations must handle the explicit Service request identity and use `Some(existing_deadline)` for an existing absolute OpenContext/CarrierContext deadline. `None` retains native operation budgets; ordinary business connects retain existing DNS and per-candidate socket timers. Resource start/close/accept/handshake deadlines remain explicit.
- Native SOCKS5 fallback proceeds only when no CONNECT request has been submitted on an empty root prefix. Partial submission, an executed prefix or an unconfirmed request stops replay. Local cancellation, revocation and stale local plans remain neutral to remote health.
- Relay chains whose network nodes all expose native Stream roles now compose strictly shorter, endpoint-bound prefixes through those roles. They retain actual business/component identities, original named-node wire targets, per-hop Outer observation and existing relay Flow counters. Mixed chains retain their existing executor pending migration; revoked roles cannot fall back to a legacy claim.
- The Hysteria byte-stream carrier keeps at least two packets of congestion window at small bandwidth-delay products, matching Quinn's full-packet send reservation. QUIC close-only packets bypass the data congestion/pacing gate as required by RFC 9002 section 7.2, so queued application frames cannot strand CONNECTION_CLOSE. Existing authentication, negotiated rates, payload/FIN assertions and deadlines remain unchanged; both failure mechanisms have captured negative regressions.
- Browser Dialer retirement now reports `io::ErrorKind::ConnectionAborted` with the existing message. Permanent retirement must terminate byte I/O; returning `Interrupted` let retrying I/O helpers spin inside one poll. Existing lifecycle, reuse, peer-close and deadline checks remain.
- HTTP/1 XHTTP packet upload ownership retains existing request, byte and queue budgets and deadlines, and yields after bounded admission handoff so the consumer can progress before the next pipelined request. The receipt remains bounded ownership acknowledgment. Captured negative/positive regressions and the separate pinned reference-client byte-count qualification issue are recorded under design/v0.0.3/implementation.

## Unreleased: Host Wintun activation and XHTTP upload receipts

- Existing host-owned Wintun adapters validate their configured addresses and MTU before opening an exclusive packet session, then wait for Preferred address state on the activated link. A dormant Deprecated address is not treated as ready; duplicate, invalid and expired addresses remain errors. Host addresses, routes and MTU are never modified.
- XHTTP HTTP/1 packet uploads acknowledge completely received, validated and byte-budgeted ownership before waiting for ingress queue capacity. This allows a reused upload connection to deliver a missing sequence instead of waiting for a receipt that depends on that sequence. The admission task retains its request permit, payload reservation and deadline; receipt does not mean business delivery. Queue and listener budgets, duplicate rejection, cancellation and HTTP/2/3 admission backpressure remain enforced. No configuration or public API fields change.

## Unreleased: Shared raw-IP listener egress

- Native shared raw-IP listeners apply the current physical-egress selection at the datagram send boundary. Their outer packets no longer rely on being recaptured and routed through TUN as business flows. Explicit outer proxy carriers keep their existing transport ownership.
- Per-message interface hints preserve the listener's source port and receive binding. Route hints are bounded and expire after one second or immediately on egress generation changes; unavailable physical egress and failed interface hints do not fall back to an unbound send. Linux firewall marks accompany the individual datagram. No configuration or API fields change. On macOS, specifically addressed IPv4 listeners use a synchronous, checked temporary interface scope instead of IP_PKTINFO, preserving their configured receive address and restoring the previous interface scope before returning to the executor. Send or binding errors never trigger an unbound retry.

## Unreleased: DoH connection recovery after network changes

- DoH HTTP/2 pool identity now includes the existing physical-egress generation for native and detoured queries. Connections prepared before a network change cannot enter the current pool or send a new DNS request afterwards, even when the physical interface name remains unchanged.
- Failed or cancelled exchanges, including cancellation by the configured backend deadline, retire only their own connection. A delayed cancellation cannot evict a replacement connection. Healthy connection reuse, native platform binding, explicit detours, DNS dispatch, fallback selection and query deadlines retain their existing contracts; this does not introduce automatic query replay or new configuration fields.

## Unreleased: Connector reload recovery and sample delivery

- Tokio 最低版本声明为 1.53.3，Unix TUN 与路由监测使用对应的注册接口，保持描述符直到注销后再关闭；macOS 接收宿主描述符时继续保留其拥有关系。V1 配置、控制面及 TUN 路由语义不变。

- In-process dispatcher replacement retains an instance-qualified replay cursor and its bounded volatile delivery state. Missing retained history is reported through existing replay-gap status. This is not crash recovery without an outbox; receivers still deduplicate at-least-once delivery.
- Newly admitted `flow.updated`, `stats.sampled`, `stats.scopes_sampled`, `stats.host_interfaces_sampled`, and `endpoint.stats_sampled` are best-effort samples and do not enter the outbox. Samples already persisted by older versions retain their delivery/ACK obligation. Lifecycle facts, `stats.reset`, business usage and quota state are preserved.
- The source bridge stops receiving while preceding events cannot be admitted. `api.dispatcher.source_queue_max_bytes` defaults to 8 MiB of serialized event bytes, alongside the existing count limit (`replay_batch_size`). Reservations follow live events until sink admission. One existing oversized event may reserve the whole byte budget and enter alone; one event waiting for admission and a single replay/bootstrap event are additional bounds. This is not a heap/RSS ceiling, nor a byte budget for pending/in-flight delivery copies; those remain separately managed. Replay processes one owned event at a time. No existing durable delivery is deleted to enforce these limits.
- `api.dispatcher.max_in_memory_delivery_bytes` adds a 16 MiB default serialized-byte charge for the combined queued, retry/ACK-retry and in-flight delivery working set (valid 1..=256 MiB). The existing count limit remains independent. Durable entries beyond the budget stay indexed on disk. An older entry waiting for byte room pauses additional live working-set admission so continuous smaller events cannot starve it; volatile facts apply source backpressure. One oversized event is admitted only when the working set is empty. Lowering a reload budget retains inherited volatile facts and stops additional admission until they drain; it never acknowledges or deletes durable obligations.
- `sinks.delivery_buffer` optionally reports the shared dispatcher `byte_budget`, `reserved_bytes` and `over_budget` state, repeated per sink and not summable. Live charges measure event serialization; recovered charges conservatively include the journal frame. Shared fan-out obligations are charged separately. Carrier bodies, decoding scratch, source buffers, one prepared input, journal indexes and other runtime memory are separate: this is not an RSS limit. Oversized events and inherited state can exceed the configured charge. The application status projection retains its existing refresh interval.
- Connector workers and asynchronous sinks share immutable event ownership rather than cloning payloads for each handoff. A differing sink `source_id` still materializes a sink-specific event for compatibility; those copies are charged independently once queued. Only the current prepared input is cached during admission retries, avoiding repeated payload copies; reload invalidates that preparation for the new sink settings. Durable paging caches only the last candidate frame length (invalidated on compaction) to avoid rescanning a large record on every admission poll, checks lengths before decoding and does not retain whole encoded frames. Journal/webhook encoding pre-sizes scratch to avoid geometric Vec growth; JSONL delivery streams through a small I/O buffer instead of materializing a complete frame, and failed writes are not retried by buffer drop. JSON decoding and carrier encoding still require bounded-by-record scratch; this is not a zero-copy or arbitrary-size-event memory guarantee. Journal index metadata uses the disk-backed derived index described below.
- Outbox recovery, inspection, paging and compaction now use an exact disk-backed derived index instead of retaining two full Rust maps of pending identities. JSONL remains the only persistent source of delivery/ACK truth; its format and existing pending obligations are unchanged. Index tables contain identity/offset/count metadata only, not event payloads. SQLite is bundled only with the optional `event-dispatcher` feature; basic metering and querying remain independent of Connector. Each scratch connection targets a 256 KiB page cache with spill enabled and memory mapping disabled. Recovery/index construction commits at most 128 journal operations per batch; paging iterates the offset index without collecting all identities. Compaction temporarily uses two small page caches and streams frames into the replacement journal, preserving latest-offset order, all unconfirmed identities, payloads, attempts and ACKs.
- Derived indexes use private scratch files in the operating system temporary directory, including on Android where SQLite's anonymous temporary databases would otherwise be forced into memory. Deployments should place the OS temporary directory (for example via TMPDIR) on disk-backed storage when memory pressure matters; tmpfs charges remain system/cgroup memory. This trades RAM for disk I/O and temporary disk capacity proportional to pending metadata; it is not an RSS cap, a disk quota, or constant-cost recovery. Caches, the largest individual identity/frame scratch, configured-sink status projections and SQLite internals still consume memory; OS file cache is separate. Normal close removes scratch files; after abrupt termination leftover private scratch directories are disposable under the OS temporary-file cleanup policy, never a recovery source. Read-only preflight requires temporary storage but does not modify the inspected journal. Scratch allocation, full-disk or index-write failures are reported as index failures, never as journal corruption or successful delivery. The optional Rust Connector API adds `ConnectorError::OutboxIndex`; exhaustive matches must handle it, while control-plane envelopes/configuration stay unchanged. After a durable append whose index update fails, paging/status refuse stale offsets and the next persistence/ACK retry rebuilds the index from JSONL. No pending event is dropped to satisfy memory limits, and Engine cumulative meters, business usage and quotas are unaffected.
- Connector records are notification/delivery state, not authoritative cumulative traffic counters. Dropped/evicted samples do not decrement Engine counters, business usage or quotas. Consumers recover cumulative observations by queries, checking core instance and statistics epoch; replay gaps cannot restore per-flow detail absent from retained history. Cross-restart billing durability is not implied by in-memory statistics or a best-effort subscription.
- `sinks` query adds optional `source_buffer` admission gauges: channel capacity, byte budget, current reservations, high-water values, waiting event bytes, oversized admission count and pause state. These describe the shared dispatcher source, are repeated per sink and must not be summed. They exclude replay/handoff payloads and delivery copies, are read independently and reset with dispatcher replacement. `reserved_bytes` is budget charge, not RSS; oversized admission charges the whole budget. Missing fields mean unavailable on older cores or synchronous sinks.
- Single-file ordinary log rotation (`max_files=1`) now replaces the active file at its size boundary. Rotation errors propagate without resetting length accounting. One oversized write/event remains intact and rotates before the following record. Outbox retention is unchanged.
- Engine-generated opaque event IDs include an instance-local identity ordinal, preventing same-millisecond facts from colliding in durable deduplication. Identity allocation is separate from published sequence, so subscription snapshots and prepared completion facts create no replay gaps. Persisted IDs and caller-provided external IDs remain unchanged; consumers must treat IDs as opaque.

本文记录会影响 GUI、SDK、面板、事件 Sink 或进程内 Rust 集成的控制面语义变化。公开接口与事件说明由 [ZeroDeNet 文档仓库](https://github.com/zerodenet/docs)维护；本文只维护 Core 的版本边界和迁移要求。

## 消费者如何判断兼容性

外部消费者连接内核后应依次检查：

1. `health.engine_build_id`，确定实际运行的内核版本；
2. `capabilities.api_id` 和 `capabilities.schema_id`，确定请求与事件信封版本；
3. `capabilities.features`、`build_features` 和协议矩阵，确定当前构建实际启用的能力；
4. 本文对应版本的语义变更，再决定是否启用兼容分支。

兼容性标识的含义：

| 标识 | 当前值 | 何时必须变化 |
|------|--------|--------------|
| `api_id` | `zero.api.v1` | 请求、响应信封或既有字段出现不兼容 wire 变化 |
| `schema_id` | `zero.event.v1` | 事件信封或既有事件字段出现不兼容 wire 变化 |
| `engine_build_id` | Cargo 包版本 | wire 兼容但行为、时序或恢复语义发生变化 |

新增可选字段、未知事件类型和新增 capability 通常保持向前兼容；消费者必须忽略不认识的可选字段和事件。改变 ACK 时序、快照含义、增量合并规则、重放范围或既有字段含义，即使 JSON 形状不变，也必须在本文登记。

## 版本矩阵

| 版本 | 影响面 | 迁移结论 |
|------|--------|----------|
| `Unreleased` | VMess cipher 配置 | 旧私有 `zero` 配置需迁移为 `zero-plus`；`zero` 现为 Xray 标准模式 <!-- version-contract:unreleased-row --> |
| `0.0.1` | 首个统一正式版、TUN 路由恢复 | 发布编号重置；配置与控制面仍为 V1，旧版本需手动安装 |
| `0.0.16-rc.202609070904` | Direct 入站、监听热更新、构建能力发现 | Direct 默认绑定 TCP 与 UDP；仅需 TCP 时使用现有 `udp.enabled: false`，面板按能力事实判断支持 |
| `0.0.16-rc.202609060636` | CLI 配置预检查、内核安装器 | `zero validate` 不再启动运行时或访问 Fake-IP / 配额持久化状态；可与运行中的内核并行校验 |
| `0.0.16-rc.202609051609` | - | No pending compatibility changes |
| `0.0.16-rc.202609050728` | GUI 首次启动、进程内 Proxy 集成 | 空入站且无 TUN 配置进入管理待命；控制面可应用第一个监听，移除最后监听后继续待命 |
| `0.0.16-rc.202609041712` | - | No pending compatibility changes |
| `0.0.16-rc.202609041703` | - | No pending compatibility changes |
| `0.0.16-rc.202609041004` | - | No pending compatibility changes |
| `0.0.16-rc.3` | - | No pending compatibility changes |
| `0.0.16-rc.2` | - | No pending compatibility changes |
| `0.0.16-rc.1` | - | No pending compatibility changes |
| `0.0.15` | - | No pending compatibility changes |
| `0.0.15-rc.4` | - | No pending compatibility changes |
| `0.0.15-rc.3` | Diagnostics API 消费者、Connector 运维与事件 Sink | `diagnostics.trace_route` 改为执行真实会话路由追踪；Connector 对事实事件施加有界内存与可选 outbox 背压，采样事件改为仅 best-effort 且不持久化；默认重试次数由 3 调整为 10 |
| `0.0.15-rc.2` | 构建脚本、事件消费者、Webhook 接收端 | 公开 Cargo feature 改用 kebab-case；引擎生成的 `event_id` 增加每次启动唯一的随机 epoch；开发期固定中心 API 被撤销，Connector 收缩为通用 Webhook 事件投递；认证项速率改为 Zero 主体策略聚合 |
| `0.0.15-rc.1` | 进程内 Rust `EventSource`、事件 Sink | Rust 实现者必须迁移到实时 `EventStream`；IPC/HTTP/gRPC GUI wire 无变化 |
| `0.0.15-rc` | GUI flow 生命周期 | 订阅 ACK 后以 `flow.snapshot` 建立活动连接基线，再合并 flow 增量 |

## Unreleased

### Connector 持久化预检与配置热更新

- `zero connector state`、outbox 启动回放、dead-letter 和配额状态检查改为流式读取。预检只检查打开文件时的快照，不申请持久化写锁、不修复文件，也不改变运行中的内核；日志格式及 API/event V1 标识保持不变。
- outbox 在 PUT、ACK 和启动时检查历史压缩阈值，保留每个未确认事件的最新完整记录、payload 和重试状态。真正未投递的事件不会因压缩被删除；空间不足时保留原日志并延迟压缩。
- 配置热更新只重建实际变化的应用组件。仅修改代理设置时保留 dispatcher、状态监视器和未变化的 hook；需要重建 dispatcher 时取消已有 outbox 副本的在途请求，保留持久化事实供恢复，丢弃临时采样。没有 outbox 的生命周期事实仍须完成投递；接收端继续按既有事件身份幂等处理可能的重投。

### Direct dial 与 IPv6 监听（v0.0.3）

- `OutboundConfig.dial` 新增可选 `address_family`（`auto` / `only_ipv4` / `only_ipv6`）、`interface` 和 `source_ip`。省略与默认值保持旧行为；非默认策略首期仅用于 Direct TCP/UDP。接口与源地址必须存在且归属相符，启动/重载准备与实际拨号均校验；失败不能撤销约束重试。
- `direct_outbound_dial_policy_v1` 表示此能力。原始 PacketSink 与 ICMP 不支持非默认 dial；`auto` 的 TCP/UDP 转为受限 Flow。既有 TCP 保留旧策略，仅策略变更的 Direct UDP 失效重建，期间允许短暂丢包。
- 业务 DNS 的严格族覆盖仅作用于所选 Direct 解析，不改变全局 DNS 上游 socket、Node 或控制面默认值。System resolver 只保证候选过滤与实际拨号族。
- 监听 IP 使用结构化端点；合法裸/括号 IPv6 归一化。`::` 是否接受 IPv4 保留 OS 默认，不保证跨平台 dual-stack，也不保证双 wildcard 同端口共存。
- 进程内 Rust 集成：`OutboundConfig` 字面量添加 `dial: Default::default()`；`ResolvedLeafOutbound::Direct` 添加 `dial_policy` 与 `dial_generation`，只匹配 tag 的模式使用 `..`。wire API/event V1 标识不变。
- 内部边界遵守 [工程约束](../AGENTS.md)，对应测试契约随源码维护。

<!-- Record implemented but unsealed compatibility changes here. -->

- VMess `cipher: zero` 现采用 Xray 标准的 NONE（0x05）、无 ChunkStream body 语义。此前使用 Zero 私有 0x06 格式的节点必须将配置显式改为 `cipher: zero-plus`，并协调两端升级；不保留含糊别名。标准 `zero` 与私有 `zero-plus` 不可互换。

## 0.0.1

- 经项目所有者授权清理旧 Release 和 tag，基于当前 main 源码发布统一的首个正式版。以下旧编号条目仅保留兼容性背景，不代表仍可下载的发布记录。
- 配置、控制 API 和事件信封继续使用既有 V1 契约，先前实现的能力保持不变。
- 修复 Windows TUN 路由初始化时继承系统默认 Loopback 标志的问题，以及 macOS 路由锁被继承或复制的文件描述符延长持有的问题。
- 旧版 0.0.4 至 0.0.16 的版本号高于新基线；切换至此版本需手动安装，不会通过递增版本检查自动升级。

## 0.0.16-rc.202609070904

<!-- Record implemented but unsealed compatibility changes here. -->

- 固定目标 `direct` 入站默认同时转发 TCP 和 UDP。升级前只有 TCP 的监听现在也会绑定同端口 UDP；若要保持 TCP-only，应将该入站或全局 `runtime.udp.enabled` 设为 `false`。任意一层禁用 UDP 都只绑定 TCP，热更新会重新协调监听，绑定失败通过既有事务回滚。协议配置仍为 `type`、`target` 和 `port`，不新增 `network` 字段。
- `zero build-info` 新增 `protocol_capabilities:` JSON 行，内容复用既有协议能力记录。控制器应确认当前构建中 `direct` 入站 UDP 能力为受支持后再下发 UDP 转发；旧内核接受 `udp.enabled` 并不意味着支持该转发。未编译所需 UDP 运行时的构建会在配置验证阶段拒绝启用 UDP 的 direct 入站。
- WebSocket 自定义 `Host` 现在替换拨号地址生成的默认 Host，避免发出重复 Host 头；连接地址、TLS 及其他头的配置不变。

## 0.0.16-rc.202609060636

`zero validate CONFIG` 仅校验当前构建支持的配置和资源，不再实例化 Engine / Proxy、获取 Fake-IP 状态锁或读取配额状态。安装器可在旧内核运行时执行该命令。成功不代表运行端口可用、权限满足或持久化数据兼容；后者仍由运行启动和独立状态检查负责。CLI 输出保留 `config valid` 统计，移除校验阶段误报的 `engine started` 日志。

## 0.0.16-rc.202609051609

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.202609050728

<!-- Record implemented but unsealed compatibility changes here. -->

- 空入站且无 TUN 的运行配置不再以 `NoInbounds` 退出，保持管理循环供控制器通过现有 `config.apply` 事务添加监听。首次绑定失败仍保留空配置供重试，移除最后监听后返回待命。
- API 与事件信封版本不变。依赖空配置启动失败的进程内集成应显式校验自身约束；GUI 可在导入代理配置前启动管理内核。

## 0.0.16-rc.202609041712

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.202609041703

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.202609041004

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.3

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.2

<!-- No compatibility changes in this release. -->

## 0.0.16-rc.1

<!-- No compatibility changes in this release. -->

## 0.0.15

<!-- No compatibility changes in this release. -->

## 0.0.15-rc.4

<!-- No compatibility changes in this release. -->

## 0.0.15-rc.3

### Diagnostics 路由追踪使用真实会话语义

`diagnostics.trace_route` 现在构造 TCP 或 UDP 会话并复用代理运行时的路由追踪路径。域名规则没有直接命中且路由需要解析 IP 时，会通过真实 DNS 解析结果再次匹配 IP 规则。响应中的 `effective_mode`、`route_action` 和 `matched_rule` 因此反映实际运行时决策，不再使用简化推断。依赖旧诊断结果的控制端应以新结果为准。

### Connector backlog 改为有界工作集

Connector 不再让慢速或失联 Sink 导致进程内待投递队列无界增长：

- 配置 outbox 时，事实事件先写入持久化 journal，内存只保留有界工作集，其余记录按空位从磁盘分页恢复；
- 未配置 outbox 时，事实事件通过停止继续消费事件源施加背压，不会静默丢弃；
- `flow.updated` 与 `stats.sampled` 是可丢弃采样，只做 best-effort 投递、不会写入 outbox；工作集已满时优先替换同一 Sink 的旧采样，不驱逐事实事件；
- `api.dispatcher.max_in_memory_deliveries = 0` 表示 outbox-only 模式，因此必须同时配置 `api.outbox_path`；
- `max_retry_attempts` 默认值由 `3` 调整为 `10`，显式配置不受影响。

事件 Sink 不应把吞吐采样当作可靠事实流；需要恢复保证的消费者应使用事实事件并配置 outbox。

## 0.0.15-rc.2

### 撤销开发期固定中心 API

项目尚未发布 Connector 合同，因此开发期的节点注册、同步、traffic、presence、访问配置和私有命令设计直接撤销，不保留兼容层。

已移除顶层 `push`、`PushConfig`、`/api/v1/nodes/{node_id}/*`、中心 OpenAPI、conformance 和 production gate。外部控制器通过 Zero API/gRPC 管理节点，并使用 `config.apply` 注册通用 `api.event_sinks`。Connector 只向完整 Webhook URL 推送 `zero.event.v1`，并定义 HTTP 状态确认分类。

### 公开 Cargo feature 改用 kebab-case

构建入口不再暴露下划线式能力名。构建脚本、CI 和制品 feature 校验需要完成以下迁移：

| 旧名称 | 新名称 |
| --- | --- |
| `status_api` | `status-api` |
| `event_dispatcher` | `event-dispatcher` |
| `sink_jsonl` | `sink-jsonl` |
| `panel_connector` | `connector` |
| `grpc_api` | `grpc-api` |

Rust 函数、模块和变量仍按语言规范使用 `snake_case`；本次变化只影响 Cargo feature 名称和二进制对外报告的 feature 字符串。历史候选证据保留其原始名称，不得重写后冒充新候选。

### 引擎生成事件使用跨启动唯一 ID

旧事件 ID 仅由事件类型、进程内 flow ID/序号和毫秒时间戳组成。进程快速重启后这些值可能复用，使 Connector 接收端或 Sink 把新事实误判为已处理事件。

新语义：

- 每个 `EngineEventLog` 创建时生成一个 128 位随机 epoch；
- 所有引擎内部生成的事件 ID 均以前述 epoch 限定，在同一进程内重放时保持不变；
- 通过进程内 `emit()` 注入、由调用者拥有 ID 的外部事件保持原 ID；
- `event_id` 的内部拼接形式不是公共契约，消费者只能比较完整字符串并用于幂等去重。

该变化不修改 `zero.event.v1` 的 JSON 字段形状，但修正了跨进程启动的唯一性语义。任何依赖旧 `{type}:{flow_id}:{timestamp}` 格式解析的消费者必须删除该解析逻辑，改用 `event_type`、`payload.record.flow_id` 和 `occurred_at_unix_ms` 等正式字段。

### 用户速率改为 Zero 主体策略聚合

开发态预资格期间，`up_bps` / `down_bps` 曾被描述并执行为单条 TCP/UDP flow 的限制。该语义允许同一主体通过增加并发连接绕过带宽策略，不满足 Zero 主体策略的定义。

新语义：

- 同一 `principal_key`、`policy_revision` 和双向速率组成一个 Zero 主体策略身份；
- 该身份下的并发 TCP/UDP 会话共享上传、下载 GCRA 时间线；
- revision 或速率变化建立新时间线，旧会话在确认式清退前继续持有旧策略；
- 没有 `principal_key` 的入站默认限速仍按会话独立执行。

JSON 字段形状和当时的 `zero.panel.v1` schema ID 不变。该修正发生在首个清洁 release candidate 和正式生产签字之前；历史开发态 manifest 只能证明当时的每流实现，不得继续作为当前候选产物证据。接收端无需修改 wire payload，但容量规划和限速验收必须改为并发 TCP/UDP 聚合测试。

## 0.0.15-rc.1

### `EventSource` 统一为实时订阅

旧语义存在两个不同实现：

- `Engine::subscribe()` 返回一次性的 `Vec<RawApiEvent>` 历史快照；
- `EngineHandle::subscribe()` 返回实时 `EventSubscriber`。

新语义：

- 所有 `EventSource::subscribe()` 都返回实现 `EventStream` 的实时订阅；
- `latest(limit, filter)` 只用于读取近期历史；
- `since(sequence, limit, filter)` 用于按事件序号恢复，返回 `requested_after`、`actual_from` 和 `has_gap`；
- `has_gap = true` 时，消费者不得直接继续套用增量，必须先通过快照或 Query 重建状态；
- 包含 flow 生命周期的实时订阅仍可在增量前发送合成的 `flow.snapshot`。

进程内 Rust 实现者需要：

1. 将 `type Stream = Vec<RawApiEvent>` 替换为实现 `EventStream` 的实时流；
2. 实现阻塞 `recv()` 和非阻塞 `try_recv()`；
3. 实现新的 `EventSource::since()` 游标恢复方法；
4. 不再把 `subscribe()` 当作历史查询使用。

### EventDispatcher 投递时序

EventDispatcher 从周期性事件环扫描改为持有一个实时订阅：

- dispatcher 不再反复把历史快照当作新事件扫描；实时订阅仍按配置的轮询间隔排空并投递到 Sink；
- `flow.snapshot` 仍只用于实时客户端同步，不投递到 JSONL/Webhook；
- Webhook、重试、死信和 Sink 过滤语义保持不变；
- 外部 Sink 应继续使用 `event_id` 去重，并按 `source_id + sequence` 检测缺口。

### 对外 GUI 影响

IPC、HTTP SSE 和 gRPC 的 wire 格式保持 `zero.api.v1` / `zero.event.v1`，现有 GUI 不需要因本次待发布变更修改帧解析。GUI 仍需遵守 `0.0.15-rc` 建立的快照与增量合并规则。

## `0.0.15-rc`

### Flow 订阅改为“基线 + 增量”

包含任一 flow 生命周期事件的 IPC/SSE/CLI 实时订阅，在订阅确认后先收到 `flow.snapshot`：

1. 使用 `payload.records` **替换**当前活动连接集合；
2. 记录快照 `watermark`；
3. 按 `flow_id + revision` 合并后续 `flow.started`、`flow.routed`、`flow.updated`；
4. 收到 `flow.completed` 后从活动集合移除，并由 GUI 自行保存需要展示的历史；
5. 不把 `recent_flows` 当作断线重建或长期历史数据库。

`flow.snapshot` 是同步基线，不进入事件环，也不会投递到 JSONL/Webhook。`flow.completed.payload.record` 是自包含完成事实，新客户端应优先解析 `record`，同时容忍旧内核没有该字段。

### GUI 兼容分支建议

| 内核版本 | GUI 行为 |
|----------|----------|
| `< 0.0.15-rc` | 使用 `active_flows` 查询作为活动连接基线，并兼容旧 flow payload |
| `>= 0.0.15-rc` | 等待 subscribe ACK 和 `flow.snapshot`，之后按 revision 合并增量 |

## 新增条目的要求

后续每个破坏性或语义性变更必须在发布前补充：

- 首个受影响版本；
- 影响的通道和消费者；
- 旧语义与新语义；
- wire 标识是否变化；
- 兼容窗口和可检测条件；
- GUI/SDK/面板的明确迁移步骤；
- 对应回归测试位置。

开发期间只在版本矩阵和 `## Unreleased` 下登记，不预判最终发布版本，也不写入 Cargo 的 `-dev.N` 构建号。完整测试通过后，由 `Prepare Release` 工作流或 `scripts/release.sh` 将矩阵行、章节标题和 workspace 版本一起封板；禁止手工分别修改这些位置。

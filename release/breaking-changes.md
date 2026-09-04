# 控制面兼容性与破坏性变更

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
| `Unreleased` | - | No pending compatibility changes <!-- version-contract:unreleased-row --> |
| `0.0.16-rc.2` | - | No pending compatibility changes |
| `0.0.16-rc.1` | - | No pending compatibility changes |
| `0.0.15` | - | No pending compatibility changes |
| `0.0.15-rc.4` | - | No pending compatibility changes |
| `0.0.15-rc.3` | Diagnostics API 消费者、Connector 运维与事件 Sink | `diagnostics.trace_route` 改为执行真实会话路由追踪；Connector 对事实事件施加有界内存与可选 outbox 背压，采样事件改为仅 best-effort 且不持久化；默认重试次数由 3 调整为 10 |
| `0.0.15-rc.2` | 构建脚本、事件消费者、Webhook 接收端 | 公开 Cargo feature 改用 kebab-case；引擎生成的 `event_id` 增加每次启动唯一的随机 epoch；开发期固定中心 API 被撤销，Connector 收缩为通用 Webhook 事件投递；认证项速率改为 Zero 主体策略聚合 |
| `0.0.15-rc.1` | 进程内 Rust `EventSource`、事件 Sink | Rust 实现者必须迁移到实时 `EventStream`；IPC/HTTP/gRPC GUI wire 无变化 |
| `0.0.15-rc` | GUI flow 生命周期 | 订阅 ACK 后以 `flow.snapshot` 建立活动连接基线，再合并 flow 增量 |

## Unreleased

<!-- Record implemented but unsealed compatibility changes here. -->

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

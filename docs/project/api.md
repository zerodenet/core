# API 能力模型

`zero-api` 的长期目标不是先定义一组 HTTP endpoint，也不是按 HTTP、本地 IPC、FFI、gRPC 拆出一批平行能力模块，而是定义一套稳定的控制、观测和事件导出能力。

HTTP/HTTPS 适合调试和跨语言接入，但不应该成为唯一形态。Zero 需要同时支持本地低开销控制、嵌入式调用、文件落盘、远程回调和兼容适配。这些形态应通过 trait 和 Cargo feature 接入同一个 `zero-api` 能力模型。

## 原则

- API 先定义能力和语义，再定义传输和编码
- 内核概念以 Zero 自有模型为准：`flow`、`outbound`、`policy`、`target`、`route`、`event`
- HTTP、Unix socket、Windows named pipe、file、gRPC、FFI 都只是 trait 实现，不是新的能力边界
- 高频观测优先用增量事件或流式订阅，避免频繁轮询完整状态
- 远程控制和远程事件推送必须显式开启并有认证
- 内核不引入 `panel`、`node`、`user` 这类上层业务概念
- 可选推送能力只处理归一化事件和 sink，不理解事件最终消费者
- Clash、sing-box、Xray 兼容 API 放在 adapter，不进入核心 API 契约

## 分层

`zero-api` 的能力边界按 CQRS 思路组织：

- `Query`
  - 读取快照、状态、统计和能力，不改变内核状态
- `Command`
  - 表达控制意图，可能改变运行时状态或配置状态
- `Event`
  - 描述内核已经发生的事实，用于订阅、审计、计量和外部同步
- `Sink`
  - 把事件投递到外部目标

这里不是完整 Event Sourcing。Zero 内核的真实状态仍来自 `RuntimeConfig`、`EnginePlan`、`EngineState`、运行中的 flow registry 和 runtime task。事件不是内核状态的唯一来源，也不要求通过事件回放恢复正在运行的 socket、UDP association 或 Tokio task。

### 核心类型

核心类型只描述方法、输入、输出、事件、错误和权限，不关心 HTTP path、socket 帧格式或 FFI ABI。

它应该可以被下面几类前端共用：

- CLI
- 本地 UI
- 桌面客户端
- 移动端控制入口
- SDK
- 兼容适配层

### 能力特征

能力 trait 描述可被调用或订阅的内核能力。

建议先围绕这些 trait 收敛：

- `QueryService`
  - 执行只读查询
- `CommandService`
  - 执行控制命令
- `EventSource`
  - 订阅内核产生的归一化事件
- `EventSink`
  - 把事件写到某个外部目标
- `ApiCodec`
  - 负责 JSON、CBOR、MessagePack、postcard 等编码
- `ApiAuth`
  - 负责权限判断和凭据校验

HTTP、file、local、gRPC、FFI 或自定义二进制连接都应实现这些 trait，而不是复制一套 API 概念。

### 适配器实现

适配实现负责把核心能力接到具体环境。

可选实现包括：

- HTTP/HTTPS JSON
- Unix domain socket
- Windows named pipe
- 文件 sink
- Webhook sink
- gRPC sink
- 长连接二进制帧
- in-process Rust 调用
- FFI
- 兼容网关

这些实现应按 Cargo feature 开关接入。依赖重、场景窄的实现可以不进入默认构建，面向 ESP 等嵌入式集成时应能只保留 core types、trait 和必要的 in-process callback。

### 兼容适配器

兼容适配层负责第三方 API 映射。

这层可以提供 Clash 风格路径、sing-box 风格字段或其他生态兼容能力，但不能反向约束 `zero-engine`、`zero-config` 和核心 API 模型。

## Adapter 和 Sink

### Request/Response Adapter

这类 adapter 暴露 query 和 command 能力，例如：

- `capabilities.get`
- `runtime.get`
- `policies.select`
- `flows.close`

它可以跑在 HTTP、Unix socket、Windows named pipe、FFI 或内存调用上。它们的差别是传输和编码，不是能力语义。

### Event Sink

事件 sink 负责把归一化事件推送到外部目标。

支持的目标可以包括：

- `local`
  - 进程内回调或本机 IPC
- `file`
  - JSON Lines、二进制日志或其他可追加格式
- `http` / `https`
  - Webhook POST
- `grpc`
  - gRPC streaming 或 unary ingest
- `custom`
  - 上层宿主自己实现的 Rust trait

sink 不知道最终消费者，也不负责用户业务。它只接收归一化事件，做编码、重试、过滤和投递。

### HTTP/HTTPS JSON

用途：

- 调试
- 通用服务端接入
- 跨语言客户端
- 远程管理

特点：

- 易接入、易排查
- 文本开销较高
- 高频轮询成本较高
- 远程开启时必须有认证和加密

HTTP 适合作为通用入口，但不是高频观测的最佳入口。

### 本地 IPC Adapter

Unix 平台优先使用 Unix domain socket，Windows 平台优先使用 named pipe。

用途：

- 本机 CLI 控制运行中的 Zero
- 本地 GUI 或托盘程序
- 系统服务和用户进程之间通信

特点：

- 不暴露 TCP 端口
- 可复用操作系统权限模型
- 延迟和资源消耗低于 HTTP
- 适合作为默认本地控制入口

### 二进制帧 Adapter

在本地 IPC 或受保护的 TCP/QUIC 连接上，可以使用轻量二进制帧承载核心方法。

用途：

- 高频事件订阅
- 活动 flow 增量更新
- 统计采样推送
- 低开销控制面 SDK

建议形态：

- 固定帧头：协议标识、消息类型、请求 ID、payload 长度
- payload 编码可选：JSON、CBOR、MessagePack、postcard
- 支持 request/response
- 支持 server event stream
- 支持 backpressure 和订阅过滤

这一路径用于降低 CPU、内存分配和序列化成本，不作为人工调试首选入口。

### In-process Rust Adapter

用途：

- 嵌入式场景
- 测试
- 上层产品把 Zero 当库集成

特点：

- 没有传输成本
- 类型最强
- 与 Rust crate 边界更强绑定

这层可以直接围绕 `Engine` / `EngineHandle` 暴露受控方法，但对外命名仍应与核心 API 保持一致。

### FFI Adapter

用途：

- 移动端
- 桌面客户端
- 非 Rust 宿主

特点：

- 低于 HTTP 的调用成本
- ABI 稳定性要求高
- 内存所有权和异步回调需要单独规范

FFI 不应直接暴露内部 Rust 类型，应暴露稳定句柄、字节缓冲和事件回调。

### 远程推送

远程推送不应该引入特定上层服务概念。更合适的模型是：`EventSource` 产出归一化事件，`EventSink` 按配置把事件推送到一个或多个外部地址。

推送地址可以是本地 IPC、文件、HTTP/HTTPS webhook、gRPC 或自定义 sink。远程写控制只适合在认证、加密、权限模型明确后再落地。默认阶段不应开放公网可写控制面。

## 归一化事件

内核事件和传输投递需要分开。

内核只产生核心事件 payload。事件表示已经发生的事实，不是命令，也不是状态存储本身。

事件类型目录：

| Event | 当前状态 | 触发时机 | Payload |
| --- | --- | --- | --- |
| `flow.started` | 已实现 | flow 被内核接受并进入活动表 | `FlowEventPayload`，`traffic` 为 0；存在 `principal_key` 时携带迁移后的 `principal_active_flows`、`session_registry_revision` 和 `observed_at_unix_ms` |
| `flow.routed` | 已实现 | 路由决策和实际出站建立完成 | `FlowEventPayload`，`record.route`、`record.path` 已可用于展示选择链、实际出站和远端 |
| `flow.updated` | 已实现 | 活动 flow 的流量或路由信息发生增量变化 | `FlowEventPayload`，用于低频增量快照，不用于高频逐包上报 |
| `flow.completed` | 已实现 | flow 结束、阻断或失败后生成最终计量结果 | `FlowEventPayload`，包含最终 `traffic`、`timing` 和 `outcome`；存在 `principal_key` 时携带迁移后的 Principal 活跃数和注册表修订号 |
| `flow.snapshot` | 已实现 | flow 订阅成功后发送当前活动连接基线 | `{watermark, session_registry_revision, principal_flows, records}`；只用于订阅同步，不进入历史事件环和外部 sink |
| `policy.selected` | 已实现 | selector / fallback / url_test 等策略组的当前选择发生变化 | `PolicySelectedPayload` |
| `policy.probe.completed` | 已实现 | url_test 或显式探测完成一轮探测 | `PolicyProbeCompletedPayload` |
| `stats.sampled` | 已实现 | 周期性统计采样或按需统计采样 | 统计快照 payload，字段来源于 runtime/stats 快照 |
| `config.changed` | 已实现 | 运行时配置经过校验并完成 staged apply | 配置变更摘要，不包含密钥和完整敏感配置 |
| `engine.started` | 已实现 | 引擎完成启动并进入监听/运行状态 | 引擎构建标识、启动时间、启用能力和监听摘要 |
| `engine.stopped` | 已实现 | 引擎完成关闭流程 | 关闭原因、运行时长和最终统计摘要 |
| `engine.warning` | 已实现 | 内核或 adapter 产生需要上层关注的非致命告警 | `WarningPayload` |

所有 flow 生命周期事件保留原有顶层兼容字段，并在 `payload.record` 携带统一的 `FlowRecord`。记录包含 `revision`、状态、入口、认证归因、来源地址和进程、原始目标和解析地址、路由模式/命中规则/选择链、实际出站/远端/中继链、四方向流量、速率、时间以及结构化失败。汇总的 `traffic.bytes_up` / `bytes_down` 按用户方向计数，同一个中继字节只计一次；四个边界计数继续用于传输诊断。`flow.completed` 必须自包含，消费者不需要再回查内核才能解释一条已关闭连接。

内核只保存运行中 flow 的权威状态，并保留既有的有限诊断窗口；它不承担 GUI 历史数据库职责。GUI、面板和其他消费者收到 `flow.completed` 后自行维护、索引或持久化连接记录。需要长期可靠留存时配置 JSONL 或 webhook sink，而不是扩展内核历史查询。

活动连接订阅以 `flow.snapshot` 建立基线，随后按 `revision` 合并 `flow.started`、`flow.routed`、`flow.updated` 和 `flow.completed`。`flow.updated` 是可丢弃的速率/流量样本；`flow.completed` 是最终事实。事件 envelope 的 `sequence` 用于发现传输缺口，断线后重新订阅会获得新的活动快照。

Principal 活跃连接观测由 `SessionRegistry` 维护，其计数索引与活动会话表在同一临界区内变更，不存在第二套统计真相源。`flow.started` 和 `flow.completed` 中的 `principal_active_flows` 是状态迁移后的绝对值，不是加减量；`session_registry_revision` 是同一内核实例内的 Principal 状态顺序，`observed_at_unix_ms` 是该迁移发生的时间。并发投递时事件 envelope 的 `sequence` 仍只表示投递/回放顺序，消费者必须按 `session_registry_revision` 合并 Principal 当前状态。

`principal_flows` 查询返回 `{core_instance_id, session_registry_revision, principals}` 的完整替换快照。`principals` 只列出当前活跃数大于零的 Principal；快照中缺失的 Principal 在该修订号下视为零。消费者发现事件缺口时用该快照修复“当前值”，历史窗口是否完整仍由消费者自行判断，内核不提供可重置峰值或面板业务窗口。

`zero-api` 负责把它们包进稳定 envelope：

```text
event {
  schema_id,
  event_id,
  event_type,
  occurred_at_unix_ms,
  source_id,
  sequence,
  principal_key,
  labels,
  payload
}
```

字段语义：

- `schema_id`
  - 事件结构标识
- `event_id`
  - 事件唯一标识，用于去重
- `event_type`
  - 例如 `flow.completed`
- `source_id`
  - 由宿主或 adapter 注入的来源标识，不是内核的 `node` 概念
- `sequence`
  - 来源内单调递增序号，可用于断点续传和 ack
- `principal_key`
  - 对内核不透明的归因 key
- `labels`
  - 上层可附加的低基数字段
- `payload`
  - 具体事件内容

`source_id`、`sequence`、重试、ack 和缓冲属于 `zero-api` adapter/sink 层；`zero-engine` 不需要知道事件最终会被写入文件、推送到 webhook，还是被外部服务消费。

事件可以用于外部系统构建自己的投影，例如按 `principal_key` 统计流量消耗。但这些外部投影不反向成为 `zero-engine` 的状态来源。

当前已实现的归因来源是入站访问配置。`protocol.users[*].principal_key` 会进入事件顶层和 `payload.auth`；协议 UUID 或密码只作为认证秘密使用，不会默认写入事件。

## Webhook 和外部回调

外部回调应建模为 sink 配置，不建模为某个特定上层服务连接。

概念形态：

```json
{
  "event_sinks": [
    {
      "tag": "billing-webhook",
      "type": "http",
      "url": "https://collector.example.com/zero/events",
      "events": ["flow.completed", "stats.sampled"],
      "encoding": "json"
    },
    {
      "tag": "local-audit",
      "type": "file",
      "path": "zero-events.jsonl",
      "events": ["flow.completed"],
      "encoding": "json-lines"
    }
  ]
}
```

`zero-api` 的 `webhook` feature 提供 `WebhookEventSink`，用于把归一化事件以 JSON POST 到 HTTP/HTTPS 地址；运行时配置和多 sink 编排由 connector/sink 层承载。

sink 的共同要求：

- 只消费归一化事件 envelope
- 支持事件过滤
- 支持失败重试或明确声明不重试
- 能表达投递成功、投递失败和丢弃
- 不把业务用户、套餐、余额、外部节点管理等概念写入核心模型

如果未来需要更复杂的连接生命周期，可以把这层实现称为 connector，但语义仍然是 event exporter/sink，不是内核里的远程客户端。

## 首批核心能力

首批能力应该覆盖当前已经实现并能长期复用的模型。

### 查询

- `capabilities.get`
  - 返回协议标识、支持的 adapter、sink、feature、只读/可写权限
- `health.get`
  - 返回进程存活、启动时间、构建标识和基础状态
- `config.get`
  - 返回当前有效配置视图
- `runtime.get`
  - 返回运行时统计、活动 flow、最近完成 flow 和 policy 状态
- `stats.get`
  - 返回轻量统计，不包含完整 flow 列表
- `principal_flows.get`
  - 返回当前内核实例的权威 Principal 活跃 flow 完整快照；HTTP 路径为 `GET /api/v1/principal_flows`
- `flows.list_active`
  - 返回活动 flow 快照
- `flows.list_recent`
  - 返回最近完成 flow
- `flows.get`
  - 按 flow id 查询单个 flow
- `policies.list`
  - 返回所有 policy 状态
- `policies.get`
  - 查询单个 policy

HTTP adapter 将这些能力暴露在 `/api/v1/*` 命名空间下，IPC 和 CLI 共享同一组 query 语义。

### 命令

- `config.validate`
  - 已实现。校验一份配置输入，不改变运行中状态；虽然不产生状态变更，但它表达的是提交前的写入意图，放在 command 侧更清晰
- `flows.close`
  - 已实现。主动关闭某个活动 flow
- `policies.select`
  - 已实现。切换 `selector` 当前成员
- `policies.probe`
  - 已实现。触发 `url_test` 立即探测
- `diagnostics.probe_target`
  - 已实现。对指定出站做 TCP 可达性探测
- `diagnostics.probe_outbound`
  - 已实现。经指定出站执行中性的 HTTP 延迟探测，返回稳定错误码和实际超时上限，不修改 `url_test` 策略状态
- `diagnostics.dns_lookup`
  - 已实现。解析域名
- `diagnostics.trace_route`
  - 已实现。按真实会话路由流程查看规则匹配结果；域名未命中且规则需要目标 IP 时会解析 DNS 后复算

热重载已实现，覆盖路由规则、出站组/出站、运行模式、入站 listener、声明式 `runtime.tun`、DNS 配置、flow hooks、EventDispatcher 和原生 connector。`runtime.tun.dual_stack` 默认启用 IPv4/IPv6 双栈接管，并在同一设备配置两族地址；`secondary_addr` 可覆盖自动选择的另一族 TUN-local CIDR。命令式 `tun.start` 提供相同的 `dual_stack` 与 `secondary_addr` 选项。`config.apply` 是持久化部署边界；`config.apply_runtime` 是不写回源文件的运行时覆盖边界。两者通过可等待确认的公共命令服务依次完成 proxy reconciliation 与 application service reconciliation；TUN 设备或系统路由安装失败同样会恢复上一份 TUN 和 last-known-good 配置。`api.control` 的监听地址和鉴权不能在承载当前命令时自替换，变更会被拒绝并要求显式重启。

`policies.select` 通过 HTTP `POST /api/v1/commands` 调用，请求体示例：

```json
{
  "method": "policies.select",
  "params": {
    "policy_tag": "proxy",
    "target_tag": "direct"
  }
}
```

### 事件

- `events.subscribe`
  - 订阅运行时事件
- `events.export`
  - 把事件写入一个或多个配置的 sink

事件类型使用上文“归一化事件”里的事件目录。配置里不应该出现目录外的事件类型；如果需要实验事件，应使用显式 feature 或实验命名空间，避免面板误把它当作稳定契约。

事件订阅和导出应该支持过滤条件，例如只订阅 flow、只订阅某个 inbound、只订阅 policy 变化。

### 诊断

- `diagnostics.probe_target`
  - 已实现。对指定出站做 TCP 可达性探测
- `diagnostics.probe_outbound`
  - 已实现。经指定出站执行中性的 HTTP 延迟探测；不触发 `url_test` 选路或策略事件
- `diagnostics.dns_lookup`
  - 已实现。解析域名
- `diagnostics.trace_route`
  - 已实现。查看路由规则匹配结果

## 鉴权

内核采用简单的 Bearer Token 鉴权，不做细粒度权限隔离：

- 未配置 token（本地 `127.0.0.1` 监听）：所有端点无鉴权
- 已配置 token：请求须携带 `Authorization: Bearer <token>`，通过后全部端点可用

IPC 本地 socket 依赖文件权限（`0o600`），连接即视为已鉴权。

权限隔离应在上层面板/网关实现。内核只负责"认证通过与否"的二元判断。

## 错误模型

核心 API 错误应稳定，不能直接泄露内部 Rust 错误类型。

建议基础错误码：

- `not_found`
- `invalid_argument`
- `permission_denied`
- `feature_disabled`
- `conflict`
- `unsupported`
- `internal`

错误响应应包含：

- 稳定错误码
- 人类可读消息
- 可选字段路径
- 可选底层原因

## 协议标识和能力发现

控制面暴露协议标识和能力列表，供消费者确认当前可用表面。

当前暴露：

- `api_id`
- `schema_id`
- `engine_build_id`
- `capabilities`
- `experimental_features`

`api_id` 和 `schema_id` 是 wire identifier，不承载发布历史。外部消费者以 `capabilities` 判断当前构建是否启用某项能力。

## 初始落地顺序

1. 在 `zero-api` 中定义 query、command、event、sink、错误和权限模型
2. 把现有 `export_status` / `export_runtime` / `export_config` 映射为核心能力
3. 把 `selector` 切换映射为 `policies.select`
4. 定义 `QueryService`、`CommandService`、`EventSource`、`EventSink`、`ApiCodec` 等 trait
5. 增加 in-process adapter，先服务测试、嵌入式宿主和内部调用
6. 增加 HTTP JSON adapter，服务调试和通用接入
7. 增加事件 sink，先支持 file 和 HTTP/HTTPS webhook
8. 再按 feature 评估 IPC、gRPC、FFI、二进制帧和第三方网关

## 当前不做

- 不把外部生态路径固化为内核 API
- 不直接复制第三方控制面字段
- 不按 HTTP、IPC、FFI 等传输形态拆散 API 能力
- 不把 `panel`、`node`、`user` 写进内核 API
- 不在首批做完整热重载
- 不把事件流设计成内核状态的唯一来源
- 不默认开放远程写控制面
- 不让高频 flow 更新依赖 HTTP 轮询

# Connector 实现审查报告

> 初始审查日期：2026-07-24
>
> 边界修订日期：2026-07-26
>
> 本轮按能力提交与最终验证见 [Connector 能力提交分析报告](./connector-capability-commit-report-2026-07-26.md)。
>
> 结论基于当前源码，不以旧设计文档或测试名称代替实现事实。

## 结论

Connector 已从“内核规定中心工作流”收缩为通用 Webhook 事件投递。中心通过现有 Zero HTTP/gRPC API 执行 `config.apply` 注册 `api.event_sinks`；Zero 使用注册方给出的完整 URL，只定义 `zero.event.v1` 推送 envelope 和 HTTP 确认分类。

当前实现不再包含中心节点注册、同步、traffic、presence、访问配置、用户模型、inbound 管理或第三方面板适配。此前将 Connector 设计成中心服务端协议的方向已撤销，未保留未发布合同的兼容层。

## 当前能力划分

| 能力 | 当前实现 | 归属 |
|------|----------|------|
| Webhook 注册 | HTTP `POST /api/v1/commands` 或 gRPC `Control.Execute` 承载 `config.apply` | Zero API + application |
| 多地址/多能力分流 | `api.event_sinks` 可注册多个通道；每个通道以 `events` 过滤事件类型 | `zero-config` + `zero-connector` |
| URL | `EventSinkConfig::Webhook.url` 是完整地址，投递端不拼接路径 | 外部注册方 |
| 注册身份 | `tag` 仅是节点本地 sink/outbox 键；URL 不绑定节点、inbound 或代理协议 | `zero-config` + `zero-connector` |
| 来源标识 | 可选 `source_id` 只写入事件 envelope，不参与投递路由 | 外部注册方 |
| 请求 headers | 注册方提供不透明 `headers`；Zero 不强制 Bearer | 外部注册方 |
| 推送格式 | 单个 `zero.event.v1` JSON envelope | `zero-api` |
| 确认 | `2xx` 成功；`429`/`5xx`/网络错误可重试；其他状态不可重试；忽略正文 | `zero-connector` |
| 故障隔离 | 每个 sink 独立 worker；一个接收端阻塞不影响其他注册地址 | `zero-connector` |
| 可靠交付 | 可选 outbox；timeout、重试、退避与耗尽策略可配置；默认持续重试；按文件系统实时空间保留 1 GiB/5% 中较大水位，低水位时 fail-closed，ACK/压缩可使用保留空间但受紧急维护水位约束；提供 dead letter 与投递/磁盘状态 | `zero-connector` |
| 热应用 | `config.apply` 重建 EventDispatcher，无需重启节点 | application/proxy |
| 中心管理节点 | Query、Command、`config.apply` | Zero HTTP/IPC/gRPC |
| gRPC 安全 | 可选 Bearer、原生 TLS/mTLS 及外部 TLS 终止；非 loopback 明文需显式开启，远程无 Bearer 时必须使用 mTLS | `zero-grpc` + application |
| 证书等材料投递 | 已形成通用配置事务设计，尚未实现 DTO、staging 与回滚 | application（规划） |

## 已撤销的固化设计

以下实现和产物已删除：

- 顶层 `push` / `PushConfig`；
- `ConnectorPeer` 及 register/sync/traffic/presence 方法；
- `/api/v1/nodes/{node_id}/register|sync|traffic|presence` 路径拼接；
- 注册成功后才允许上报的中心工作流；
- 专用 traffic journal、presence reporter 和节点 observation；
- `zero.connector.v1` 中心 OpenAPI；
- `zero connector contract`、conformance、production-gate CLI；
- 中心合同测试、资格脚本和生产门禁模板；
- Webhook 固定 `Authorization: Bearer` 认证约定。

这些内容不是“边缘化”或暂时隐藏，而是从内核实现和合同中撤销。项目尚未发布对应合同，因此不保留迁移 DTO、旧字段或版本兼容。

## 仍然存在的 Connector 代码

| 文件/目录 | 作用 | 是否新增 |
|-----------|------|----------|
| `crates/connector/src/dispatcher.rs` 与 `dispatcher/{outbox,worker}.rs` | 事件订阅、过滤、重试、持久 outbox 调度与每 sink worker | 修改/新增 |
| `crates/connector/src/registry.rs` | 从通用 `EventSinkConfig` 构建 JSONL/Webhook sink | 修改 |
| `crates/connector/src/webhook.rs` | 完整 URL 的 HTTP POST 与状态分类 | 新增 |
| `crates/connector/src/state.rs` | outbox/dead-letter 本地状态检查与文件锁 | 修改 |
| `crates/config/src/model/api.rs` | `api.event_sinks`、dispatcher、outbox/dead letter | 修改 |
| `src/application/services.rs` | config.apply 后热重建 EventDispatcher | 新增 |
| `crates/grpc/src/lib.rs` 与 `security.rs` | gRPC 服务编排，以及可组合 Bearer、原生 TLS/mTLS 与远程明文门禁 | 修改/新增 |
| `docs/project/managed-materials.md` | 通用证书/私钥等材料的原子配置事务设计 | 新增（设计） |

`zero-api` 不再依赖 HTTP 客户端。它只保留事件 envelope、EventSink trait 和 `PublishResult`；具体 Webhook 传输位于 `zero-connector`。

## 工程规则检查

已修正的规则问题：

- Connector 不再定义另一套中心 API；
- 外部系统适配 Zero，而不是 Zero 适配外部面板；
- Webhook 注册只按地址和事件类型过滤，不按节点、inbound 或代理协议绑定；
- 控制面仍统一使用 `CommandRequest::ConfigApply`；
- Webhook 具体 HTTP 实现不位于独立合同 crate `zero-api`；
- 测试继续位于 sibling `tests/` 目录；
- `connector` feature 只依赖 `event-dispatcher`，不隐式启用 `status-api` 或 `grpc-api`；
- Dispatcher 的 sink 网络调用已从串行协调线程移至每 sink 独立 worker；
- 固定 Webhook timeout、固定退避和隐式重试耗尽行为已改为 `api.dispatcher` 显式配置；
- outbox 已增加文件系统实时空闲水位，达到水位后暂停新 PUT、保留未推进事件游标；ACK 与压缩可使用正常保留空间排空，但仍受紧急维护水位约束，状态通过 `outbox_storage` 暴露；
- gRPC 已补可选原生 TLS/mTLS、独立 Bearer 开关和非 loopback 明文显式门禁；仅使用 HTTP 的配置不被未启用的 gRPC 策略误伤。

仍需持续关注：

- `dispatcher.rs` 体积较大，应继续把 worker、调度和结果处理拆到 sibling 模块，但不得借拆分恢复中心业务模型；
- `config.apply` 是完整配置事务，中心必须基于最新配置生成候选值，避免用陈旧副本覆盖无关本地变更；
- `headers` 中可能包含敏感值，部署方应保护配置文件和控制 API；后续若增加 secret provider，应保持通用，不得恢复固定认证方案；
- 通用受管材料目前只有事务设计，证书/私钥投递尚不能作为已实现能力宣传；
- 本地 outage/soak 可证明故障处理与资源边界，但仍不等于真实接收端和真实部署环境的生产验收。
- 磁盘保护避免 outbox 写满节点，但长期低水位仍可能使尚未持久化的事件超过 engine event log 保留窗口；此时会显式记录 replay gap，需要外部业务账本对账，不能宣称无限零丢失。

## 当前完善度

从机场或其他中心系统对接角度，Zero 已提供基础能力：

- 中心可通过 HTTP/gRPC 管理配置；
- 节点可把流量完成、告警、统计等统一事件主动推送到任意接收端；
- 单节点可注册多个接收端并按事件类型分流；多个节点也可复用同一地址；
- 事件可过滤，并有至少一次可靠投递基础；
- 单个接收端超时不会阻塞其他 sink，投递策略可以按节点部署配置。

它不是机场业务系统，也不提供用户、套餐、计费或面板工作流。限流、停用、升级和通知由外部中心决策；需要改变内核运行状态时通过现有 Zero API/gRPC 调用通用方法或应用配置，程序升级由部署系统执行。Connector 只负责事件转换、过滤、可靠投递与投递状态。这一边界是目标本身，不是待补齐的“面板适配缺失”。

## 2026-07-25 本地资格结果

以下结果来自本机 ignored qualification 测试，不是外部生产环境证明：

| 场景 | 参数 | 结果 |
|------|------|------|
| 接收端 outage | 1,000 events；先返回 `503`，再恢复；持久 outbox | 通过；1,000 全部收敛；失败尝试 1；重启无重复；17.982 秒 |
| restart soak | 5,000 events；5 次 dispatcher 重启；至少 30 秒 | 通过；无丢失、无重复；79.677 秒；62.8 events/s；峰值 RSS 8,880,128 bytes |
| sink 故障隔离 | 一个 Webhook 持有响应，另一个同时接收同一事件 | 通过；健康 sink 在慢 sink 释放前完成 |

soak 的 62.8 events/s 是当前 Windows 本地同步 outbox 基线，不是容量承诺。它说明 10–30 节点的初期规模可以继续联调，也说明逐 delivery `sync_data` 的吞吐成本必须保留为后续优化和更长时间验收项。

同一源码状态还通过了 `cargo test --workspace`、Connector/Config/gRPC 专项测试、Connector 与 gRPC 的 `clippy -D warnings`、`connector`/`grpc-api` 单 feature 编译，以及 89 个 Markdown 文件的文档检查。外部 Xray/sing-box 等 ignored 互操作测试未因本轮 Connector 工作强制执行；它们验证代理线协议，不是 Connector 接收端合同。

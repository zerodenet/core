# Connector 通信边界

## 定位

Connector 是面向面板和外部控制系统的可选节点侧事件投递模块。它解决的是 HTTP/IPC/gRPC 控制 API 主要由外部控制端主动访问节点，而节点缺少可靠事件回传通道的问题。面板接入是其明确用途；实现保持控制端中立，负责节点内的事件转换与可靠投递循环，不承担面板的业务模型，也不引入第二个部署进程。

外部控制端通过现有 Zero API 的 `config.apply` 注册 `api.event_sinks`。Connector 随后把 `EventSource` 中的事件投递给接收端。注册、管理和推送没有第二套协议：

```text
外部控制端
  └─ Zero HTTP/gRPC: config.apply(api.event_sinks)
       └─ application 热重建 EventDispatcher
            └─ Connector POST 完整 Webhook URL
                 └─ 接收端以 HTTP 状态确认
```

## 所有权

| 层 | 拥有 |
|----|------|
| `zero-api` | `zero.event.v1` envelope、事件类型、`PublishResult` |
| `zero-config` | `api.event_sinks`、outbox/dead-letter 配置及验证 |
| application/proxy | `config.apply` 事务、持久化、运行时重建和回滚 |
| `zero-connector` | Webhook HTTP 投递、ACK 分类、过滤、每 sink 独立 worker、重试、outbox、dead letter、sink 状态 |
| 外部控制端 | 完整 URL、headers、认证方案、接收端业务、事件消费和管理策略 |

## 注册维度

每个 `api.event_sinks` 元素表示一个独立投递通道：

- `tag` 是节点本地唯一的通道身份，也是 sink 状态和 outbox 的投递键；
- `url` 是完整接收地址，可由多个注册或多个节点复用；
- `events` 是事件类型过滤器，用于按能力分流；空集合表示全部事件；
- `source_id` 只是可选生产者元数据，不参与投递选择；
- 节点、inbound、凭据和 VMess/VLESS/Trojan 等代理协议都不是注册轴。

因此实际关系是多对多：一个节点可以投递到多个地址，一个地址可以接收多个节点；同一节点也可按事件类型把流量统计、运行告警等送往不同接收端。

## 执行边界

外部系统消费事件并决定是否限流、停用、升级或通知。需要改变内核运行状态时，外部系统调用既有 Zero HTTP/IPC/gRPC Query、Command 或 `config.apply`；程序升级由外部部署系统执行。Connector 不反向解析面板命令，也不把业务决定转换成内核私有指令。

Connector 所维护的“保活”仅指投递任务持续运行、失败重试、outbox 恢复和 sink 可观测状态。它不定义中心连接会话或固定 heartbeat/presence 端点。

Dispatcher actor 在 Connector 自己的专用执行线程上独占事件游标、outbox 和重试状态，并通过事件到达、投递完成、重试截止时间或关闭信号唤醒，不以在途请求作为轮询条件。每个 sink 使用独立、单并发的异步投递任务；Webhook 使用可取消的异步 HTTP，JSONL 等同步 sink 通过 blocking adapter 执行。一个 URL 超时不会阻塞其他注册地址，sink 任务也不能直接修改 outbox。请求超时、退避、重试阈值和耗尽策略全部来自 `api.dispatcher`，不存在隐藏的固定三次删除策略。

`sinks.get` 的每个 `SinkStatus` 保留兼容的总 `pending` 计数，并通过可选 `delivery` 对象公开调度器当前生命周期：`in_flight`、`retry_pending`、`ack_retry_pending`、`durable_pending` 和 `next_retry_at_unix_ms`。这些状态彼此独立；例如一个 sink 可以同时有一条在途请求和多条持久 backlog。磁盘保留空间阻塞继续由 `outbox_storage.write_blocked` 表达，历史恢复和事件缺口分别由 `outbox_recovery` 与 `replay_gaps` 表达。旧内核没有 `delivery` 时，消费者必须按未知状态处理，不能根据 `pending` 猜测是在途还是重试。

`zero-api::EventSink` 保持同步、运行时中立，用于内核内的持久化钩子和通用文件 sink；Connector 在自身边界内把它适配到异步投递能力，不把 Tokio 或 HTTP 类型引入 Engine。配置重载或关闭时，已经写入 outbox 的在途 Webhook 可以取消并由下一次 Dispatcher 启动恢复；没有 outbox 的事实投递仍执行 graceful flush，不能以取消为由静默丢弃。

Webhook 的 TCP 建连通过 Connector 定义的窄网络边界执行，Zero application 注入与 Proxy、DNS 共用的物理出口权威。每次新连接都会按解析候选的地址族读取最新出口 generation；TUN 活跃但对应地址族没有可信物理出口时必须失败关闭，不能退回默认 socket 重新进入 TUN。独立嵌入 Connector 且不运行 Zero TUN 时可以显式选择系统网络。HTTPS 默认验证公开根证书信任集；`allow_insecure` 只作为显式关闭证书验证的部署选项。

应用进程第一次启动已配置的 delivery sink 时，先建立实时订阅，再从内核保留事件中补取该 engine 实例的 `engine.started`。实时流与补取结果按事件 `sequence` 去重，因此启动时序不会造成漏投或重复投递。配置热更新重建 dispatcher 时不会再次补发同一进程的启动事件；`flow.snapshot` 的订阅同步和常规游标回放语义保持不变。

配置热更新按应用组件的实际配置差异进行重建。仅修改入口、路由或其他代理设置时保留现有 dispatcher、状态监视器和 flow hook；hook 单独变化也不重建 dispatcher。`config.apply` 的成功确认仍须等待需要变化的组件完成协调。

dispatcher 关闭时取消已有 outbox 副本的在途投递，保留持久化事实用于恢复；未持久化的生命周期事实仍须完成投递。`flow.updated` 和 `stats.sampled` 属于可丢弃的临时采样，关闭时丢弃待发采样；有 outbox 恢复保障时也取消在途采样，无 outbox 时保留 worker 以完成后续事实投递，不能将待发采样留给已经停止的 worker。持久化受阻时释放源线程的有界发送等待，明确记录未持久化事件，且不越过该事件推进游标。

## 持久化扫描与压缩

持久化预检和 outbox 启动回放使用固定缓冲区流式扫描，不把整个日志或事件 payload 载入内存；仅保留待投递事件的键及其最新记录偏移，内存随真实待投递键数增长。预检按已打开文件的初始长度读取，允许运行中的 dispatcher 继续追加或原子压缩，不申请写锁、不修复文件、不改变内核运行状态。dead-letter 和 quota 预检也通过流式读取解析，quota 仍保留校验所需的余额结构。

跳过 payload 存储不等于跳过校验。outbox/dead-letter 的预检保持实际 JSON Value 加载的数值范围、Unicode 转义和嵌套深度边界；不可加载的完整记录必须报告 incompatible，不能以 ready 放行。普通记录在有界缓冲中校验；大记录在同一已打开文件快照内流式复核，额外读取不随 payload 大小分配内存。ready 仅确认该快照的格式兼容，不证明接收端可达、后续磁盘空间或业务处理成功。

outbox 在 PUT、ACK 和启动时检查历史压缩阈值，持续重试无需等待成功 ACK。压缩逐条流式复制每个待投递键的最新完整原始记录，保留 payload、attempts 和 message；完成临时文件同步后原子替换，不重启 dispatcher 或节点。磁盘空间不足时延迟压缩，原日志仍有效。压缩仅移除已确认和被更新覆盖的历史记录，不能缩减真正未投递事件的数量；接收端故障造成的真实 backlog 仍需恢复投递。末尾不完整记录先流式保存完整证据，再修复工作日志；预检只报告该状态。

## 面板接收与交付契约

- Connector 向注册的完整 URL POST `zero.event.v1`，使用提供的不透明 headers。它不规定面板接收路径、登录/注册流程或账户模型。
- HTTP 2xx 表示接收端确认接收；Connector 不解释响应 body，不从响应中接收配置或控制命令。接收端应在达到自身所承诺的可靠接收边界后返回成功状态；需可靠消费时先持久化或提交可恢复队列，再确认。
- HTTP 429、5xx 以及超时/网络错误按配置重试；其他非 2xx 状态按非重试结果处理。耗尽后的 retry_forever/dead_letter/discard 语义由 `api.dispatcher` 决定；不能把终止或死信误报为接收端已处理。
- 接收端已接收、但响应丢失或本地 ACK 尚未落盘时，事件可能重投。面板按稳定的来源和事件身份幂等消费，不能假定 exactly-once。重试可能改变到达顺序，消费者使用 envelope 的实例、sequence 和发生时间解释事实。
- outbox 为已成功持久化的未确认投递提供恢复依据；未配置 outbox、存储受阻的尚未持久化事件、有限事件回放缺口及显式终止策略不具备同等保障。相关阻塞、恢复、死信和缺口状态须按事实展示。
- `flow.updated`、`stats.sampled` 是临时采样，可以合并或丢弃，不进入可靠事实积压。曲线消费者在断线、缺口或统计周期变化后通过 Zero Query 重建基线；完整累计计量仍由内核负责。
- 面板改变配置、权限、端点或运行状态时使用既有 Zero Query/Command/config.apply；部署升级留在面板/部署系统。当前事件回传不提供节点主动建立的双向管理会话，后续如需该能力，应单独评估通用控制传输。

## 禁止边界

Zero 不得在 Connector 中：

- 拼接或规定中心 URL；
- 定义节点注册、周期同步、presence 或 traffic 专用端点；
- 定义中心资源模型、面板 DTO 或第三方适配接口；
- 解释用户、套餐、计费、凭据、inbound 或协议管理语义；
- 按节点、inbound 或代理协议绑定 Webhook 地址；
- 解释限流、停用、升级、通知等外部业务决策；
- 建立私有配置、命令或 ACK 协议替代 `zero-api`；
- 因某个参考面板的工作流改变内核合同。

外部系统适配 Zero HTTP/gRPC 和 `zero.event.v1`。不愿适配的系统使用独立兼容桥，兼容代码不进入内核。

## Feature

- `event-dispatcher`：通用事件分发和状态；
- `sink-jsonl`：本地 JSONL sink；
- `connector`：Webhook sink，并依赖 `event-dispatcher`；
- `grpc-api`：可选 gRPC 控制入口。

`connector` 不隐式启用 `status-api` 或 `grpc-api`。节点仍是一个进程，Feature 只控制编译裁剪，不引入第二个部署程序。

## 生产资格验证

普通 Linux 回归在子进程内设置 192 MiB 地址空间限制，覆盖 512 MiB 历史日志、单条 256 MiB 待投递记录和 350,000 个真实待投递键，验证只读预检、启动回放、尾部证据保存及压缩保留事件。配额回归以 256 MiB 输入验证预检和启动读取。资源限制仅作用于一次性测试进程，不作用于已运行的节点。

`.github/workflows/connector-qualification.yml` 在 Connector、API 或相关配置边界变化时运行两项 Linux 门禁：接收端离线后恢复并完整清空 10,000 条持久 backlog；25,000 条事件跨 5 次 Dispatcher 重启并持续至少 60 秒，验证没有丢失或已 ACK 事件重投。普通测试中的 stalled-webhook 用例使用可计数事件源约束调度器唤醒/回放次数，避免用 CI 主机瞬时 CPU 百分比作为唯一判据。事件规模按同步持久化路径的实测吞吐设置，确保每次相关 PR 都能执行而不是依赖偶发的超长人工任务。

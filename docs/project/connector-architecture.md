# Connector 通信边界

## 定位

Connector 是 Zero 的可选出站事件通信能力。它解决的是 HTTP/IPC/gRPC 控制 API 主要由外部控制端主动访问节点，而节点缺少可靠事件回传通道的问题。它是节点内的事件转换与可靠投递循环，不是面板客户端，也不是第二个部署进程。

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

Dispatcher 线程独占事件游标、outbox 和重试状态；每个 sink 使用独立阻塞 worker，仅执行该 sink 的一次投递。一个 URL 超时不会阻塞其他注册地址，worker 也不能直接修改 outbox。请求超时、退避、重试阈值和耗尽策略全部来自 `api.dispatcher`，不存在隐藏的固定三次删除策略。

应用进程第一次启动已配置的 delivery sink 时，先建立实时订阅，再从内核保留事件中补取该 engine 实例的 `engine.started`。实时流与补取结果按事件 `sequence` 去重，因此启动时序不会造成漏投或重复投递。配置热更新重建 dispatcher 时不会再次补发同一进程的启动事件；`flow.snapshot` 的订阅同步和常规游标回放语义保持不变。

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

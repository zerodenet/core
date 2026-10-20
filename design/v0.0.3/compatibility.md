# 外部与嵌入兼容边界

必要的公开 Rust 接口迁移不放宽 GUI、面板的 V1 行为和现有 FFI ABI。历史消费者观察未包含未提交工作区内容，真实组合验证尚待 PR01/PR10。

[方案入口](README.md) · [架构](architecture.md) · [契约](contracts.md) · [迁移与任务卡](migration.md) · [兼容](compatibility.md) · [验证](validation.md) · [来源](references.md)

<!-- source-section: 16 -->
# 客户端面板与嵌入兼容边界

目标是保持现有客户端与面板的接入方式。内部 Rust 接口可以迁移，但外部兼容必须以实际消费者验证，不能预先宣称零影响。

| 表面 | 保持的契约 | 允许的内部调整 |
| --- | --- | --- |
| V1 配置 | 字段、默认值、协议名称、路径与校验语义 | 内部 Options、构建结果与资源归属 |
| CLI | run、validate、reload、status、build-info 和已使用参数、退出语义 | 命令调用新宿主 |
| IPC HTTP gRPC | 方法、信封、错误码、认证、权限与响应语义 | 服务实现及 DTO 投影位置 |
| 能力发现 | api_id、schema_id、build_features 与协议能力的含义 | 内部能力来源和缓存 |
| 事件与 Webhook | zero.event.v1、身份、顺序边界、ACK、重放与投递义务 | 内部事件源和 dispatcher 连接方式 |
| TUN | 开关参数、就绪确认、父进程退出、宿主资源拥有关系与恢复 | 设备实现与管理位置 |
| Rust 公共 API | 下游依赖和迁移路径清楚；不承诺全部源码兼容 | 允许必要调整公开 trait、构造函数和配置结构，提供接口迁移说明 |
| FFI | 导出符号、调用约定、句柄、字符串释放与当前行为 | 句柄内部实现；不隐式扩大功能 |

## 客户端 GUI

当前 GUI 通过 `src-tauri/src/kernel/zero` 的 IPC 适配器与独立内核进程交互，解析事件与配置；它还依赖固定 revision 的 zero-rule。内部 proxy trait 改动不直接要求 GUI 适配，但 zero-rule 公共 API 改动属于实际源码兼容面。

必须保留启动参数、控制 socket、父进程生命期、管理待命、TUN 启停与网络恢复。stats reset、订阅 ACK、flow.snapshot 与后续增量的边界必须和客户端解析规则一致。内核更新不得以旧版本解析成功代替真实组合测试。

## ZBoard 面板

当前发布链路读取 build-info，校验配置，执行 reload，再检查 core_instance_id 与 revision，记录配置发布结果；失败不应偷偷变成 systemctl restart。

配置 renderer 和事件接收器仍按既有 V1 配置及 zero.event.v1 工作。重复发布不重启健康内核，无变化配置不破坏现有连接。reload 成功不能早于已承诺的应用与协调完成，失败恢复不伪装成成功。

保留流量方向、单位、累计值、重置和结算语义，以及 source_id、event_id、核心实例身份和幂等处理。面板凭证和节点模型留在面板，不进入组件标准。

## Rust 和 FFI

允许必要调整 Proxy、ProxyHandle、RunningProxy 等公开 Rust 接口，不以长期兼容包装阻碍标准收敛。保留有职责的入口，逐项记录公开 trait、构造函数、配置结构及 feature 迁移和实际下游编译影响；必要过渡 facade 设定删除边界。此决定不放宽 GUI、面板的 V1 wire 行为及现有 FFI ABI 兼容要求。

FFI 目前直接使用 EngineHandle。其旧功能边界与错误处理需要先建立测试，不能借宿主统一让 zero_start 自动承担从前没有的网络副作用。

## 兼容版本规则

当前标识为 zero.api.v1、zero.event.v1，配置与控制数值版本为 1。内部重构不自动改变这些标识；发生不兼容变更时也不能为了保留标识而隐瞒变化。

新增可选字段需验证旧消费者忽略行为；修改 ACK、快照、重放、计量或恢复语义即使 JSON 形状相同，仍属于需要登记与评估的外部变更。相关现行规则见 [兼容性账本](../../release/breaking-changes.md#L39)。
<!-- /source-section: 16 -->

## TaskScope build 阶段登记迁移

公共 Rust `SpawnError` 新增 `NotStarted`：宿主以 `TaskScope::suspended(runtime, declared_limit)` 创建实际组件作用域，工厂 build 只取得受限 `TaskRegistrar`；真实所有者安装后，宿主在 lifecycle start 前通过不可克隆的 Scope owner 调用 `enable_registration()`。保留的 registrar 无法开放登记，也无法重新开放已关闭的 Scope。依赖此枚举作穷尽匹配的 Rust 调用方须新增该分支；SS PluginFactory 与 ReplayFactory 将其映射到 `InvalidConfig` 并保留原始来源。

`TaskScope::new` 与 `empty` 的直接登记语义保持，`None` 任务预算在开放后仍为零。此变化不增加 V1 配置字段、IPC 方法或 FFI 符号。新增回归覆盖 build future 未被轮询、实际 start 的 TaskId/退出确认，以及关闭后无法重开。该批尚待父任务的集成编译与资格检查；不代表完整 RuntimeHost 或 M4 用户/I/O 关闭确认。

Datagram 关闭确认持有者由 Proxy 构造并在所有执行服务中共享同一 `Arc`；DNS 回调仅持 `Weak`。本阶段独立宿主策略 `RETIRED_DATAGRAM_LIMIT = 4096` 限制原生 active 与 retiring 控制器总数，它不是旧 UDP packet queue 的容量或既有 session 预算，不驱逐未确认关闭的会话。实际新 open 必须先预留该持有者的容量，旧调用方不能保留失败 Control 时不接入生产。运行时在消费者结束后、组件退役前调用真实 `wait_closed`，超时与失败保留未完成控制器，不伪装为关闭成功。该批接线尚待关联实现的集成验证。

入站 listener 的实际 driver 与 carrier 生命周期登记在 Proxy 构造的持久 `InboundOwnerRegistry`，不放在可取消的 run/activate future 内；角色为空的实际 carrier 使用同一组件分配器的真实 `ComponentInstanceId`。作用域预算由实际外层 driver 一个任务加已准备操作声明的内部 driver 数量组成，TCP+datagram 为两个；group 递归登记，不增加无上限重启。原监听 JoinSet 仅观察真实 Scope，退出 ID 与原始错误先写回持久 owner。停止等待不再使用 watch 接收端消失作为确认，候选退役未确认时保留 owner 并拒绝替代。

该批确认范围是实际 listener/UDP driver task 的退出及协议 owner 报告的直接 Child 回收；它不证明旧 UDP dispatch 的所有逻辑 association、DoH 等连接池、进程后代或所有 transferred I/O 已关闭。旧 inbound process spawn 仍未统一到新的 native ProcessRequest grant，不以同状态生命周期 handle 冒充授权迁移完成。新增真实 socket pending/observer 取消/TaskId 回归尚待父任务集成运行。

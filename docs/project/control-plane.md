# 控制面规范

Zero 的控制面和观测模型以自有核心规范为准。

Clash、sing-box、Xray 等项目只作为行业经验参考，不作为 Zero 内核的 API 契约来源。任何兼容转换由外部项目负责；Zero 内核、SDK 和客户端围绕统一的 Zero 模型设计。

## 原则

- 核心规范先行，外部兼容后置
- 内核能力不继承外部项目的历史包袱
- 外部系统、客户端和 SDK 面向 Zero 规范，而不是面向某个第三方 API
- Clash / sing-box / Xray 兼容只能作为 Zero 仓库和进程之外的转换工具存在
- 外部转换工具不得反向约束 `zero-engine`、配置模型、运行时状态和长期 API

## 边界

核心规范负责定义：

- 配置模型
- 运行时状态模型
- flow 观测模型
- policy 状态和控制动作
- 统计口径
- 错误和事件语义

## 命名

长期控制面命名优先使用 Zero 自己的核心概念：

- `flow`：一次可观测流量生命周期。TCP connection 是 flow，SOCKS5 UDP 到同一目标的报文序列也是 flow
- `outbound`：具体出站能力，例如 `direct`、`block`、`socks5`
- `policy`：选择或组合策略，例如 `selector`、`fallback`、`url_test`
- `target`：路由和模式引用的统一目标，可以指向 outbound 或 policy
- `route`：匹配条件到 target 的决策规则
- `event`：面向控制面消费者的运行时事件

不要把内部实现名固化为外部 API：

- `Session` / `sessions` 是当前内核实现细节，对外应使用 `flow` / `flows`
- 配置字段 `outbound_groups` 在控制面中以 `policy` / `policies` 表达运行时状态

外部适配层负责处理：

- 第三方 API 路径和字段名
- 外部系统私有协议
- 第三方历史字段
- 语义不完全一致时的映射和降级

## 当前控制入口

`--status-listen` 开启本地 HTTP 控制入口。能力模型、trait 边界、adapter/sink 和首批方法见 [api.md](api.md)。控制面先定义核心能力，再由 HTTP/HTTPS、本地 IPC、file、gRPC、二进制帧、Rust API 或 FFI 等实现挂载。

新增观测或控制能力应先回答：

- 这个字段是否属于 Zero 核心规范
- 它是否能被外部系统、客户端和 SDK 长期复用
- 它是否只是某个外部生态的兼容字段
- 如果是外部兼容字段，是否应完全留在 Zero 之外

## GUI 接入短期优先级

短期优化应优先降低 GUI 对接成本，但能力本身仍必须是内核通用原语：

1. **机器可读契约**：从 `zero-api` / `zero-config` 类型导出 JSON Schema 或 OpenAPI，避免 GUI 依赖手写 Markdown 猜字段。
2. **配置预检**：在 `config.apply` 之前提供只读的 apply plan，说明哪些变更可热加载、哪些需要重启 listener、哪些因 feature 未启用会失败。
3. **结构化诊断**：配置校验和 command 错误应稳定返回 `field_path`、错误码和可选详情，方便 GUI 定位到具体表单字段。
4. **观测增量**：优先补齐 flow、policy、config、stats 的事件，让 GUI 少轮询完整 runtime。
5. **DNS 与路由解释**：DNS 查询、Fake IP 映射、route trace 和 policy 决策链属于内核诊断能力，应通过 diagnostics 暴露。
6. **日志控制面**：提供受控日志级别查询/设置和日志流订阅，便于桌面 GUI 排障；日志展示、搜索和归档属于 GUI。

这些能力不应引入面板业务状态。用户、套餐、订阅、计费、节点售卖和审计策略由上层系统保存；内核只提供 `flow`、`policy`、`target`、`route`、`event` 和 `stats` 等原语。

## 参考主流项目的方式

可以参考主流项目解决过的问题：

- Clash 的连接列表、代理组选择和流量观测
- sing-box 的多出站组、规则和平台边界
- Xray 的入站/出站统计和服务化 API

但参考的是问题拆分和成熟口径，不是直接继承 endpoint、字段名和历史行为。

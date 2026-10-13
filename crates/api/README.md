# zero-api

承载 Zero 的核心 API 能力模型、trait 和可选 adapter/sink 实现。

`zero-api` 不应只等同于 HTTP/HTTPS 服务，也不应按传输形态拆散能力。长期形态是：

- 核心类型：`flow`、`outbound`、`policy`、`target`、`route`、`event`
- 核心 trait：`QueryService`、`CommandService`、`EventSource`、`EventStream`、`EventSink`、`ApiCodec`、`ApiAuth`
- 已有 command JSON 形态：`{"method":"policies.select","params":{...}}`
- 可选 adapter：HTTP/HTTPS、IPC、FFI、gRPC、二进制帧、in-process Rust 调用
- 已有 sink 基础实现：local callback、JSON Lines writer、feature-gated HTTP/HTTPS webhook
- 后续可选 sink：file path、gRPC、custom sink
- 第三方兼容 adapter

能力组织按 CQRS 思路划分为 query、command、event 和 sink，但不是完整 Event Sourcing；事件用于观测、审计、计量和外部同步，不作为内核状态的唯一来源。

`EventSource::subscribe` 只表示实时订阅，可在实时增量前附带同步基线；历史检查使用 `latest`，按序号断点恢复使用 `since`。所有自定义 `EventSource` 实现必须返回实现 `EventStream` 的流，不能再用 `Vec<RawApiEvent>` 表示订阅。

对接契约见独立文档站的 [API 契约](https://docs.zerodenet.org/projects/core/control-plane/contract)。

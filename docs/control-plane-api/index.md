# 控制与集成

Zero 通过 HTTP JSON API、gRPC、Unix Domain Socket / Windows Named Pipe、CLI 和事件 Sink 提供统一的查询、命令、事件与诊断能力。本节是 GUI、CLI 和任意外部系统对接 Zero 的正式契约入口。

::: info 历史设计
`control-plane/` 下的路线图和方案已归档为 [控制面设计档案](../control-plane/)，不与本节并列作为对外规范。
:::

## 快速导航

| 文档 | 说明 |
|------|------|
| [configuration.md](./configuration.md) | `api.*` 配置模型完整参考 |
| [http-api.md](./http-api.md) | HTTP JSON 端点规范 |
| [ipc-protocol.md](./ipc-protocol.md) | UDS / Named Pipe 帧协议 |
| [events.md](./events.md) | 事件目录和 payload 规范 |
| [hooks.md](./hooks.md) | FlowHook 扩展点 |
| [connector.md](./connector.md) | Webhook 注册、事件推送格式与 HTTP 确认语义 |
| [cli.md](./cli.md) | CLI 控制命令 |
| [contract.md](./contract.md) | API 契约和外部命名规则 |
| [breaking-changes.md](./breaking-changes.md) | 版本语义、破坏性变更和 GUI 迁移要求 |
| [GUI 接入指南](../guides/gui-integration.md) | 本地 GUI 的 IPC/HTTP 接入流程、状态模型和短期补齐建议 |
| [Connector 接入指南](../guides/connector-integration.md) | 通过 Zero API/gRPC 注册 Webhook 并接收事件 |

## 架构概览

```
┌─────────────────────────────────────────────────────┐
│                 GUI / CLI / 外部系统                  │
├──────────┬──────────┬──────────┬──────────┬─────────┐
│  HTTP    │  gRPC    │   UDS    │   CLI    │ Webhook │
│  :9090   │  :9091   │ .sock    │  zero    │  Sink   │
├──────────┴──────────┴──────────┴──────────┴─────────┤
│                   EngineHandle                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐ │
│  │  Query   │ │ Command  │ │     EventSource      │ │
│  │ Service  │ │ Service  │ │  (SSE / IPC / Sink)  │ │
│  └──────────┘ └──────────┘ └──────────────────────┘ │
├─────────────────────────────────────────────────────┤
│                      Engine                          │
│  ┌──────────┐ ┌──────────────┐ ┌─────────────────┐  │
│  │  Router  │ │ Session Reg  │ │   Event Log     │  │
│  └──────────┘ └──────────────┘ └─────────────────┘  │
└─────────────────────────────────────────────────────┘
```

图中的通道职责不同：GUI 和外部控制器通过 IPC/HTTP/gRPC 直接使用查询、命令、配置和实时事件；中心通过同一控制 API 的 `config.apply` 注册 `api.event_sinks`。EventDispatcher 订阅 `EventSource`，Connector 将筛选后的 `zero.event.v1` 事件可靠投递到注册方给出的完整 Webhook URL。Connector 不定义中心端点、节点注册、同步或配置语义。详见 [Connector 边界](./connector.md)。

## 四种通道对比

| 维度 | HTTP | gRPC | IPC (UDS/Pipe) | CLI |
|------|------|------|----------------|-----|
| 传输 | TCP | HTTP/2；可选原生 TLS/mTLS 或外部 TLS 终止 | Unix Domain Socket / Named Pipe | UDS / Named Pipe |
| 认证 | Bearer token | 可选 Bearer、mTLS，或两者叠加 | 文件系统权限 (0600) | 文件系统权限 |
| 查询 | `GET /api/v1/*` | proto RPC | `{"type":"query","id":1,...}` | `zero status/flows/policies` |
| 命令 | `POST /api/v1/commands` | proto RPC | `{"type":"command","id":1,...}` | `zero select <p> <t>` |
| 事件流 | SSE (`text/event-stream`) | server streaming | JSON-line 推送 | `zero events` |
| 适用场景 | 远程调试、外部控制器 | 服务端集成、SDK | 本地 GUI 进程 | 终端管理 |
| 默认端口/路径 | 127.0.0.1:9090 | 127.0.0.1:9091 | `~/.zero/control.sock` / `\\.\pipe\zero-control` | 自动发现 |

## 核心设计原则

1. **内核通用** — API 不绑定任何特定外部产品或平台，所有消费者平等
2. **能力原语** — 暴露原子能力（查询、切换、关闭），业务逻辑在外部
3. **多通道一致** — HTTP、gRPC、IPC、CLI 四种通道共享相同的语义和数据模型
4. **安全可组合** — 本地 IPC 依赖文件权限；远程控制可组合 Bearer、原生 TLS/mTLS 或外部 TLS 终止，不把单一部署方案写死
5. **事件驱动** — 所有状态变更以归一化事件推送，支持 SSE、IPC 流、Sink 投递三种消费方式

## GUI 对接重点

本地 GUI 优先使用 IPC，HTTP 作为浏览器 WebView 或远程调试备选。GUI 首屏应先查询 `health`、`capabilities`、`config`、`runtime`，再建立事件订阅；重连后用 `runtime` / `stats` 重建界面状态。配置编辑应先走 `config.validate`，成功后再调用 `config.apply`。

短期对 GUI 最有价值的内核能力是机器可读契约、配置影响预检、结构化校验诊断、DNS/路由解释、日志流和更完整的 flow/policy 事件。这些能力应作为 Zero 内核通用控制面原语设计，不引入外部业务概念。

## 最小可用配置

```json
{
  "inbounds": [...],
  "outbounds": [...],
  "route": {...},
  "api": {
    "control": {
      "enabled": true,
      "listen": { "address": "127.0.0.1", "port": 9090 }
    }
  }
}
```

启动后即可通过 HTTP 或 IPC 访问控制平面：

```bash
# HTTP
curl http://127.0.0.1:9090/api/v1/runtime

# CLI (自动连接 ~/.zero/control.sock)
zero status
zero select proxy direct
zero events
```

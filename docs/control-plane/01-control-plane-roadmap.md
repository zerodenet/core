# 控制面实现路线图

> [!IMPORTANT]
> 本文是历史设计与路线图，不是当前 API 契约。当前实现以[公开控制面 API](https://docs.zerodenet.org/projects/core/control-plane/)为准。

本文档详细规划 Zero 控制面的实现步骤、模块划分和验收标准。

## 核心设计哲学

> **预留集成能力，但不实现业务逻辑。**

| ✅ 内核做什么（原子能力） | ❌ 内核不做什么（业务逻辑） |
| --- | --- |
| 提供标准化 API | 不实现特定面板 UI |
| 导出归一化事件 | 不内置计费/套餐逻辑 |
| 支持配置热重载 | 不做用户数据库 |
| 按租户归因统计 | 不做多节点集群管理 |
| 提供策略切换原语 | 不做智能调度算法 |
| 定义稳定的扩展点 | 不做闭源私有功能 |

所有业务逻辑（机场面板、客户端、计费系统等）通过标准化 API 对接内核，不侵入核心代码。

---

## 关键设计决策一览

| # | 决策 | 设计方案 | 详细文档 |
| --- | --- | --- | --- |
| 1 | 控制面模式 | 中心使用 Zero API；节点通过已注册 Webhook 回传事件 | [Connector 边界](../project/connector-architecture.md) |
| 2 | 节点上报 | 中心以 `config.apply` 注册完整 URL，Connector 可靠投递 | [Connector 边界](../project/connector-architecture.md) |
| 3 | 机场集成 | 预留能力，不实现业务逻辑 | [06-service-provider-integration.md](./06-service-provider-integration.md) |
| 4 | 性能承载 | 单节点 5万~10万并发连接 | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |
| 5 | 控制面开销 | 占总开销 < 5% | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |
| 6 | 限流分层 | 内核做基础限流，上层做业务限流 | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |
| 7 | 扩展机制 | FlowHook ABI，支持插件接入 | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |
| 8 | 设备数限制 | Connector + 中心控制面实现 | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |
| 9 | 积分/余额 | Hook + 计费系统联动 | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |
| 10 | 共享检测 | 中心控制面跨节点分析 | [08-performance-and-rate-limiting.md](./08-performance-and-rate-limiting.md) |

---

## 阶段划分

### 阶段 1：核心模型收敛（当前）

**目标**：完成 `zero-api` 核心类型和 trait 定义，对齐已有能力。

**交付物**：

1. **核心类型定义**
   - `Flow` 类型：活动 flow 和已完成 flow 的统一视图
   - `Policy` 类型：`selector`、`fallback`、`url_test` 的状态表示
   - `Stats` 类型：运行时统计的归一化结构
   - `RuntimeInfo` 类型：进程、构建标识、启动时间、监听信息

2. **能力 Trait**
   - `QueryService` trait：定义所有只读查询方法签名
   - `CommandService` trait：定义所有控制命令方法签名
   - `EventSource` trait：定义事件订阅接口
   - `EventSink` trait：定义事件投递接口

3. **错误和权限**
   - 稳定错误码枚举和错误结构
   - 权限范围枚举和权限检查接口
   - 认证 trait 定义

**验收标准**：
- `/api/v1/runtime`、`/api/v1/config`、`/api/v1/stats` 的字段能映射到核心类型
- `POST /api/v1/commands` 的 `policies.select` 能映射到 `CommandService`
- `flow.completed` 事件能映射到 `EventSource`
- webhook sink 能实现 `EventSink` trait

---

### 阶段 2：In-process Adapter

**目标**：提供进程内 Rust API，服务测试、嵌入式集成和内部调用。

**交付物**：

1. `EngineHandle` 封装
   - 实现 `QueryService`
   - 实现 `CommandService`
   - 实现 `EventSource`

2. 内存事件总线
   - 支持多订阅者
   - 支持事件过滤
   - 支持 backpressure

**验收标准**：
- 测试用例可以通过 `EngineHandle` 查询运行时状态
- 测试用例可以通过 `EngineHandle` 切换 `selector`
- 测试用例可以订阅 `flow.completed` 事件

---

### 阶段 3：HTTP JSON Adapter

**目标**：提供标准 HTTP/HTTPS JSON 接口，服务调试和通用接入。

**交付物**：

1. **路由设计**
   ```
   GET  /api/v1/capabilities
   GET  /api/v1/health
   GET  /api/v1/config
   GET  /api/v1/runtime
   GET  /api/v1/stats
   GET  /api/v1/flows
   GET  /api/v1/flows/{flow_id}
   GET  /api/v1/policies
   GET  /api/v1/policies/{policy_tag}
   POST /api/v1/commands
   GET  /api/v1/events/stream
   ```

2. **命令请求格式**
   ```json
   {
     "id": "req-123",
     "method": "policies.select",
     "params": {
       "policy_tag": "proxy",
       "target_tag": "direct"
     }
   }
   ```

3. **事件流格式**
   - Server-Sent Events (SSE)
   - 支持 `Last-Event-ID` 断点续传
   - 支持事件类型过滤

4. **认证**
   - Bearer Token 认证
   - 可选 mTLS 认证
   - 权限范围检查

**验收标准**：
- 所有 Query 方法可通过 HTTP 访问
- `policies.select` 命令可通过 HTTP 执行
- 事件流可通过 SSE 订阅
- 未认证请求被拒绝
- 权限不足请求被拒绝

---

### 阶段 4：事件 Sink 框架

**目标**：完成可配置的多 Sink 事件导出框架。

**交付物**：

1. **Sink 配置模型**
   ```json
   {
     "event_sinks": [
       {
         "tag": "billing",
         "type": "http",
         "url": "https://example.com/events",
         "events": ["flow.completed", "stats.sampled"],
         "encoding": "json"
       },
       {
         "tag": "audit",
         "type": "file",
         "path": "/var/log/zero/events.jsonl",
         "events": ["flow.completed"],
         "encoding": "json-lines"
       }
     ]
   }
   ```

2. **Sink Manager**
   - 多 Sink 并行投递
   - 按事件类型过滤
   - 投递状态跟踪
   - 失败重试策略
   - 背压和缓冲

3. **内置 Sink 实现**
   - `FileSink`：JSON Lines 文件，支持轮转
   - `HttpSink`：HTTP/HTTPS Webhook，支持批量
   - `MemorySink`：内存缓冲，用于测试

**验收标准**：
- 配置中可定义多个 sink
- 事件按配置过滤后投递到对应 sink
- 投递失败按策略重试
- 文件 sink 支持按大小轮转
- HTTP sink 支持批量投递

---

### 阶段 5：本地 IPC Adapter

**目标**：提供高性能本地控制入口，服务 CLI 和桌面客户端。

**交付物**：

1. **Unix Domain Socket Adapter** (Linux/macOS)
   - 字节帧协议
   - 支持 request/response
   - 支持事件流
   - 利用文件系统权限

2. **Named Pipe Adapter** (Windows)
   - 兼容 Windows 权限模型
   - 相同协议语义

3. **CLI 控制命令**
   ```bash
   zero status
   zero stats
   zero flows
   zero policies
   zero select <policy> <target>
   zero events --tail
   ```

**验收标准**：
- CLI 可以查询本地运行中的 zero 进程状态
- CLI 可以切换 policy 选择
- CLI 可以实时 tail 事件流
- 没有权限的用户无法访问 IPC socket

---

## 认证和安全

### 认证方式

1. **Bearer Token**
   - 配置中设置 `api.secret`
   - 请求头：`Authorization: Bearer <token>`

2. **mTLS** (可选)
   - 客户端证书认证
   - 适合远程管理场景

3. **IPC 权限**
   - Unix socket 使用文件系统权限 (`0600`)
   - Windows named pipe 使用 ACL

### 权限矩阵

| 操作 | `read` | `control` | `config` | `admin` |
| --- | :---: | :---: | :---: | :---: |
| `capabilities.get` | ✓ | ✓ | ✓ | ✓ |
| `health.get` | ✓ | ✓ | ✓ | ✓ |
| `config.get` | ✓ | ✓ | ✓ | ✓ |
| `runtime.get` | ✓ | ✓ | ✓ | ✓ |
| `stats.get` | ✓ | ✓ | ✓ | ✓ |
| `flows.*` | ✓ | ✓ | ✓ | ✓ |
| `policies.*` | ✓ | ✓ | ✓ | ✓ |
| `policies.select` | | ✓ | ✓ | ✓ |
| `policies.probe` | | ✓ | ✓ | ✓ |
| `flows.close` | | ✓ | ✓ | ✓ |
| `config.validate` | | | ✓ | ✓ |
| `config.apply` | | | ✓ | ✓ |
| `diagnostics.*` | | | | ✓ |

---

## 当前状态

> **五个阶段全部完成。** 控制面已实现完整的 Query/Command/Event 三类 API，
> 通过 HTTP、IPC、CLI 三种通道对外暴露，共享 `zero-api` 类型层和统一信封格式。

### 已实现能力总览

| 阶段 | 交付物 | 代码位置 |
|------|--------|----------|
| 1 · 核心模型 | 6 个 trait、13 种 Query、11 种 Command、12 种事件、7 种错误码、4 级权限 | `crates/api/src/` |
| 2 · In-process | `EngineHandle`（Query + Command + EventSource）+ 内存事件总线 | `crates/engine/src/api/` |
| 3 · HTTP | `/api/v1/*` 端点 + Bearer Token + 限流 + SSE | `src/http_adapter/` |
| 4 · Sink | 6 种 Sink 实现 + SinkManager + 事件分发器 + DeadLetter | `crates/api/src/sink.rs`、`crates/connector/src/` |
| 5 · IPC | Unix Socket + Windows Named Pipe + 多路复用 + CLI 集成 | `src/ipc/` |

### 路线图之外已实现

- `mode.set` 命令（rule / global / direct 运行时切换）
- `diagnostics.*`（probe_target / probe_outbound / dns_lookup / trace_route）
- `tun.start` / `tun.stop` 命令（通过 ProxyHandle 拦截转发）
- `ipc.connected` / `ipc.disconnected` 生命周期事件
- HTTP 限流（Query 100/s、Command 10/s、SSE 5 并发）
- `DeadLetterSink`（失败事件持久化到 JSON-line 文件）
- Push connector（节点主动上报 + 心跳 + 远程命令）
- FlowHook trait（外部决策插件）

---

## 验收检查清单

### 阶段 1 验收 ✅

- [x] 所有现有状态字段映射到 `zero-api` 核心类型
- [x] `QueryService` trait 包含所有现有查询能力
- [x] `CommandService` trait 包含 `policies.select`
- [x] `EventSource` trait 支持 `flow.completed`
- [x] 统一错误码和错误结构定义完成
- [x] 权限范围枚举定义完成

### 阶段 2 验收 ✅

- [x] `EngineHandle` 实现 `QueryService`
- [x] `EngineHandle` 实现 `CommandService`
- [x] `EngineHandle` 实现 `EventSource`
- [x] 集成测试验证查询能力
- [x] 集成测试验证 policy 切换
- [x] 集成测试验证事件订阅

### 阶段 3 验收 ✅

- [x] HTTP 路由覆盖所有 Query 端点
- [x] HTTP 命令端点支持 `policies.select`
- [x] SSE 事件流支持 `flow.completed`
- [x] Bearer Token 认证生效
- [x] 权限检查生效
- [x] 所有端点返回统一错误格式

### 阶段 4 验收 ✅

- [x] 配置模型支持多 sink 定义
- [x] `flow.completed` 事件投递到配置的 sink
- [x] HTTP sink 支持失败重试
- [x] File sink 支持日志轮转
- [x] 事件过滤按配置生效
- [x] 投递状态可查询

### 阶段 5 验收 ✅

- [x] Unix domain socket adapter 实现
- [x] CLI status 命令可用
- [x] CLI select 命令可用
- [x] CLI events --tail 命令可用
- [x] 文件系统权限保护生效
- [x] 相同 API 语义在 HTTP 和 IPC 上一致

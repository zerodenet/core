# HTTP API 端点

> [!IMPORTANT]
> 本文是历史设计稿。当前 HTTP 端点以[公开 HTTP JSON API](https://docs.zerodenet.org/projects/core/control-plane/http-api)为准。

本文档是当前本地 HTTP 控制面的历史索引。详细线路契约见[公开 HTTP JSON API](https://docs.zerodenet.org/projects/core/control-plane/http-api)。

## 基础路径

所有 HTTP 控制端点均在 `/api/v1/` 下。

HTTP 和 IPC 响应共享 `zero_api::ApiResponse` 信封：

```json
{
  "api_id": "zero.api.v1",
  "ok": true,
  "result": {}
}
```

错误使用 `snake_case` 机器码：

| 错误码 | HTTP | 含义 |
|------|------|------|
| `not_found` | 404 | 资源不存在 |
| `invalid_argument` | 400 | 请求参数无效 |
| `permission_denied` | 403 | 调用者权限不足 |
| `insufficient_os_privilege` | 403 | 宿主机操作系统权限不足 |
| `feature_disabled` | 501 | 功能未启用 |
| `conflict` | 409 | 状态冲突 |
| `unsupported` | 501 | 操作未暴露 |
| `internal` | 500 | 内部错误 |

## 查询端点

| 端点 | 含义 |
|------|------|
| `GET /api/v1/capabilities` | 构建/运行时能力、适配器、接收器、权限 |
| `GET /api/v1/health` | 进程健康状态 |
| `GET /api/v1/config` | 当前配置快照 |
| `GET /api/v1/runtime` | 运行时快照，含统计、流、日志状态 |
| `GET /api/v1/stats` | 即时统计快照 |
| `GET /api/v1/flows` | 活动流列表 |
| `GET /api/v1/flows/{flow_id}` | 单个活动流 |
| `GET /api/v1/policies` | 策略/组状态 |
| `GET /api/v1/policies/{policy_tag}` | 单个策略/组状态 |
| `GET /api/v1/sinks` | 事件接收器投递状态 |
| `GET /api/v1/tun_status` | TUN 运行时状态 |
| `GET /api/v1/events` | 事件日志快照 |

`GET /api/v1/stats` 以及 `GET /api/v1/runtime` 中的统计部分是处理请求时根据当前内存计数器计算的。

## 命令端点

所有写入/控制操作使用 `POST /api/v1/commands`。

```json
{
  "method": "policies.select",
  "params": {
    "policy_tag": "proxy",
    "target_tag": "direct"
  }
}
```

当前命令方法：

| 方法 | 含义 |
|------|------|
| `policies.select` | 为 selector 策略选择成员 |
| `policies.probe` | 触发 url_test 探测 |
| `flows.close` | 关闭活动流 |
| `config.validate` | 验证配置对象 |
| `config.apply` | 将配置对象应用到运行时状态 |
| `mode.set` | 设置全局路由模式 |
| `tun.start` | 启动 TUN |
| `tun.stop` | 停止 TUN |
| `diagnostics.probe_target` | 探测目标 TCP 端点 |
| `diagnostics.probe_outbound` | 经指定出站执行 HTTP 延迟探测；绕过且不修改共享出站健康状态，也不修改 URLTest 策略状态 |
| `diagnostics.dns_lookup` | 解析主机名 |
| `diagnostics.trace_route` | 追踪目标的路由决策 |

## 事件流

`GET /api/v1/events/stream` 返回 Server-Sent Events。

查询参数：

| 参数 | 含义 |
|------|------|
| `types` | 逗号分隔的事件类型白名单 |
| `since` | 从指定序号之后回放事件 |

服务端也接受 `Last-Event-ID` 进行断点续传。

`stats.sampled` 在运行时活动期间每秒发射一次。它是供 GUI 刷新和 sink 投递使用的粗粒度系统事件，不能替代按需查询快照。

## 认证

配置了 API 密钥时，调用者使用：

```http
Authorization: Bearer <token>
```

或：

```http
X-Zero-Api-Key: <token>
```

未配置 HTTP 认证时，请求被视为本地管理控制。公网监听应配置 API 密钥和防火墙边界。

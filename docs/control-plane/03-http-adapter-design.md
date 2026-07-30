# HTTP 适配器设计

> [!IMPORTANT]
> 本文用于保存实现设计背景，不是当前接口契约。对接时请使用[公开控制面 API](https://docs.zerodenet.org/projects/core/control-plane/)。

本文档详细规划 HTTP JSON Adapter 的实现架构和模块划分。

## 代码结构

```
crates/api/src/http/
├── mod.rs
├── server.rs
├── router.rs
├── handlers/
│   ├── mod.rs
│   ├── capabilities.rs
│   ├── health.rs
│   ├── config.rs
│   ├── runtime.rs
│   ├── stats.rs
│   ├── flows.rs
│   ├── policies.rs
│   ├── commands.rs
│   └── events.rs
├── middleware/
│   ├── mod.rs
│   ├── auth.rs
│   ├── cors.rs
│   ├── logging.rs
│   └── ratelimit.rs
├── response.rs
└── sse.rs
```

---

## 核心组件

### 1. HttpServer

负责启动和管理 HTTP 服务器生命周期。

```rust
pub struct HttpServer {
    listen: SocketAddr,
    router: Router,
    shutdown: Option<oneshot::Receiver<()>>,
}

impl HttpServer {
    pub fn new(config: &HttpConfig, query: Arc<dyn QueryService>, 
               command: Arc<dyn CommandService>, events: Arc<dyn EventSource>) -> Result<Self>;
    pub async fn run(self) -> Result<()>;
}

pub struct HttpConfig {
    pub listen: SocketAddr,
    pub secret: Option<String>,
    pub cors_allowed_origins: Vec<String>,
    pub enable_sse: bool,
}
```

---

### 2. Router

使用 `axum` 作为底层 Web 框架，路由分发：

```rust
pub fn make_router(
    query: Arc<dyn QueryService>,
    command: Arc<dyn CommandService>,
    events: Arc<dyn EventSource>,
    auth: Arc<AuthService>,
) -> Router {
    Router::new()
        .route("/api/v1/capabilities", get(capabilities::get))
        .route("/api/v1/health", get(health::get))
        .route("/api/v1/config", get(config::get))
        .route("/api/v1/runtime", get(runtime::get))
        .route("/api/v1/stats", get(stats::get))
        .route("/api/v1/flows", get(flows::list))
        .route("/api/v1/flows/:flow_id", get(flows::get))
        .route("/api/v1/policies", get(policies::list))
        .route("/api/v1/policies/:policy_tag", get(policies::get))
        .route("/api/v1/commands", post(commands::execute))
        .route("/api/v1/events/stream", get(events::stream))
        .layer(middleware())
}
```

---

### 3. Handler 统一模式

```rust
pub async fn handler(
    State(ctx): State<Arc<Context>>,
    Query(params): Query<Params>,
    claims: Claims,
) -> Result<Json<ApiResponse<T>>, ApiError> {
    claims.require_permission(Permission::Read)?;
    validate_params(&params)?;
    let result = ctx.query_service.method(params).await?;
    Ok(Json(ApiResponse::ok(result)))
}
```

---

## Handler 实现示例

### Commands Handler

```rust
#[derive(Debug, Deserialize)]
pub struct CommandRequest {
    pub id: Option<String>,
    pub method: String,
    pub params: serde_json::Value,
}

pub async fn execute(
    State(ctx): State<Arc<Context>>,
    claims: Claims,
    Json(req): Json<CommandRequest>,
) -> Result<Json<ApiResponse<serde_json::Value>>, ApiError> {
    let result = match req.method.as_str() {
        "policies.select" => {
            claims.require_permission(Permission::Control)?;
            let params: SelectParams = serde_json::from_value(req.params)?;
            let result = ctx.command_service.select_policy(params).await?;
            serde_json::to_value(result)?
        }
        "policies.probe" => {
            claims.require_permission(Permission::Control)?;
            let params: ProbeParams = serde_json::from_value(req.params)?;
            let result = ctx.command_service.probe_policy(params).await?;
            serde_json::to_value(result)?
        }
        "flows.close" => {
            claims.require_permission(Permission::Control)?;
            let params: CloseFlowParams = serde_json::from_value(req.params)?;
            let result = ctx.command_service.close_flow(params).await?;
            serde_json::to_value(result)?
        }
        _ => return Err(ApiError::unsupported("unknown command method")),
    };
    
    Ok(Json(ApiResponse::with_id(req.id, result)))
}
```

---

### Events SSE Handler

```rust
use axum::response::sse::{Event, Sse};
use futures::stream::Stream;

pub async fn stream(
    State(ctx): State<Arc<Context>>,
    Query(params): Query<StreamParams>,
    claims: Claims,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, ApiError> {
    claims.require_permission(Permission::Read)?;
    
    let type_filter = params.types.map(|s| s.split(',').map(|t| t.trim().into()).collect());
    let subscriber = ctx.event_source.subscribe(type_filter).await?;
    
    let stream = subscriber
        .map(|event| {
            Event::default()
                .id(event.id)
                .event(event.event_type)
                .data(serde_json::to_string(&event.payload)?)
        });
    
    Ok(Sse::new(stream))
}
```

---

## Middleware

### Auth Middleware

```rust
#[derive(Debug, Clone)]
pub struct AuthService {
    secret: Option<String>,
}

impl AuthService {
    pub fn new(secret: Option<String>) -> Self {
        Self { secret }
    }
    
    pub async fn verify<B>(&self, req: Request<B>) -> Result<Request<B>, ApiError> {
        if self.secret.is_none() {
            return Ok(req);
        }
        
        let auth_header = req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| ApiError::permission_denied("missing authorization header"))?;
        
        if !auth_header.starts_with("Bearer ") {
            return Err(ApiError::permission_denied("invalid authorization scheme"));
        }
        
        let token = &auth_header[7..];
        
        if Some(token) != self.secret.as_deref() {
            return Err(ApiError::permission_denied("invalid token"));
        }
        
        Ok(req)
    }
}
```

---

## Response 统一格式

```rust
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub api_id: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorBody>,
}
```

---

## 错误处理

```rust
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("permission denied")]
    PermissionDenied,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("internal error")]
    Internal,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            ApiError::PermissionDenied => StatusCode::FORBIDDEN,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Conflict(_) => StatusCode::CONFLICT,
            ApiError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
            ApiError::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        
        let body = Json(ApiResponse::<()>::error(self));
        (status, body).into_response()
    }
}
```

---

## Context 状态传递

```rust
pub struct Context {
    pub query_service: Arc<dyn QueryService>,
    pub command_service: Arc<dyn CommandService>,
    pub event_source: Arc<dyn EventSource>,
    pub features: Vec<String>,
}
```

---

## 与 zero-engine 集成

在 `zero-proxy` 中：

```rust
async fn start_api_server(&self, config: &ApiConfig, engine_handle: EngineHandle) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    
    let query = Arc::new(EngineQueryAdapter::new(engine_handle.clone()));
    let command = Arc::new(EngineCommandAdapter::new(engine_handle.clone()));
    let events = Arc::new(EngineEventSource::new(engine_handle));
    
    let http_config = HttpConfig {
        listen: config.listen,
        secret: config.secret.clone(),
        cors_allowed_origins: config.cors_allowed_origins.clone(),
        enable_sse: true,
    };
    
    let server = HttpServer::new(&http_config, query, command, events)?;
    tokio::spawn(server.run());
    
    Ok(())
}
```

---

## 路由边界

HTTP adapter 只暴露 `/api/v1/*` 控制面端点。所有控制命令进入
`POST /api/v1/commands`，再交给 `zero_api::CommandRequest` 反序列化和
`CommandService` 执行。adapter 不维护旧路径映射，也不直接操作 engine 内部结构。

---

## 测试策略

### 单元测试
- 测试每个 handler 的请求解析
- 测试参数校验
- 测试错误响应格式

### 集成测试
```rust
#[tokio::test]
async fn test_capabilities_endpoint() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    
    let resp = client.get(&format!("http://{}/api/v1/capabilities", server.addr()))
        .send()
        .await
        .unwrap();
    
    assert_eq!(resp.status(), 200);
}
```

---

## 实现顺序

1. **Phase 1**: 基础框架
   - ApiResponse 和 ApiError 类型
   - Context 定义
   - 基础 Router 骨架
   - capabilities、health handler

2. **Phase 2**: Query 端点
   - config handler
   - runtime handler
   - stats handler
   - flows handler
   - policies handler

3. **Phase 3**: Command 端点
   - /api/v1/commands 入口
   - policies.select 实现
   - policies.probe 实现

4. **Phase 4**: 事件流
   - SSE 事件流实现
   - flow.completed 事件转发

5. **Phase 5**: Middleware
   - Auth middleware
   - CORS middleware
   - Logging middleware

6. **Phase 6**: 集成测试
   - 所有端点的集成测试
   - 认证和权限测试
   - 路由边界测试

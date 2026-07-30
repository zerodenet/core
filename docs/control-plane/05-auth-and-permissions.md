# 认证与权限模型设计

> [!IMPORTANT]
> 本文是认证与权限设计背景。当前公开接口及实际权限行为以代码和[公开控制面 API](https://docs.zerodenet.org/projects/core/control-plane/)为准。

本文档详细规划控制面的认证、授权和权限检查机制。

## 权限模型

### 权限层级

```
admin ───────────────────┐
  │                      │
  ├─ config              │
  │                      │
  ├─ control             │
  │   └─ policies.select │
  │   └─ flows.close     │
  │                      │
  └─ read                │
      └─ flows.*         │
      └─ policies.*      │
      └─ runtime         │
      └─ config          │
      └─ stats           │
```

### 权限枚举

```rust
// crates/api/src/auth.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Read,      // 只读权限
    Control,   // 控制权限（切换 policy、关闭 flow）
    Config,    // 配置变更权限
    Admin,     // 管理员权限
}

impl Permission {
    pub fn implies(self, required: Permission) -> bool {
        match (self, required) {
            (Self::Admin, _) => true,
            (Self::Config, Self::Read) => true,
            (Self::Config, Self::Config) => true,
            (Self::Control, Self::Read) => true,
            (Self::Control, Self::Control) => true,
            (Self::Read, Self::Read) => true,
            _ => false,
        }
    }

    pub fn all() -> &'static [Self] {
        &[Self::Read, Self::Control, Self::Config, Self::Admin]
    }
}
```

---

## 权限矩阵

| 操作 | Read | Control | Config | Admin |
| --- | :---: | :---: | :---: | :---: |
| **Query 端点** | | | | |
| GET /api/v1/capabilities | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/health | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/config | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/runtime | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/stats | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/flows* | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/policies* | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/diagnostics | ✓ | ✓ | ✓ | ✓ |
| GET /api/v1/sinks | ✓ | ✓ | ✓ | ✓ |
| **Command 端点** | | | | |
| policies.select | | ✓ | ✓ | ✓ |
| flows.close | | ✓ | ✓ | ✓ |
| policies.probe | | ✓ | ✓ | ✓ |
| config.validate | | | ✓ | ✓ |
| config.apply | | | | ✓ |
| diagnostics.probe_target | | | | ✓ |
| diagnostics.dns_lookup | | | | ✓ |
| diagnostics.trace_route | | | | ✓ |
| mode.set | | | | ✓ |
| tun.start | | | | ✓ |
| tun.stop | | | | ✓ |

---

## 认证方式

### 1. Bearer Token 认证

用于 HTTP API，配置中设置密钥：

```yaml
api:
  listen: 127.0.0.1:3000
  secret: "your-secret-key-here"
```

请求时携带：
```
Authorization: Bearer your-secret-key-here
```

> **安全规则**: 监听 0.0.0.0 时必须配置 secret，否则启动失败。

### 2. 本地无认证模式

仅监听 127.0.0.1 且未配置 secret 时，默认所有请求拥有全部权限。
这是为了方便本地开发和 CLI 控制。

### 3. Unix Socket 权限（未来）

- socket 文件权限：`0600` (仅所有者可读写)
- 由操作系统级别的权限控制访问

---

## 核心实现

### Claims 结构

```rust
// crates/api/src/auth.rs
#[derive(Debug, Clone)]
pub struct Claims {
    pub permissions: Vec<Permission>,
    pub principal: Option<String>,
    pub auth_method: AuthMethod,
}

#[derive(Debug, Clone, Copy)]
pub enum AuthMethod {
    None,          // 本地无认证
    BearerToken,   // HTTP Bearer Token
    UnixSocket,    // Unix Socket
    InProcess,     // 进程内调用
}

impl Claims {
    pub fn all_permissions() -> Self {
        Self {
            permissions: Permission::all().to_vec(),
            principal: None,
            auth_method: AuthMethod::None,
        }
    }

    pub fn has_permission(&self, required: Permission) -> bool {
        self.permissions.iter().any(|&p| p.implies(required))
    }

    pub fn require_permission(&self, required: Permission) -> Result<(), ApiError> {
        if self.has_permission(required) {
            Ok(())
        } else {
            Err(ApiError::PermissionDenied)
        }
    }
}
```

---

### Auth Service

```rust
// crates/api/src/auth.rs
#[derive(Debug, Clone)]
pub struct AuthService {
    config: AuthConfig,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub listen_addr: SocketAddr,
    pub secret: Option<String>,
}

impl AuthService {
    pub fn new(config: AuthConfig) -> Result<Self> {
        // 安全检查：公网监听必须配置 secret
        if !config.listen_addr.ip().is_loopback() && config.secret.is_none() {
            return Err(ApiError::InvalidArgument(
                "public interface requires api.secret to be set".into()
            ));
        }
        Ok(Self { config })
    }

    pub fn authenticate_http(&self, req: &Request<Body>) -> Result<Claims, ApiError> {
        // 如果未配置 secret，返回所有权限（仅本地监听允许）
        let secret = match &self.config.secret {
            None => return Ok(Claims::all_permissions()),
            Some(s) => s,
        };

        // 获取 Authorization header
        let auth_header = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or(ApiError::PermissionDenied)?;

        if !auth_header.starts_with("Bearer ") {
            return Err(ApiError::PermissionDenied);
        }

        let token = &auth_header[7..];

        // 使用 constant time 比较防止时序攻击
        use subtle::ConstantTimeEq;
        if token.as_bytes().ct_eq(secret.as_bytes()).into() {
            Ok(Claims {
                permissions: Permission::all().to_vec(),
                principal: None,
                auth_method: AuthMethod::BearerToken,
            })
        } else {
            Err(ApiError::PermissionDenied)
        }
    }
}
```

---

## Axum 中间件集成

```rust
pub async fn auth_middleware(
    State(auth): State<AuthMiddleware>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // 公开端点跳过认证
    let path = req.uri().path();
    if path == "/health" || path == "/api/v1/health" || path == "/api/v1/capabilities" {
        return Ok(next.run(req).await);
    }

    // 认证
    let claims = auth.auth_service.authenticate_http(&req)?;

    // 将 claims 存入 request extensions 供 handler 使用
    req.extensions_mut().insert(claims);

    Ok(next.run(req).await)
}
```

---

## 配置验证

### 启动时安全检查

```rust
async fn validate_api_config(config: &ApiConfig) -> Result<()> {
    // 检查是否是公网监听
    let is_public = !config.listen.ip().is_loopback();

    // 公网监听必须配置 secret
    if is_public && config.secret.is_none() {
        return Err(anyhow!(
            "security violation: listening on {} requires api.secret to be set",
            config.listen
        ));
    }

    // 警告：使用弱 secret
    if let Some(secret) = &config.secret {
        if secret.len() < 16 {
            tracing::warn!("api.secret is too short (minimum 16 characters recommended)");
        }
    }

    Ok(())
}
```

---

## 未来扩展

### 1. 多 Token 支持

```yaml
api:
  tokens:
    - token: "admin-token"
      permissions: ["admin"]
    - token: "viewer-token"
      permissions: ["read"]
    - token: "controller-token"
      permissions: ["read", "control"]
```

### 2. JWT 支持

支持标准 JWT token，包含过期时间、权限声明等。

### 3. mTLS 客户端认证

通过客户端证书进行认证，适合高安全要求的部署场景。

---

## 实现顺序

1. **Phase 1**: 权限模型
   - Permission 枚举和蕴含逻辑
   - Claims 结构

2. **Phase 2**: Auth Service
   - AuthService 实现
   - Bearer Token 验证
   - 时序攻击防护

3. **Phase 3**: HTTP 中间件
   - Axum auth middleware
   - Handler 权限检查模式

4. **Phase 4**: 配置验证
   - 启动时安全检查
   - 公网监听强制认证

5. **Phase 5**: 测试
   - 权限逻辑单元测试
   - HTTP 认证集成测试
   - 安全边界测试

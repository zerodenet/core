# GUI 开发者配置验证失败指南

本文档描述常见的配置失败模式以及它们如何通过 Zero 控制面 API 展现。可用于构建带验证反馈的配置编辑 UI。

## 验证工作原理

Zero 提供两个与配置相关的命令，均可通过 IPC 和 HTTP 使用：

| 命令 | 效果 | 使用场景 |
|---------|--------|----------|
| `config.validate` | 解析并验证配置。无副作用，不会影响正在运行的内核。 | 应用前的预检。可在编辑过程中安全地反复调用。 |
| `config.apply` | 解析、验证并将配置应用到运行中的内核。热重载路由规则、分组和模式；inbounds/outbounds 变更需要重启。 | 用户确认变更后的最后一步。 |

### GUI 推荐两步工作流

1. **维护本地草稿配置** -- 用户在 UI 中编辑字段，GUI 将草稿序列化为 JSON `config` 值。
2. **调用 `config.validate`** -- 将草稿作为 `config` 参数发送。成功时（`ok: true`，result `{"valid": true}`），启用"应用"按钮。失败时，解析错误并显示在对应字段旁边。
3. **调用 `config.apply`** -- 在用户明确操作时（例如点击"应用"），发送相同草稿。成功时，重新查询 `config` 和 `runtime` 以确认内核状态。

validate 步骤已验证但未应用。apply 步骤已验证并已应用。两者均运行相同的 `RuntimeConfig::parse()` 路径，因此验证错误在两者之间完全相同。

## 错误响应格式

所有控制面响应使用 `ApiResponse` 信封：

```json
{
  "api_id": "zero.api.v1",
  "ok": true,
  "result": { "valid": true }
}
```

错误时：

```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "error": {
    "code": "invalid_argument",
    "message": "config validation failed",
    "field_path": null
  }
}
```

### EnvelopeError 字段

| 字段 | 始终存在 | 描述 |
|-------|---------------|-------------|
| `code` | 是 | 机器可读的错误码，使用 `snake_case` 格式。据此进行分支判断。 |
| `message` | 是 | 人类可读的摘要。可直接显示。 |
| `field_path` | 否 | 当存在时，指向无效参数（例如 `"policy_tag"`）。为 null 或不存在时表示错误不关联到单一字段。 |

### ApiErrorCode 值

| 代码 | 含义 | 典型配置场景 |
|------|---------|------------------------|
| `invalid_argument` | 输入格式错误或验证失败 | JSON 语法错误、缺少必填字段、无效的 cipher、错误的端口、重复的 tag、循环分组引用 |
| `feature_disabled` | 该协议/功能未编译进此二进制文件 | 配置中使用了 `trojan` 但二进制文件未使用 `--features trojan` 构建 |
| `permission_denied` | 调用方缺少所需权限 | IPC 会话权限不足（配置操作中少见） |
| `not_found` | 引用的资源不存在 | 引用了未定义的路由目标 tag 或规则集 tag |
| `internal` | 意外的内核错误 | 配置值格式错误，通过了 parse 但在后续处理中失败 |

**重要提示**：不要解析 `error.message` 用于控制流。始终根据 `error.code` 进行分支判断。`message` 字段是人类可读的上下文。

### `cause` 字段（高级）

底层的 `ApiError` 结构体还有一个 `cause` 字段，携带原始的 `ConfigError` 显示字符串。此字段不包含在 `EnvelopeError` 有线格式中（有线格式仅包含 `code`、`message` 和 `field_path`）。然而，配置验证错误的 `message` 字段包含类似 `"config validation failed"` 的摘要 -- 详细的 cause 仅在内核日志或 CLI 输出中可用。

对于 GUI 用途，`code` 和 CLI 验证输出（如果你通过 shell 调用 `zero validate`）提供了足够的信息来将错误映射到表单字段。

## 常见失败模式

### 1. 协议未编译进二进制文件

**场景**：配置引用了构建时未包含的协议。

**示例配置**：
```json
{
  "inbounds": [
    {
      "tag": "trojan-in",
      "listen": { "address": "127.0.0.1", "port": 443 },
      "protocol": {
        "type": "trojan",
        "password": "secret",
        "tls": { "cert_path": "certs/cert.pem", "key_path": "certs/key.pem" }
      }
    }
  ],
  "outbounds": [
    { "tag": "direct-out", "protocol": { "type": "direct" } }
  ],
  "route": { "final": { "type": "direct" } }
}
```

**API 响应**：
```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "error": {
    "code": "feature_disabled",
    "message": "requested feature is not enabled in this build"
  }
}
```

**CLI 等价输出**（`zero validate`）：
```
Error: inbound `trojan-in` uses protocol `trojan` but this binary was built
without Cargo feature `trojan`
```

**GUI 指导**：在提供协议选项下拉菜单之前，始终调用 `GET /api/v1/capabilities`（HTTP）或查询 `{"capabilities":{}}`（IPC）。响应中包含 `capabilities.protocols[]` 列出了所有已编译的协议。仅显示该列表中存在的协议。如果用户导入了一个包含不受支持协议的配置，将 `feature_disabled` 映射为用户友好的消息："此配置使用了 'trojan'，但当前 Zero 构建中不可用。"

### 2. 无效的 JSON 语法

**场景**：配置值包含 JSON 语法错误（缺少逗号、尾部逗号、括号不匹配等）。

**示例**（通过 HTTP 命令传递的格式错误的 JSON）：
```json
{
  "method": "config.validate",
  "params": {
    "config": { "inbounds": [ { "tag": "socks-in" } ] }
  }
}
```
...但实际上发送了格式错误的 JSON 作为原始请求体。

**API 响应**（如果命令请求本身可解析，但内部配置值在 `serde_json::to_string` 然后 `RuntimeConfig::parse` 过程中出现语法问题）：
```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "error": {
    "code": "invalid_argument",
    "message": "config validation failed"
  }
}
```

**GUI 指导**：始终在发送到 API 之前在客户端验证 JSON 语法。使用 `JSON.parse()` 或等效方法。API 会拒绝无效的 JSON，但错误消息是通用的。尽早捕获语法错误能提供更好的用户体验。

常见 JSON 陷阱：
- 最后一个数组/对象元素后的尾部逗号
- 未加引号的属性名
- 注释（`//` 或 `/* */`）—— JSON 不支持注释
- 字符串使用单引号而非双引号

### 3. 缺少必需的顶层节

**场景**：配置对象缺少 `route`。

**示例配置**：
```json
{
  "inbounds": [
    { "tag": "socks-in", "listen": { "address": "127.0.0.1", "port": 7890 }, "protocol": { "type": "socks5" } }
  ],
  "outbounds": [
    { "tag": "direct-out", "protocol": { "type": "direct" } }
  ]
}
```

`route` 是 `RuntimeConfig` 中的必填字段（没有 `#[serde(default)]`）。serde 反序列化会因缺少字段而失败。

**API 响应**：
```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "error": {
    "code": "invalid_argument",
    "message": "config validation failed"
  }
}
```

**CLI 等价输出**（`zero validate`）：
```
Error: failed to parse config: missing field `route` at line X column Y
```

**GUI 指导**：顶层 `RuntimeConfig` 需要以下节：

| 字段 | 必需 | 省略时的默认值 |
|-------|----------|-------------------|
| `inbounds` | 否 | `[]` |
| `outbounds` | 否 | `[]` |
| `outbound_groups` | 否 | `[]` |
| `route` | **是** | -- |
| `mode` | 否 | `{ "type": "rule" }` |
| `runtime` | 否 | `{ "udp_upstream_idle_timeout_seconds": 30 }` |
| `api` | 否 | `{}` |

始终确保 `route` 存在。提供一个默认值：
```json
{ "route": { "rules": [], "final": { "type": "direct" } } }
```

### 4. 空 Tag 或重复 Tag

**场景**：tag 字段为空或仅含空白字符，或者两个条目共享相同的 tag。

**示例配置**（空 tag）：
```json
{
  "inbounds": [
    { "tag": "", "listen": { "address": "127.0.0.1", "port": 7890 }, "protocol": { "type": "socks5" } }
  ],
  "outbounds": [
    { "tag": "direct-out", "protocol": { "type": "direct" } }
  ],
  "route": { "final": { "type": "direct" } }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: inbounds[0]: `inbound` tag must not be empty
```

**重复的 inbound 监听地址**：
```json
{
  "inbounds": [
    { "tag": "a", "listen": { "address": "127.0.0.1", "port": 7890 }, "protocol": { "type": "socks5" } },
    { "tag": "b", "listen": { "address": "127.0.0.1", "port": 7890 }, "protocol": { "type": "http" } }
  ]
}
```

**CLI 等价输出**：
```
Error: duplicate inbound listen endpoint `127.0.0.1:7890`; use `mixed` for multi-protocol same-port listening
```

**API 响应**（两种情况）：
```json
{
  "api_id": "zero.api.v1",
  "ok": false,
  "error": {
    "code": "invalid_argument",
    "message": "config validation failed"
  }
}
```

**GUI 指导**：
- Tag 必须是非空字符串。在客户端验证此项。
- Inbound tag 在 inbounds 中必须唯一。
- Outbound tag 在 outbounds 中必须唯一。
- Outbound 分组 tag 在分组中必须唯一。
- 跨 inbounds 的重复 `address:port` 会被拒绝。使用 `type: "mixed"` 在相同端口上提供多个协议。
- 同一 tag 不能同时出现在 outbounds 和 outbound groups 中（内部它们共享路由目标命名空间）。

### 5. Shadowsocks 无效的 Cipher 名称

**场景**：Shadowsocks 的 `cipher` 字段包含无法识别的值。

**示例配置**：
```json
{
  "inbounds": [
    {
      "tag": "ss-in",
      "listen": { "address": "127.0.0.1", "port": 8388 },
      "protocol": { "type": "shadowsocks", "password": "secret", "cipher": "aes-128-ctr" }
    }
  ],
  "outbounds": [{ "tag": "d", "protocol": { "type": "direct" } }],
  "route": { "final": { "type": "direct" } }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: inbounds[0] `ss-in`: `shadowsocks` inbound cipher `aes-128-ctr` is not valid; expected one of: aes-128-gcm, aes-256-gcm, chacha20-ietf-poly1305, 2022-blake3-aes-128-gcm, 2022-blake3-aes-256-gcm, 2022-blake3-chacha20-poly1305
```

**有效的 Shadowsocks cipher 名称**：

| Cipher | 密钥长度（2022） |
|--------|----------------------|
| `aes-128-gcm` | -- |
| `aes-256-gcm` | -- |
| `chacha20-ietf-poly1305` | --（省略时的默认值） |
| `2022-blake3-aes-128-gcm` | 16 字节 |
| `2022-blake3-aes-256-gcm` | 32 字节 |
| `2022-blake3-chacha20-poly1305` | 32 字节 |

**GUI 指导**：从此固定列表的下拉菜单中提供 cipher 选择。不允许自由文本输入。

### 6. Shadowsocks 2022 无效的密码/密钥格式

**场景**：使用了 2022-blake3 cipher 但密码不是有效的标准 base64 或解码后长度不正确。

**示例**（密钥长度错误）：
```json
{
  "protocol": {
    "type": "shadowsocks",
    "password": "dG9vLXNob3J0",
    "cipher": "2022-blake3-aes-256-gcm"
  }
}
```
`dG9vLXNob3J0` 解码后为 9 字节，但 `2022-blake3-aes-256-gcm` 需要 32 字节。

**CLI 等价输出**：
```
Error: invalid inbound: inbounds[0] `ss-in`: `shadowsocks` inbound 2022 password decoded length must be 32 bytes, got 9
```

**示例**（不是有效的 base64）：
```json
{
  "protocol": {
    "type": "shadowsocks",
    "password": "!!!not-base64!!!",
    "cipher": "2022-blake3-aes-128-gcm"
  }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: inbounds[0] `ss-in`: `shadowsocks` inbound 2022 password must be standard base64 key material
```

**按 cipher 的密码要求**：

| Cipher 家族 | 密码格式 | 验证 |
|---------------|----------------|------------|
| 非 2022（`aes-*-gcm`、`chacha20-ietf-poly1305`） | 任意非空字符串 | 不能为空 |
| `2022-blake3-aes-128-gcm` | 标准 base64，解码后 16 字节；可为冒号分隔的身份密钥链（最后一段为用户 PSK） | 必须从 base64 解码为恰好 16 字节 |
| `2022-blake3-aes-256-gcm` | 标准 base64，解码后 32 字节；AES 变体支持冒号分隔 | 必须从 base64 解码为恰好 32 字节 |
| `2022-blake3-chacha20-poly1305` | 标准 base64，解码后 32 字节；ChaCha20 不支持冒号分隔 | 必须从 base64 解码为恰好 32 字节 |

**GUI 指导**：当用户选择 2022 cipher 时，将密码字段切换为 base64 验证器。显示期望的解码字节长度（16 或 32）。对于非 2022 cipher，仅要求非空密码字符串。

### 7. 无效的端口号 / 缺少服务器地址

**场景**：outbound 的 `port: 0`、`server` 为空，或端口值超出有效的 u16 范围（0-65535）。

**示例配置**：
```json
{
  "outbounds": [
    { "tag": "bad", "protocol": { "type": "trojan", "server": "", "port": 0, "password": "secret" } }
  ]
}
```

**CLI 等价输出**：
```
Error: invalid outbound: outbounds[0] `bad`: `trojan` outbound requires a non-empty `server`
```
（修复 server 后）：
```
Error: invalid outbound: outbounds[0] `bad`: `trojan` outbound `port` must be greater than 0
```

**按 outbound 类型的约束**：
- `server` 必须非空（所有链式 outbounds：socks5、vless、hysteria2、shadowsocks、trojan、vmess、mieru）
- `port` 必须大于 0（同上列表）
- 端口必须是有效的 u16（0-65535），由 serde 反序列化强制执行

**GUI 指导**：为端口字段使用 input type="number" 并设置 min="1" max="65535"。要求 server/host 字段为非空字符串。

### 8. TLS 证书/密钥文件未找到

**场景**：TLS 的 `cert_path` 或 `key_path` 已配置但为空，或文件在给定路径下不存在。空路径在验证时被捕获；缺少文件在实际 TLS 设置期间被捕获（而非配置解析时）。

**示例**（空路径，验证时捕获）：
```json
{
  "protocol": {
    "type": "vless",
    "users": [{ "id": "11111111-2222-3333-4444-555555555555" }],
    "tls": { "cert_path": "", "key_path": "certs/key.pem" }
  }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: inbounds[0] `vless-in`: `vless tls.cert_path` must not be empty
```

**约束**：
- 如果存在 TLS 块，`cert_path` 和 `key_path` 必须非空
- 文件存在性不在配置验证时检查（仅在监听器或 outbound 连接启动的运行时检查）
- Trojan inbound 需要同时配置 `tls`、`cert_path` 和 `key_path`
- VMess inbound 需要同时配置 `tls`、`cert_path` 和 `key_path`

**GUI 指导**：
- 在客户端验证 TLS 证书/密钥路径为非空字符串
- 为证书/密钥选择提供文件选择器对话框
- 应用配置后，监控 `engine.warning` 事件以获取运行时 TLS 错误

### 9. 不支持的传输组合

**场景**：配置尝试组合不兼容的传输方式。

**示例**（VLESS Reality 搭配 WebSocket）：
```json
{
  "protocol": {
    "type": "vless",
    "users": [{ "id": "11111111-2222-3333-4444-555555555555" }],
    "reality": { "private_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
    "ws": { "path": "/vless" }
  }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: `vless` inbound `reality` supports raw TCP only, not `ws`
```

**不兼容的 VLESS 传输组合**：
- `reality` 不能与以下组合：`tls`、`ws`、`grpc`、`h2`、`http_upgrade`、`quic`
- `tls` 和 `reality` 互斥

**不兼容的 VMess 传输组合**：
- `ws` 和 `grpc` 不能同时设置（互斥）

**GUI 指导**：为传输选择使用单选按钮或互斥开关。当用户选择 Reality 时，禁用/隐藏非 TCP 传输选项。当用户选择 WebSocket 时，禁用/隐藏 gRPC 选项（反之亦然）。

对于 VLESS：
- Reality 支持：仅原始 TCP
- TLS/WS 支持：原始 TCP、WebSocket、gRPC、H2、HTTPUpgrade、QUIC
- WS 和 TLS 可组合为 WSS（WebSocket Secure）

### 10. 无效的路由规则语法

**场景**：路由规则条件格式错误或引用了未定义的 outbound。

**示例**（未定义的 outbound 目标）：
```json
{
  "route": {
    "rules": [
      {
        "condition": { "type": "domain", "values": ["example.com"] },
        "action": { "type": "route", "outbound": "non-existent" }
      }
    ],
    "final": { "type": "direct" }
  }
}
```

**CLI 等价输出**：
```
Error: route or mode references undefined target tag `non-existent`
```

**示例**（空的 domain 条件值）：
```json
{
  "condition": { "type": "domain", "values": [] },
  "action": { "type": "direct" }
}
```

**CLI 等价输出**：
```
Error: invalid rule condition: `domain` condition requires at least one value
```

**常见路由验证错误**：

| 错误 | 触发条件 |
|-------|---------|
| 未定义的目标 tag | `route` 动作引用了不存在的 outbound/分组 tag |
| 未定义的规则集 tag | `rule_set` 条件引用了不在 `route.rule_sets` 中的 tag |
| 空的条件值 | `domain`、`domain_keyword`、`domain_regex`、`ip`、`sni`、`geoip` 的 `values` 数组为空 |
| 空的嵌套条件 | `and` / `or` 的 `items` 数组为空 |
| 空的路由动作 outbound | `route` 动作的 `outbound` 字符串为空 |
| 循环分组引用 | Outbound 分组成员形成循环 |
| 分组引用未定义目标 | 分组 `outbounds`/`proxies` 列表包含非已定义 outbound 或分组的 tag |
| url_test 使用非 http URL | `url_test` 的 `url` 必须以 `http://` 开头 |
| url_test 间隔为零 | `url_test` 的 `interval_seconds` 必须大于 0 |
| relay 代理数少于 2 | `relay` 分组至少需要 2 个代理 |
| selector 默认/选中值不在 outbounds 中 | `selector` 的 `default` 或 `selected` 必须为其 `outbounds` 之一 |

**路由规则 GUI 指导**：
- `route` 动作中的 `outbound` 字段必须引用已存在的 tag。提供从 outbound tags 和 outbound 分组 tags 并集填充的下拉菜单。
- 规则集 `tag` 引用必须存在于 `route.rule_sets` 中。提供下拉菜单。
- 每种条件类型有其自身的 `values` 要求。在客户端验证适用情况下的非空数组。
- 当添加引用其他分组的 outbound 分组时，在提交到 API 前在客户端检查循环引用。

### 11. 无效的 UUID 格式

**场景**：VLESS 或 VMess 用户 `id` 不是有效的规范 UUID 或 32 位十六进制数字。

**示例**：
```json
{
  "protocol": {
    "type": "vless",
    "users": [{ "id": "not-a-uuid" }]
  }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: inbounds[0] `vless-in`: `vless` inbound user `id` must be a canonical UUID or 32 hex digits
```

**GUI 指导**：使用 UUID 输入字段或在客户端生成 UUID（例如 `crypto.randomUUID()`）。在提交前验证格式：`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`（36 字符，在第 8、13、18、23 位有 4 个连字符）或恰好 32 位十六进制数字。

### 12. VLESS Reality 密钥格式

**场景**：Reality 的 `private_key`（inbound）或 `public_key`（outbound）不是有效的 32 字节 base64url 值。

**示例**：
```json
{
  "protocol": {
    "type": "vless",
    "users": [{ "id": "11111111-2222-3333-4444-555555555555" }],
    "reality": { "private_key": "abc123", "short_ids": ["abcd"] }
  }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: `vless` inbound `reality.private_key` must be a 32-byte base64url value without padding
```

**约束**：
- Reality 密钥必须是 base64url 编码（字母表：A-Z、a-z、0-9、-、_）
- 不允许 `=` 填充
- 必须解码为恰好 32 字节（X25519 密钥）
- `short_id` / `short_ids`：最多 16 个十六进制字符，仅限十六进制数字
- `cipher_suites` 如果指定，必须来自：`TLS_AES_128_GCM_SHA256`、`TLS_AES_256_GCM_SHA384`、`TLS_CHACHA20_POLY1305_SHA256`

**GUI 指导**：提供 Reality 密钥的生成器或验证器。显示期望的格式（32 字节 base64url，无填充）。对于 `short_id`，强制最多 16 个十六进制字符。

### 13. WebSocket 保留头部

**场景**：自定义 WebSocket 头部包含了为 WebSocket 握手保留的头部名称。

**示例**：
```json
{
  "ws": {
    "path": "/vless",
    "headers": { "Host": "evil.com", "Connection": "Upgrade" }
  }
}
```

**CLI 等价输出**：
```
Error: invalid inbound: `vless ws.headers` contains reserved header `Host` which is managed by WebSocket handshake
```

**保留的头部名称**（大小写不敏感）：
- `host`
- `connection`
- `upgrade`
- `sec-websocket-key`
- `sec-websocket-version`
- `sec-websocket-protocol`
- `sec-websocket-extensions`
- `sec-websocket-accept`

**GUI 指导**：在自定义头部编辑器中阻止这些头部名称。大小写不敏感匹配："Host"、"HOST"、"host" 均被拒绝。

### 14. SOCKS5 认证约束

**场景**：SOCKS5 outbound 认证配置不正确。

**规则**：
- 既无 `username` 也无 `password`：无认证模式（适用于未认证的上游）
- 同时有 `username` 和 `password`：认证模式
- 仅有 `username`（无 `password`）：**无效** -- 拒绝并提示"requires both `username` and `password`, or neither"
- 仅有 `password`（无 `username`）：实际上会被规范化，`username` 默认为 `password`
- 每个凭据部分必须为 1-255 字节

**GUI 指导**：如果用户输入凭据，要求填写两个字段（或明确说明两者都留空表示无认证）。验证每个字段为 1-255 字节。

### 15. DNS 配置约束

**场景**：DNS 相关字段具有无效值。

**CLI 示例**（空的 UDP 服务器地址）：
```
Error: invalid dns config: dns server 0: udp address must not be empty
```

**CLI 示例**（无效的 fake_ip CIDR）：
```
Error: invalid dns config: `dns.fake_ip.cidr` is not a valid CIDR: not-a-cidr
```

**约束**：
- `dns.servers[]` 中 `udp` 和 `dot`：`address` 不能为空
- `dns.servers[]` 中 `doh`：`url` 不能为空
- `dns.cache.max_entries` 必须大于 0（如果配置了 cache）
- `dns.fake_ip.cidr` 必须是有效的 CIDR；前缀必须允许足够的地址（IPv4 最大 /30，IPv6 最大 /120）
- `dns.fake_ip.ttl_seconds` 必须大于 0
- `dns.routes[].domain` 不能为空
- `dns.routes[].server` 必须是 "system" 或有效的服务器索引

## GUI 实现指导

### 将错误映射到表单字段

当前 API 不会在配置验证错误的错误信封中返回 `field_path`（配置错误始终使用 `code: "invalid_argument"` 和 `message: "config validation failed"`）。详细的 cause 在 CLI 输出或内核日志中。

对于 GUI 字段级验证，使用以下策略：

1. **客户端验证优先**：在调用 API 之前验证一切可验证的内容。检查非空 tag、有效端口范围、UUID 格式、cipher 名称等。上述记录的约束均可在客户端检查。

2. **基于能力的协议列表**：启动时查询 `{"capabilities":{}}`。仅展示 `capabilities.protocols[]` 中的协议。`protocol` 和 `feature` 字段告诉你协议名称及其 cargo feature 门控。

3. **结构化错误消息解析**：当 `config.validate` 返回错误时，CLI 输出格式遵循以下模式：
   - `invalid inbound: inbounds[&lt;index&gt;] &lt;tag&gt;: &lt;detail&gt;`
   - `invalid outbound: outbounds[&lt;index&gt;] &lt;tag&gt;: &lt;detail&gt;`
   - `invalid outbound group: outbound_groups[&lt;index&gt;]: &lt;detail&gt;`
   - `invalid rule condition: &lt;detail&gt;`
   如果你通过 shell 调用 `zero validate`（或解析内核端的 `cause` 字符串），可以提取索引和 tag 来高亮出问题的条目。

### 预验证检查清单

在调用 `config.validate` 之前，在客户端确认以下事项：

- [ ] 所有 `tag` 字段均为非空字符串
- [ ] 同一命名空间内无重复 tag（inbounds、outbounds、groups）
- [ ] 跨 inbounds 无重复的 `address:port` 绑定
- [ ] `route` 节存在且 `final` 动作已设置
- [ ] 所有 `route` 动作的 `outbound` 引用指向已存在的 tag
- [ ] 所有 `rule_set` 条件的 `tag` 引用指向已定义的规则集
- [ ] 端口号在 1-65535 范围内
- [ ] Outbound 的 `server` 字段非空
- [ ] 协议 `type` 值与编译进的能力匹配
- [ ] JSON 语法有效（使用 `JSON.parse` 或等效方法解析）
- [ ] Cipher 名称匹配该协议允许的列表
- [ ] 2022 Shadowsocks 密码是正确长度的有效 base64
- [ ] UUID 是规范格式（36 字符带连字符）或 32 位十六进制数字
- [ ] Reality 密钥是 32 字节 base64url 无填充
- [ ] WebSocket 自定义头部不使用保留名称
- [ ] `ws` 和 `grpc` 不同时设置（VMess、VLESS）
- [ ] TLS 配置时 `cert_path` 和 `key_path` 非空
- [ ] Reality 不与其他传输方式组合（VLESS）
- [ ] Trojan 和 VMess inbound 已配置 TLS
- [ ] `url_test` URL 以 `http://` 开头且间隔 > 0
- [ ] `relay` 分组至少有 2 个代理
- [ ] 分组成员引用不形成循环
- [ ] 速率限制字段（`up_bps`、`down_bps`）为非负数
- [ ] `idle_timeout_secs` 如果设置则大于 0

### 向用户呈现错误

**应该做：**
- 显示摘要计数："发现 3 个验证错误"
- 列出每个错误及其影响的条目名称和清晰的描述
- 在表单中高亮出问题的字段（使用基于索引的映射）
- 在可能的情况下提供自动修复（例如生成 UUID、删除 JSON 中的尾部逗号）
- 在可折叠的"详情"区域显示完整的 CLI 验证输出

**不应该做：**
- 不要向最终用户展示原始 JSON 解析错误（改为"配置语法错误，位置 X"）
- 不要将 `error.code` 字符串作为唯一面向用户的消息（它用于程序化处理）
- 不要静默丢弃错误 -- 始终将验证失败呈现给用户
- 不要在 `config.validate` 通过之前允许 `config.apply`

### 带影响预览的两阶段应用

配置编辑的推荐流程：

```
1. 用户在 GUI 表单中编辑配置
2. GUI 序列化为 JSON 草稿
3. GUI 使用草稿调用 config.validate
   → 错误时：显示错误，禁用"应用"按钮
   → 成功时：启用"应用"按钮，显示摘要
4. 用户点击"应用"
5. GUI 使用相同草稿调用 config.apply
   → 错误时：显示错误（此时如果 validate 已通过，这种情况很少见）
   → 成功时：重新查询 config + runtime 以确认状态
```

成功执行 `config.apply` 后：
- 路由规则、分组和 mode 立即热重载
- Inbounds 和 outbounds 的添加/删除/变更可能需要重启
- 查询 `config` 以确认内核的当前配置符合预期
- 查询 `runtime` 以查看新配置下的活跃会话

### 能力发现（必要步骤）

在提供协议选项之前，始终调用 `GET /api/v1/capabilities`（HTTP）或查询 `{"capabilities":{}}`（IPC）。响应包括：

```json
{
  "protocols": [
    { "protocol": "socks5", "feature": "socks5", "inbound": true, "outbound": true },
    { "protocol": "vless", "feature": "vless", "inbound": true, "outbound": true }
  ],
  "features": ["status-api", "config_snapshot"],
  "adapters": [{ "kind": "in_process", "enabled": true }]
}
```

使用 `protocols[*].inbound` 和 `protocols[*].outbound` 来填充按方向的协议下拉菜单。不要假设某个协议仅因存在于文档中就可用。

### 完整错误参考表

| 错误场景 | `error.code` | 关键约束 |
|---------------|-------------|----------------|
| 功能未编译 | `feature_disabled` | 检查 `capabilities.protocols[]` |
| JSON 语法错误 | `invalid_argument` | `serde_json` 解析失败 |
| 缺少必填字段 | `invalid_argument` | `route`、inbound tags 等 |
| 空 tag | `invalid_argument` | 检查前去除空白字符 |
| 重复 tag | `invalid_argument` | Tag 在每个命名空间中必须唯一 |
| 重复监听地址 | `invalid_argument` | 使用 `mixed` 支持同端口多协议 |
| 无效的 cipher 名称 | `invalid_argument` | 对照允许的 cipher 列表检查 |
| 无效的密码/密钥 | `invalid_argument` | 2022 cipher 的 base64 解码长度 |
| 无效的 UUID 格式 | `invalid_argument` | 规范 UUID 或 32 位十六进制数字 |
| 无效的 Reality 密钥 | `invalid_argument` | Base64url，解码后 32 字节 |
| Port = 0 | `invalid_argument` | 必须为 1-65535 |
| 空的服务器地址 | `invalid_argument` | 要求非空字符串 |
| 空的 TLS 证书/密钥路径 | `invalid_argument` | 要求非空字符串 |
| Reality + 传输组合 | `invalid_argument` | Reality 仅支持原始 TCP |
| WS + gRPC 同时设置 | `invalid_argument` | 互斥 |
| 未定义的路由目标 | `invalid_argument` | Tag 必须存在于 outbounds 或 groups 中 |
| 未定义的规则集 tag | `invalid_argument` | Tag 必须存在于 `route.rule_sets` 中 |
| 空的条件值 | `invalid_argument` | 至少需要一个值 |
| 保留的 WS 头部 | `invalid_argument` | 阻止 Host、Connection、Upgrade 等 |
| SOCKS5 认证不匹配 | `invalid_argument` | 同时有或同时无 |
| 循环分组引用 | `invalid_argument` | 检测分组成员关系中的循环 |
| url_test 非 http URL | `invalid_argument` | URL 必须以 `http://` 开头 |
| relay 代理数 < 2 | `invalid_argument` | 至少需要 2 个代理 |
| 无效的 CIDR（fake_ip） | `invalid_argument` | 有效的 CIDR，合理的前缀 |
| 无效的 DNS 服务器 | `invalid_argument` | 非空的 address/url |

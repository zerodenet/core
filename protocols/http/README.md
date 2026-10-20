# HTTP Proxy

> RFC 9110 / RFC 9112 | Crate: `http`

该 crate 实现 Zero 的 HTTP 入站代理能力，同时支持 `CONNECT` 隧道和标准 absolute-form HTTP 正向代理请求。

## 协议来源

| 项目 | 来源 |
|------|------|
| HTTP 语义 | [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110) |
| HTTP/1.1 消息语法与 request-target | [RFC 9112](https://www.rfc-editor.org/rfc/rfc9112) |
| 本实现 | `http` crate |

## 功能对齐状态

| 特性 | 状态 |
|------|------|
| `CONNECT` authority-form 解析 | ✅ |
| `200 Connection Established` 响应 | ✅ |
| absolute-form `http://` GET/POST 等请求解析 | ✅ |
| 目标地址提取并进入统一路由流程 | ✅ |
| absolute-form 到 origin-form 请求行改写 | ✅ |
| `http` 与 `mixed` 入站复用 | ✅ |
| HTTPS 隧道 | ✅，通过 `CONNECT` |
| absolute-form `https://` 请求 | 不支持；客户端应使用 `CONNECT` |

## 入站流程

```text
accept request
  -> parse CONNECT authority or absolute HTTP URI
  -> CONNECT: return destination facts, host mints one real Session identity
  -> forward request: retain message request metadata
  -> evaluate route rules
  -> connect selected upstream
  -> CONNECT: reply 200 and relay tunnel
  -> forward request: replay origin-form request head and relay response
```

## 关键文件

```text
src/lib.rs       — crate root and public exports
src/parse.rs     — request-line, authority, and absolute URI parsing
src/inbound.rs   — request acceptance and responses
src/inbound/command.rs — CONNECT facts and compatibility request conversion
src/component.rs — runtime-gated standard CONNECT reply over the original stream
```

`runtime` enables the standard Stream reply and Tokio bridge. The default feature set keeps parsing and validation independent of a runtime. Production HTTP and mixed CONNECT use the same host admission, routing, accounting and relay path; the reply is emitted after local upstream readiness. HTTP forward messages retain their existing message executor. The legacy request API remains available for Rust callers.

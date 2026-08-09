# Trojan Inbound

对应 `protocols/trojan/src/inbound.rs` — `TrojanInbound` 以及协议内部 accept/session route glue。

## TrojanInbound

实现 `InboundProtocol` trait，通过 `serve_inbound()` 统一管线。

### TCP 入站流程

1. TLS 握手完成（必须）
2. 读取 Trojan request：`[PASSWORD_HASH][CRLF][CMD][ATYP][ADDR][PORT][CRLF]`
3. 验证 password hash
4. 将命中的密码映射到稳定 `principal_key` 和访问策略
5. 返回协议内部 accept state，再投影为带 `SessionAuth` 的 session

密码验证流程：
```rust
// read_password 读取到第一个 CRLF 为止
let password = shared::read_password(&mut stream).await?;
// 验证 SHA224(password) 是否匹配
if sha224(password) != expected_password_hash {
    return Err(TrojanError::AuthFailed);
}
```

## Inbound 配置

```json
{
  "tag": "trojan-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "trojan",
    "users": [{
      "password": "your-password",
      "principal_key": "account:1001",
      "up_bps": 10000000,
      "down_bps": 50000000
    }],
    "tls": {
      "cert_path": "certs/fullchain.pem",
      "key_path": "certs/privkey.pem"
    }
  }
}
```

- `users`: 面板管理与多用户节点使用；每项包含 secret `password`、稳定身份和可选用户限速
- `password`: 兼容静态单用户节点；不能与 `users` 同时配置
- `tls`: 必需，Trojan 必须使用 TLS
## 边界说明

用户集合由共享 profile 原子替换，现有 listener 无需解绑端口。Raw Trojan accept state is module-private.

目标为 `v1.mux.cool:666` 的 TCP 请求会进入协议自有 Mux.Cool server，并通过中立 `InboundMuxStreamRoute` 把 TCP/UDP 子流交给通用运行时。普通 TCP 和 CMD_UDP 仍走原路径。`mux_response_backlog_frames` 与 `mux_response_backlog_bytes` 在入站限制回程排队，溢出只终止对应子流并释放预算。
Downstream glue should use `accept_session()`, `accept_client()`, or the route helpers
instead of depending on handshake-local accept models.

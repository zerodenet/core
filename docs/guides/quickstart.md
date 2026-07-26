# 快速开始

5 分钟从零到代理跑起来，并通过控制面管理。

## 1. 构建

```bash
git clone https://github.com/zerodenet/zero.git
cd zero
cargo build --release
```

## 2. 最小配置

`config.json`：

```json
{
  "inbounds": [
    {
      "tag": "socks-in",
      "listen": { "address": "127.0.0.1", "port": 1080 },
      "protocol": { "type": "mixed" }
    }
  ],
  "outbounds": [
    { "tag": "direct", "protocol": { "type": "direct" } },
    {
      "tag": "proxy",
      "protocol": {
        "type": "vless",
        "server": "your-server.com",
        "port": 443,
        "id": "your-uuid",
        "tls": { "server_name": "your-server.com" }
      }
    }
  ],
  "mode": { "type": "global", "outbound": "proxy" },
  "route": {
    "rules": [],
    "final": { "type": "direct" }
  },
  "runtime": {
    "log": { "level": "info" }
  }
}
```

## 3. 启动

```bash
./target/release/zero run config.json
```

输出：

```
engine started  build_id=<build-id>
loaded proxy configuration  config=config.json
ipc server ready  socket=/home/user/.zero/control.sock
```

代理已运行。IPC socket 自动创建在 `~/.zero/control.sock`。

## 4. 验证

```bash
# CLI 查状态
./target/release/zero status

# curl 查状态（HTTP 在 status-listen 启用时可用）
curl -s http://127.0.0.1:9090/api/v1/runtime

# 设浏览器 SOCKS5 代理到 127.0.0.1:1080
```

## 5. 带控制面的完整启动

```bash
# HTTP + IPC 双通道
./target/release/zero run --status-listen 127.0.0.1:9090 config.json

# 实时事件流
./target/release/zero events

# 切换节点
./target/release/zero select proxy direct

# 切换模式
./target/release/zero mode rule
./target/release/zero mode global proxy

# 校验配置
./target/release/zero validate config.json

# 热重载配置
./target/release/zero reload config.json
```

## 6. 常见场景

### 分流路由

```json
{
  "mode": { "type": "rule" },
  "route": {
    "rules": [
      {
        "condition": { "type": "domain", "values": ["geosite:cn"] },
        "action": { "type": "direct" }
      }
    ],
    "final": { "type": "route", "outbound": "proxy" }
  }
}
```

### 故障转移

```json
{
  "outbound_groups": [
    {
      "tag": "fallback-proxy",
      "type": "fallback",
      "outbounds": ["server-a", "server-b", "direct"]
    }
  ]
}
```

### 延迟选优

```json
{
  "outbound_groups": [
    {
      "tag": "auto",
      "type": "url_test",
      "outbounds": ["server-a", "server-b"],
      "url": "http://www.gstatic.com/generate_204",
      "interval_seconds": 300
    }
  ]
}
```

### 链式代理

```json
{
  "outbound_groups": [
    {
      "tag": "hk-us",
      "type": "relay",
      "proxies": ["hk-vless", "us-socks5"]
    }
  ]
}
```

### 负载均衡

```json
{
  "outbound_groups": [
    {
      "tag": "lb",
      "type": "load_balance",
      "outbounds": ["server-a", "server-b", "server-c"],
      "strategy": "round_robin"
    }
  ]
}
```

## 7. 快速集成

| 集成对象 | 推荐入口 | 覆盖能力 |
|----------|----------|----------|
| 本地 GUI 客户端 | [GUI 接入指南](gui-integration.md) | IPC/HTTP 查询、命令、实时 flow 与状态恢复 |
| 外部控制器 | [Connector 接入指南](connector-integration.md) | Zero API/gRPC 管理与 Webhook 事件回传 |

外部控制器通过 Zero HTTP/gRPC Control API 执行查询、命令和 `config.apply`；需要节点主动回传时，由中心在 `api.event_sinks` 注册完整 Webhook URL，Connector 可靠投递 `zero.event.v1`。订阅、套餐和支付仍由外部系统自身实现。

## 下一步

- [完整配置参考](../control-plane-api/configuration.md)
- [GUI 接入指南](gui-integration.md)
- [Connector 接入指南](connector-integration.md)
- [控制面 API 参考](../control-plane-api/)

# 构建特性

Zero 使用 Cargo features 来控制哪些能力子集被包含在编译后的二进制文件中，允许按需裁剪二进制大小和依赖范围。

## 预设

| 预设 | 包含内容 | 适用场景 |
|--------|---------|----------|
| `default` | `full` + `status-api` | 客户端本地使用 |
| `full` | 所有入站/出站协议 + DNS | 完整代理节点 |

```bash
# 默认构建（客户端场景，无需 connector）
cargo build --release

# 等效命令
cargo build --release --features full,status-api
```

## 入站协议

每个入站协议独立受 feature gate 控制，可按需裁剪。

| Feature | 协议 | 额外依赖 |
|---------|------|----------|
| `socks5` | SOCKS5 入站 | -- |
| `http` | HTTP CONNECT 入站 | -- |
| `mixed` | Mixed 入站（同端口 SOCKS5 TCP/UDP + HTTP CONNECT TCP） | 隐含 `socks5` + `http` |
| `vless` | VLESS 入站 | TLS / Reality / WebSocket / gRPC / H2 / HTTP Upgrade / XHTTP 等传输 |
| `hysteria2` | Hysteria2 入站 | QUIC (quinn) |
| `shadowsocks` | Shadowsocks 入站 | AEAD 加密 + 2022-blake3 |
| `trojan` | Trojan 入站 | TLS |
| `vmess` | VMess 入站 | 实验性 AEAD 实现 |
| `mieru` | Mieru 入站 | XChaCha20-Poly1305 会话帧封装 |
| -- | `direct` 入站 | 始终编译，无需 feature gate（固定目标转发器） |
| -- | `tun` 入站 | 始终编译，无需 feature gate（虚拟网络接口：Linux ioctl、macOS utun socket、Windows Wintun） |

```bash
# 裁剪示例：仅 SOCKS5 + HTTP CONNECT
cargo build --release --no-default-features \
  --features socks5,http,status-api
```

## 出站协议

| Feature | 协议 | 额外依赖 |
|---------|------|----------|
| `socks5` | SOCKS5 出站 | -- |
| `vless` | VLESS 出站 | 与入站相同的传输栈 |
| `hysteria2` | Hysteria2 出站 | QUIC (quinn) |
| `shadowsocks` | Shadowsocks 出站 | 与入站相同的加密 |
| `trojan` | Trojan 出站 | TLS |
| `vmess` | VMess 出站 | 实验性 AEAD 实现；`cipher: auto` 被规范化为当前 AEAD 基线 |
| `mieru` | Mieru 出站 | TCP 和 UDP socks5-in-tunnel 路径 |

`direct` 和 `block` 出站始终可用，无需 feature gate——它们不需要协议实现。

## DNS

| Feature | 描述 |
|---------|------|
| `dns` | DNS 解析器、缓存、路由、Fake IP 和 UDP DNS 后端 |

> 当 `dns` 未启用时，DNS 退回到系统解析器（`tokio::net::lookup_host`）。

## 管控面（服务器部署）

以下 features 用于为 Zero 节点启用可选控制与主动通信能力，**不在默认 `full` 预设中**。

| Feature | 描述 | 隐含 |
|---------|------|------|
| `status-api` | 运行时控制端点和 selector 切换，包括 HTTP 状态 API | -- |
| `grpc-api` | gRPC 管控面端点 | `dep:zero-grpc` |
| `event-dispatcher` | 事件分发器：将 zero 事件投递到外部 sink 并暴露 sink 投递状态 | `dep:zero-connector` |
| `sink-jsonl` | JSON Lines 文件 sink（事件持久化） | `event-dispatcher` |
| `connector` | 通用 Webhook 事件投递；完整 URL 和请求 headers 由注册方提供 | `event-dispatcher` |

```bash
# 服务器构建（包含可选 Connector）
cargo build --release --features full,status-api,connector
```

**`connector` 依赖范围：**

- `event-dispatcher` -- 事件投递基础设施和 sink 投递状态
- `zero-connector` crate -- EventDispatcher、Webhook sink、重试、outbox 和 dead letter
- 不隐式启用 `status-api` 或 `grpc-api`；控制入口按部署需要单独选择
- 不包含中心 API、节点注册或外部控制器方言；外部系统通过 Zero API/gRPC 管理节点

## 客户端 vs 服务器

```
客户端场景：  full + status-api  （默认）
                  - 入站/出站协议
                  - DNS
                  - HTTP 状态端点（本地调试）

服务器场景：  + connector
                  - 中心通过 Zero API/gRPC 注册 Webhook
                  - 节点按 zero.event.v1 主动投递事件
```

## 与协议实现的关系

协议 crates 通过上述根 Cargo features 编译。协议在 workspace 中存在本身并不意味着与每个外部生态系统导出具有生产级兼容性。

机器可读的协议矩阵通过 `capabilities.protocols` 暴露。它记录当前二进制文件的当前 TCP、UDP、MUX、传输、兼容性基线和限制事实。`zero-api` 定义响应结构；代理运行时从已编译的协议清单中填充协议事实。参见 [protocol-capabilities.md](protocol-capabilities.md)。

| 协议 | Feature | 备注 |
|------|---------|------|
| VMess | `vmess` | 实验性 AEAD 实现。来自 Xray/Clash 导出的 `cipher: auto` 被规范化为当前 AEAD 基线 |
| Mieru | `mieru` | TCP/UDP 入站和出站基线可用；能力状态以运行时矩阵为准 |
| HTTP CONNECT 出站 | -- | 出站方向未实现 |

入站/出站 features 不对等是正常的——某些协议不需要相反方向。

## 二进制大小

二进制大小取决于目标平台、Rust 版本、链接器、调试信息、LTO 和 strip 设置。本文档不维护容易失真的固定数值；需要比较 feature 裁剪效果时，应在相同工具链和构建参数下生成基准。

---

# 内核原语

这些跨切面能力位于内核管道中，统一应用于所有 TCP 协议。

## 空闲超时

每个 TCP 中继都包裹在空闲超时中。如果配置的持续时间内任一方向都没有数据流动，会话将被干净地终止。

- **默认值**：300 秒（5 分钟）
- **配置**：`InboundConfig.idle_timeout_secs`（可选，按入站配置）
- **作用范围**：在 `serve_inbound()` 中通过 `tokio::time::timeout` 包裹 `protocol.relay()` 来应用
- **行为**：空闲超时不是错误——会话以其当前结果（`DirectRelayed` 或 `ChainedRelayed`）结束

## 出站健康 / 熔断器

`zero-engine` 为每个出站标签维护一个 `OutboundHealth` 跟踪器。在连接到任何出站（除 `direct` 和 `block` 外）之前，内核检查该出站是否健康。

- **失败阈值**：30 秒滑动窗口内 5 次失败
- **隔离时间**：60 秒——该出站被跳过，不接受所有新连接
- **探测**：隔离期满后，允许一个连接作为探测；成功则恢复健康，失败则重置冷却期
- **跟踪**：连接错误时调用 `record_outbound_failure()`，中继完成时调用 `record_outbound_success()`
- **作用范围**：适用于 fallback 组候选选择和所有链式出站连接
- **错误类型**：`EngineError::UnhealthyOutbound { tag }`——被视为连接失败，触发下一个 fallback 候选

## URL 域名重写

在路由之前应用的基于域名的 URL 重写。规则按首次匹配优先的方式执行；一旦某条规则匹配，不再评估后续规则。

- **配置**：`route.url_rewrite`（`UrlRewriteRule` 数组）
- **匹配类型**：
  - `from` -- 精确域名匹配
  - `from_regex` -- 正则表达式模式匹配，支持捕获组替换（`$1`、`$2` 等）
- **替换**：`to` 字段指定替换域名
- **HTTP 重定向**：`status_code` 字段（如 `302`）对 HTTP CONNECT 触发 HTTP 重定向响应；非 HTTP 协议静默重写
- **作用范围**：在 `serve_inbound()` 中进行路由查找之前应用；也在 HTTP CONNECT 自身处理程序中应用以进行即时重定向

```json
{
  "route": {
    "url_rewrite": [
      { "from": "old.example.com", "to": "new.example.com" },
      { "from_regex": "^(.+)\\.mirror\\.example\\.com$", "to": "$1.example.com" },
      { "from": "temp.example.com", "to": "permanent.example.com", "status_code": 301 }
    ]
  }
}
```

## 域名正则路由条件

路由条件类型 `domain_regex` 根据一个或多个正则表达式模式匹配目标域名。

- **配置**：`{ "type": "domain_regex", "values": ["^.*\\.google\\..*$", "^.*\\.youtube\\..*$"] }`
- **匹配**：模式在启动时编译一次（`regex::Regex`），然后在决策时与目标域名匹配
- **捕获组**：不用于路由——仅用于匹配。如需基于捕获的重写，使用 `url_rewrite.from_regex`
- **作用范围**：作为规则条件系统的一部分，可与 `and`/`or` 组合

```json
{
  "condition": { "type": "domain_regex", "values": ["^.*\\.google\\..*$"] },
  "action": { "type": "route", "outbound": "proxy" }
}
```

## GCRA 速率限制

使用通用信元速率算法（GCRA）按字节整形 TCP 与 UDP 流量。

- **配置**：`InboundProtocolConfig` 上的按入站 `up_bps` 和 `down_bps`（Hysteria2、Shadowsocks、Trojan）
- **按主体**：认证结果通过 `SessionAuth` 注入主体限制；VLESS、VMess、Trojan、Shadowsocks、Hysteria2 等协议统一复用该字段，主体限制优先于入站默认值
- **内核集成**：TCP 与 UDP ingress 都会先应用认证策略，再为未设置主体限制的会话填充入站默认值
- **共享算法**：协议中立的 `transport/rate_limit.rs` 负责可共享的 GCRA 时间线；Proxy 生命周期内的主体策略注册表按 `principal_key`、`policy_revision` 和双向速率取得同一组句柄
- **TCP 路径**：读取到的业务数据在写入前按共享上传/下载时间线准入；多个并发 TCP 会话共同消费同一主体策略带宽
- **UDP 路径**：首包、已有 flow 转发及 direct/upstream/chain 响应均取得同一主体策略的双向 GCRA 时间线，并与 TCP 会话聚合
- **清退协作**：主体撤销、策略变化或配额耗尽会唤醒正在等待速率令牌的 UDP 包，不会让会话清退卡在长时间定时器上
- **策略变化**：revision 或双向速率变化会建立新时间线；旧会话在确认式清退完成前继续持有旧策略，不能污染新策略带宽
- **突发容忍度**：每个主体策略方向保留 16 KB 余量，避免饿死小写入和大于单次突发窗口的 UDP 包
- **作用范围**：带 `principal_key` 的认证会话按 Zero 主体策略跨 TCP/UDP 聚合；没有主体身份的入站默认限速按会话独立执行，避免匿名客户端互相占用带宽

```json
{
  "tag": "hysteria2-in",
  "listen": { "address": "0.0.0.0", "port": 443 },
  "protocol": {
    "type": "hysteria2",
    "password": "secret",
    "up_bps": 10485760,
    "down_bps": 52428800
  }
}
```

## TUN（虚拟网络接口）

TUN 创建一个虚拟网络接口，在第 3 层捕获 IP 数据包并通过代理内核路由它们。始终编译，无需 feature gate。

### 架构

```
TunDevice (zero-tun)          -> 平台后端（Linux ioctl、macOS utun、Windows Wintun）
    -> NetworkStack (zero-traits)    -> TcpStack / UdpStack 特征
    -> UserTcpStack (zero-stack)     -> 用户空间 TCP 状态机（SYN -> SYN-ACK -> ACK -> 数据 -> FIN）
    -> TUN 入站 (zero-proxy)      -> tokio::select!{ 读取数据包 -> 喂入栈 -> accept -> serve_inbound() }
```

### 网络栈特征

`zero-traits` 定义 `TcpStack` / `UdpStack` / `NetworkStack`；这是原始 IP 数据包和面向连接的 I/O 之间的边界。两种实现：

| 实现 | 策略 | 驱动 |
|---------------|----------|--------|
| `UserNetworkStack` | 用户空间 TCP 状态机（SYN -> Established -> CloseWait，MSS 选项，seq/ack 跟踪） | 需要 TUN 设备 |
| `SystemStack` | OS TCP 监听器（iptables/pf redirect -> accept TcpStream） | Linux/macOS 无需 |

该栈通过特征可插拔；切换实现不需要更改入站处理程序。

### TCP 状态机 (UserTcpStack)

- **SYN** -> 发送 SYN-ACK 包含 MSS 选项 -> 存储在 SynReceived 状态
- **ACK** -> 转换为 Established -> 可通过 `TcpStack::accept()` 获取
- **数据** -> 提取载荷，通过 channel 转发到代理，发送 ACK
- **FIN** -> 发送 ACK，转换为 CloseWait -> 代理关闭触发我们的 FIN
- **RST** -> 立即拆除

### 平台支持

| 平台 | 后端 | 依赖 | 提供方 |
|----------|---------|------------|-------------|
| Linux | `/dev/net/tun` ioctl | 内核内置 | OS |
| macOS | utun socket | 内核内置 | OS |
| Windows | Wintun 驱动 | `wintun.dll` | GUI / 安装器 |

在 Windows 上，`wintun.dll` 是平台资源，就像 Linux 上的 `/dev/net/tun`；它必须存在于目标系统上，但内核仅声明依赖（通过 `wintun` crate），不管理 DLL 生命周期。

### CLI 命令

```bash
zero tun start --addr 10.0.0.1 --tag proxy    # 启动 TUN
zero tun stop                                  # 停止 TUN
zero tun status                                # 查看状态
```

命令通过 IPC 路由（`ProxyHandle` 在 TUN 命令到达 engine 之前拦截它们）。

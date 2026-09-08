# Hysteria2 传输与伪装配置

配置放在 HY2 `protocol.transport` 中，入站和出站均支持。`bandwidth.up` 是本端发送带宽，
`bandwidth.down` 是本端接收带宽。字符串使用十进制比特率，支持 `bps/Kbps/Mbps/Gbps/Tbps`
整数值直接表示字节/秒。字符串也支持官方缩写 `b/k/kb/m/mb/g/gb/t/tb`；例如 `100 Mbps` 等于 12,500,000 字节/秒。
省略或 `0 bps` 表示不声明固定带宽；非零值至少 65,536 字节/秒。

```json
{
  "type": "hysteria2",
  "server": "192.0.2.1",
  "server_name": "proxy.example.com",
  "port": 443,
  "password": "replace-me",
  "transport": {
    "bandwidth": {
      "up": "100 Mbps",
      "down": "200 Mbps",
      "disable_loss_compensation": false
    },
    "congestion": {
      "type": "bbr",
      "bbr_initial_window": 38400
    },
    "quic": {
      "stream_receive_window": 8388608,
      "connection_receive_window": 20971520,
      "send_window": 20971520,
      "max_idle_timeout_secs": 30,
      "keep_alive_interval_secs": 10,
      "max_incoming_streams": 1024,
      "disable_path_mtu_discovery": false
    }
  }
}
```

## 带宽与拥塞控制

客户端在 HTTP/3 认证时发送接收带宽。服务端用客户端接收带宽与本端发送上限协商 Brutal；
客户端接收带宽为零时服务端使用自适应算法。客户端发送方向取本地发送上限和服务端接收上限中
较小的非零值；双方均未限制时也使用自适应算法。

入站可设置 `transport.ignore_client_bandwidth: true`，忽略客户端固定带宽声明并返回 `auto`，
使双方使用配置的自适应算法。该参数在出站设置为 true 会被拒绝。

`congestion.type` 可选 `bbr`（默认）或 `reno`。带宽协商得到固定发送速率时自动使用 Brutal，
因此不提供容易绕过协商的 `type: brutal`。Brutal 的发包速率与拥塞窗口分开控制，使用五秒包数样本、
至少 50 个样本和 0.8 ACK 比例下限进行丢包补偿；可用 `disable_loss_compensation` 关闭补偿。
低 RTT 下拥塞窗口至少保留两个 QUIC 数据包，以满足 Quinn 的发包边界。

BBR 使用 Quinn 的 BBR 实现，`bbr_initial_window` 的单位为字节，允许 4,800–16,777,216。
这不是官方 Go 实现的逐行移植，也不等同于官方 `conservative/standard/aggressive` 三档；这些档位尚未实现。

## QUIC 参数

接收窗口和发送缓冲窗口使用字节，允许 16,384–2^60。当前是固定窗口，
不提供 quic-go 独立的初始/最大自动扩窗参数。默认流窗口 8 MiB、连接窗口 20 MiB。

空闲超时允许 4–120 秒。保活间隔允许 2–60 秒且必须短于空闲超时；0 关闭保活。
最大并发入站双向流数至少为 8。PMTU 默认开启，关闭后不主动探测更大的路径 MTU。
窗口是资源上限，不意味着一定能够达到配置的传输速度。

## 连接复用与恢复

同一出站的 TCP 请求复用已认证 QUIC 连接，并发首次请求只执行一次拨号和认证。
缓存身份包含节点、凭据、SNI、TLS 校验、指纹、传输参数和出口网络代次。
重载清空连接缓存；正在转发的流保留自己的连接引用。未使用的缓存连接按 QUIC 空闲超时回收，
每个协议适配器最多缓存 256 项。

连接关闭后下一次请求重建连接；打开新流失败时最多重新拨号一次。已经发送代理请求或业务数据的流
会把失败交给调用方，不自动重放。UDP 继续使用既有 managed-flow/packet-path 复用和恢复边界，
其缓存键同样包含新参数；当前没有把不同 UDP 会话和 TCP 合并到一个连接池。

## HTTP/3 伪装

入站 `protocol.masquerade` 支持下列形态，默认是普通 404 响应。
认证前后都可以持续处理网页请求；无效凭据走同一伪装服务。HY2 TCP 帧只有认证完成后才能进入代理路由。

```json
{"type": "file", "dir": "./site"}
```

目录相对配置文件解析，提供静态文件和目录的 `index.html`，支持 GET/HEAD。
路径先百分号解码再做目录边界检查，禁止目录穿越及指向根目录之外的符号链接。不提供目录列表。

```json
{"type": "proxy", "url": "https://www.example.com/base", "rewrite_host": true}
```

固定转发到指定 HTTP/HTTPS 站点，保留请求方法、路径、查询、请求体以及响应状态、响应体。
`rewrite_host` 默认为 false，保留访问者的 Host；true 使用上游站点 Host。
HTTPS 校验证书，HTTP 客户端不自动跟随重定向，剥离两端逐跳头。
伪装是网站源站转发，不经过 Zero 的代理会话路由规则；源站解析与连接由普通 HTTP 载体执行。

```json
{"type": "string", "content": "<h1>Hello</h1>", "status": 200, "content_type": "text/html; charset=utf-8"}
```

每个连接最多同时处理 64 个 HTTP 请求；每个请求总时限 30 秒，反向代理请求体上限 1 MiB，
响应和文件分块发送。伪装服务使用同一 QUIC 端口；此配置不额外开放 HTTP/1、HTTP/2 或明文 HTTP 监听器。

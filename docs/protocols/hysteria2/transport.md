# Hysteria2 传输与伪装配置

HY2 与 Zero 其他限速配置使用相同的 `up_bps/down_bps`，整数单位为字节/秒。
这两个字段直接放在 `protocol` 中，入站和出站均支持；省略或 0 表示不声明该方向的本地固定带宽；最终算法仍由协议协商决定。
`up_bps` 始终指客户端到目标的上传方向，`down_bps` 指返回客户端的下载方向。
使用者无需再填写官方的 `bandwidth.up/down`，Zero 配置不接受 `transport.bandwidth`。

例如 100 Mbps 对应 `up_bps: 12500000`。官方字符串单位的解析保留在协议模块内，
若导入官方配置，应在适配边界换算为 Zero 的字节/秒整数；不增加第二个对外配置入口。

```json
{
  "type": "hysteria2",
  "server": "192.0.2.1",
  "server_name": "proxy.example.com",
  "port": 443,
  "password": "replace-me",
  "up_bps": 12500000,
  "down_bps": 25000000,
  "transport": {
    "congestion": {
      "type": "bbr",
      "bbr_initial_window": 38400,
      "bbr_profile": "standard",
      "disable_loss_compensation": false
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

### 统一字段与连接带宽

| Zero 配置 | 连接方向 | HY2 内部映射 |
| --- | --- | --- |
| 出站 `up_bps` | 本端发送到 HY2 服务端 | 本地发送带宽，参与 Brutal 目标速率协商 |
| 出站 `down_bps` | 本端接收服务端数据 | 认证请求中的接收带宽声明 |
| 入站 `up_bps` | 服务端接收客户端上传 | 认证响应中的接收带宽声明 |
| 入站 `down_bps` | 服务端向客户端发送 | 本地发送带宽，参与 Brutal 目标速率协商 |

配置层和拥有 HY2 的适配器负责方向转换；协议模块只接收归一化收发参数并处理协商。
通用 engine/runtime 不解析 HY2 带宽头。入站/出站的同名字段始终保持 Zero 的方向定义，
不会要求使用者在服务端自行交换上传、下载含义。

HY2 顶层 `up_bps/down_bps` 只用于协议带宽协商，不额外建立通用 QUIC 发送硬上限，
也不作为每条逻辑流的默认字节预算。协商后的 Brutal 控制器作用于一条物理 QUIC 连接，
连接内所有 TCP 流和 datagram 共用控制器，不会因为打开多条流而复制目标带宽。

Brutal 默认补偿丢包：例如目标 1,000,000 字节/秒、有效 ACK 比例 0.9 时，
控制器请求约 1,111,111 字节/秒的发送速率。这个速率可以超过配置的目标值，
关闭补偿后才保持目标发包速率；这也不等于网卡总线速或业务有效载荷的严格额度。
ACK、恢复探测、协议开销、突发和重传会影响测量结果。接收带宽是向对端作出的协作声明，
不能阻止不合作的对端发送数据。

`zero-transport` 保留通用 TCP/UDP 字节预算和可选 QUIC pacing 上限的执行能力；
HY2 不将自己的带宽字段隐式绑定到这两种独立策略。内核 `Session` 表示一条逻辑代理 TCP
连接或 UDP 流，不是面板登录会话。只有 HY2 认证条目显式填写 `up_bps/down_bps` 时，
该条目才附加业务字节预算；某个方向省略就没有该方向的策略预算，不从 HY2 顶层字段继承。

没有 `principal_key` 时，策略预算按逻辑连接独立；显式提供相同策略标识、修订和速率时，
才跨连接共享预算。此标识对内核是中立键，不要求面板、用户表或外部账号系统。
显式预算与协议拥塞控制各自生效；预算可能降低业务吞吐，但不会改写 HY2 的协商和补偿规则。

更换顶层速率会改变 HY2 出站缓存身份；出站池重载后新连接使用新参数，
已借出的出站连接保留原参数直到释放。入站速率属于监听形态，变更时按监听重建流程应用。

### 协商规则

客户端在 HTTP/3 认证时发送接收带宽。服务端用客户端接收带宽与本端发送带宽协商 Brutal；
客户端接收带宽为零时服务端使用自适应算法。客户端发送方向取本地发送带宽和服务端接收声明中
较小的非零值；双方均未限制时也使用自适应算法。

入站可设置 `transport.ignore_client_bandwidth: true`，忽略客户端固定带宽声明并返回 `auto`，
使双方使用配置的自适应算法。此时顶层带宽不会额外限制自适应发送速率；显式认证条目的策略预算仍有效。
该参数在出站设置为 true 会被拒绝。

`congestion.type` 可选 `bbr`（默认）或 `reno`。带宽协商得到固定发送速率时自动使用 Brutal，
因此不提供容易绕过协商的 `type: brutal`。Brutal 的发包速率与拥塞窗口分开控制，使用五秒包数样本、
至少 50 个样本和 0.8 ACK 比例下限进行丢包补偿；可用 `congestion.disable_loss_compensation` 关闭补偿。
低 RTT 下拥塞窗口至少保留两个 QUIC 数据包，以满足 Quinn 的发包边界。

BBR 使用共享传输层的采样和控制器实现，协议将 `bbr_profile` 映射到
`standard`（默认）、`conservative`、`aggressive` 对应参数。档位只在协商选择自适应 BBR 时生效。
`bbr_initial_window` 独立控制初始窗口，单位为字节，默认 38,400，允许 4,800–16,777,216。
档位变化会更新连接缓存身份，重载沿用现有连接退役规则。实现边界和验收见 [BBR 对齐](bbr.md)。

## QUIC 参数

接收窗口和发送缓冲窗口使用字节，允许 16,384–2^60。默认固定流窗口 8 MiB、连接窗口 20 MiB。
可选 `max_stream_receive_window` / `max_connection_receive_window` 设置自动增长上限，
对应的既有字段作为初始值；省略上限保持旧固定行为。详见[接收窗口与自动增长](windows.md)。

空闲超时允许 4–120 秒。保活间隔允许 2–60 秒且必须短于空闲超时；0 关闭保活。
最大并发入站双向流数至少为 8。PMTU 默认开启，关闭后不主动探测更大的路径 MTU。
窗口是资源上限，不意味着一定能够达到配置的传输速度。

## 连接复用与恢复

同一出站身份的 TCP、普通 UDP 流和 UDP packet-path 共用已认证 QUIC 连接，
并发首次请求只执行一次拨号和认证。缓存身份包含出站 tag、节点、凭据、SNI、TLS 校验、
指纹、传输参数和出口网络代次。UDP 能力拒绝只影响 UDP 请求，不关闭仍可用于 TCP 的认证连接。

每条连接只有一个 UDP 接收任务，按 session ID 分发到逻辑会话；各会话独立重组分片。
入站同样保留 session ID，访问相同目标的不同会话不会合并。带宽协商、Brutal/BBR、
QUIC 连接窗口和保活自然由共享连接统一执行，不复制一份 TCP 预算和一份 UDP 预算。

重载清空连接池；正在转发的 TCP/UDP 保留连接引用。活跃连接不会按借用时间被淘汰，
无活跃用户的连接按空闲检查回收。每个适配器最多缓存 256 个连接身份；容量满时只淘汰空闲项，
全部活跃则拒绝新身份，已有身份仍可复用。

连接关闭后，新请求通过同一入口重建。TCP 打开新流失败时最多重试一次；已发送的代理请求或
业务数据不自动重放。旧 UDP 会话收到关闭，不迁移或重放已经发送的数据；后续流创建使用新连接。
详细生命周期、队列上限、参考版本与验收见[连接统一](connections.md)。

## HTTP 伪装

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
HTTPS 默认校验证书；`insecure: true` 显式跳过源站证书链/名称检查（仍验证握手签名）。
`x_forwarded: true` 按实际连接生成 `X-Forwarded-For/Host/Proto`；默认不生成。
无论开关如何，先删除访问者提供的 `Forwarded` 和这三个 `X-Forwarded-*` 头，避免伪造来源。
HTTP 客户端复用源站连接、不自动跟随重定向，并剥离两端逐跳头。
Unix 平台还可将 `url` 设为绝对 socket 路径或 `unix:///run/site.sock`，在该 socket 上转发 HTTP；
Unix URL 不允许 host、用户信息、查询或片段；Windows 配置阶段明确拒绝。
伪装是网站源站转发，不经过 Zero 的代理会话路由规则；源站解析与连接由普通 HTTP 载体执行。

```json
{"type": "string", "content": "<h1>Hello</h1>", "status": 200, "content_type": "text/html; charset=utf-8"}
```

每个连接最多同时处理 64 个 HTTP 请求；每个请求总时限 30 秒，反向代理请求体上限 1 MiB，
响应和文件分块发送。默认仅使用同一 QUIC 端口的 HTTP/3，不额外开端口。

可在任意 `masquerade` 形态中增加网站入口：

```json
{
  "type": "proxy", "url": "https://www.example.com/base",
  "rewrite_host": true, "insecure": false, "x_forwarded": true,
  "http": {"address": "0.0.0.0", "port": 80},
  "https": {"address": "0.0.0.0", "port": 443},
  "force_https": true
}
```

`https` 通过 ALPN 提供 HTTP/1.1、HTTP/2，并复用 HY2 入站的证书配置；`http` 和 `force_https`
都要求配置 `https`。开启 `force_https` 后，明文入口返回 301，保留路径和查询，转到配置的 HTTPS 端口。
其余 TCP 网站响应覆盖源站的 `Alt-Svc`，通告本入站的 QUIC 端口：`h3=":端口"; ma=2592000`。
三个入口共享同一份内容/源站策略和 HTTP 连接池。TCP 网站上的 `/auth` 也只是网站请求，不能认证 HY2。

配置沿用 Zero 的 `{address, port}` 监听字段；协议适配器映射为网站入口，不照搬官方应用的监听字符串。
配置验证拒绝与其他 TCP 入口的端口冲突；TCP HTTPS 与 UDP QUIC 可以使用相同地址和端口号。
通用 runtime 管理原子监听组：任一绑定失败会释放本次已绑定端口；重载失败恢复旧配置和整组监听，
关闭时回收全部监听及连接。协议模块只拥有网站策略，通用 transport 提供 HTTP/TLS 载体。

源站载体当前使用 HTTP/1.1，尚未接通源站 HTTP/2 协商或代理环境变量策略。
本轮没有实现 WebSocket/HTTP Upgrade、响应 trailers、静态文件 Range/条件请求、目录列表或
固定内容的任意响应头配置；不能将这组网站入口支持理解为完整复制 Go `ReverseProxy`/`FileServer` 的行为。
固定参考为 [app/v2.12.2 的网站入口](https://github.com/HyNetworks/hysteria/blob/app/v2.12.2/extras/masq/server.go)
和[源站策略](https://github.com/HyNetworks/hysteria/blob/app/v2.12.2/app/cmd/server.go)。

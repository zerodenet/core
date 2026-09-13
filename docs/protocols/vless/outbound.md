# VLESS 出站

VLESS 出站通过注册的 TCP 和 UDP 能力接入运行时。适配器把引擎解析后的出站投影为 VLESS 传输叶子；载体打开和协议握手分属传输层与协议层。

## TCP

1. 运行时解析直连或中继最终跳。
2. VLESS 适配器准备连接或 relay-hop 操作。
3. `zero-transport` 打开选定载体。
4. `protocols/vless` 完成 VLESS 请求与响应处理。
5. 运行时统一归一化结果、错误和流量记录。

## Reality ClientHello 指纹

VLESS Reality 出站通过 `reality.client_fingerprint` 选择固定 uTLS 版本的完整 ClientHello 预设，包括 Chrome、Firefox、Safari、Edge、iOS、QQ 和随机模式，默认 `chrome` 固定到 Chrome 133。支持 22 个版本化预设；字段、随机规则与 TLS 1.3 边界见 [ClientHello 指纹](fingerprints.md)。不同指纹不会共享同一个 MUX 连接池载体。

该字段只作用于 Reality 客户端。普通 VLESS TLS 仍使用通用 TLS profile，Reality 入站也不读取客户端指纹配置。

## Flow 语义

`flow: xtls-rprx-vision` 使用 Xray VLESS Addons protobuf，并实现 Vision UUID、Continue/End/Direct 帧及 TLS 1.3 直通切换。原始 TLS 1.3、REALITY 或 VLESS Encryption 提供可切换承载；普通 TLS 1.2 在协议协商阶段拒绝。Vision UDP 使用 XUDP，默认拒绝 UDP/443；`xtls-rprx-vision-udp443` 允许该端口，线上仍发送标准 Vision flow。Vision 不使用普通 MUX TCP，也不能配置 `mux_concurrency`。TLS 1.3 内层 TCP 与 XUDP 已通过固定官方版本的双向互通。

`testseed` 使用 Xray 的四项 Vision 填充参数，默认 `[900, 500, 900, 256]`；不足四项时整体回退到该默认值，超过四项时只读取前四项。参数会进入普通 TCP、Vision XUDP/MUX 和 Rvs 的实际写帧路径，连接池身份也区分不同的有效参数。`testpre` 指定握手前保留的备用载体数量；备用载体在两分钟后淘汰，取用后延迟 200 毫秒补充。它与 MUX、gRPC、XHTTP 等载体复用层彼此独立，reload 会停止补充并释放排队连接，中继最终跳不会取用直连备用载体。

历史版本曾把 Zero 私有的请求头 AES-GCM 格式错误命名为 `xtls-rprx-vision`。该格式现仅以 `flow: zero-aead-v1` 显式保留，用于需要迁移的 Zero 对端；它不与 Xray Vision 互通。迁移必须显式选择目标语义，内核不会静默猜测旧配置。

## UDP

VLESS UDP 流计划属于 `protocols/vless` 的 `udp` 模块。传输桥保存协议流计划和中立载体选项，通用运行时只管理流生命周期、中继顺序和计量。

普通 VLESS TCP 出站先发送协议请求并立即允许业务上行，首次下行时再剥离 VLESS 响应头；该语义同时适用于原始 TCP、包装传输和 TCP relay 最终跳，避免外部服务端把响应头与首段回程数据一并冲刷时形成首包互锁。普通 VLESS UDP-over-stream 的目标由建立请求固定，后续帧为 `[2-byte payload length][payload]`；地址携带格式只属于 MUX/XUDP 路径。

Zero 原生 MUX 使用标准 Mux.Cool 元数据和独立数据长度帧。TCP 子流由 `mux_concurrency` 启用；XUDP 会话由独立的 `xudp_concurrency` 启用，首包发送 `NEW + target + GlobalID`，后续包发送 `KEEP + target`，允许同一 UDP association 在目标间切换。两者不依赖 `flow` 兼容开关。真实 Xray v26.3.27 黑盒矩阵已通过 Zero → Xray 的 TCP MUX、A → B → A 多目标 XUDP，以及同一 MUX 池中的并发 XUDP association；该结果只作为线协议互操作证据。

`mux_idle_timeout_secs` 由 Zero 自己定义为物理 MUX 载体的无帧活动期限，同时适用于 TCP MUX 与 XUDP。每个真实 MUX 帧都会刷新期限；超时会让载体的读写两半共同退出并从池中失效，下一条逻辑流建立新载体。池键包含该策略，配置不同期限的出站不会错误复用同一载体。

MUX 下行响应受 `mux_response_backlog_frames` 与 `mux_response_backlog_bytes` 两层 Zero 原生容量策略约束，默认分别为每逻辑流 32 帧、每物理载体 1 MiB，允许范围分别为 `1..=4096` 与 16 KiB..=64 MiB。载体读循环不会在慢 TCP/XUDP 消费者后面创建无界队列；任一限制触发时向该逻辑流投递显式 overflow 终止并从载体映射移除，其他未超限流可以继续使用载体。入站回程写队列使用同名配置和同一语义，越界直接返回协议所有者定义的 backlog 错误；出站池键包含完整策略，不同容量配置不会共池。

XHTTP `stream-one` 出站生产路径使用单条 H2/H2C 双向流。客户端会规范化尾斜杠、发送默认 100–1000 字节请求 padding，并在协议首包写入后异步接管响应；服务端同时接受 H2/H2C 与 Xray 明文客户端使用的 HTTP/1.1 chunked 单流，并保持相应连接驱动到流结束。该路径已通过 Xray v26.3.27 的双向直连 TCP/标准 VLESS UDP，以及经 SOCKS5 首跳后的 relay-chain 最终跳 TCP/UDP 真实进程互通。


`packet-up` 使用 path/session/sequence POST 上传并检查确认，`stream-up` 使用独立的 HTTP/2 上传和下载流。`auto` 通常选择 packet-up，配置 REALITY 时选择 stream-one。旧 `X-Session-Id` 私有配对格式已移除；迁移与未实现的高级选项见 [XHTTP 契约](./xhttp.md)。

VLESS Encryption 的 `encryption` 配置与会话语义见 [Encryption](encryption.md)。

WS/HTTPUpgrade 首包、Host 与自定义头配置见 [HTTP carrier Early Data](early-data.md)。

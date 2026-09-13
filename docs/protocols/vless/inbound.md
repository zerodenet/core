# VLESS 入站

VLESS 入站负责传输请求准备、协议接受、用户鉴权和目标解析。接受完成后，TCP、UDP 和 MUX 请求分别进入通用入站路由边界。

普通 UDP 会话在响应头之后按请求中的固定目标读写 `[2-byte payload length][payload]` 帧，并拒绝运行时试图把回包写向不同目标；MUX/XUDP 继续使用其独立的地址携带帧，不与普通 UDP 流混用。

## 责任划分

| 责任 | 所有者 |
| --- | --- |
| UUID、flow 与 VLESS 请求解析 | `protocols/vless` |
| TLS、REALITY、WebSocket、gRPC、HTTP/2、XHTTP、QUIC 载体 | `zero-transport` |
| 监听、接受循环、关闭与任务回收 | `zero-proxy` 通用入站运行时 |
| 接受后的 TCP、UDP 与 MUX 路由 | `zero-proxy` 通用路由管线 |

VLESS 适配器只准备协议所需的操作，不自行启动监听循环，也不保留完整 `Proxy` 对象。

`xtls-rprx-vision` 支持原始 TLS 1.3、REALITY，以及 VLESS Encryption 提供的可切换承载。响应头之后使用 Vision UUID/Continue/End/Direct 帧；直通控制穿过录制与计量包装，TLS 层在完整记录边界切换。TLS 1.2 不提供直通控制，连接在协议协商阶段拒绝。Vision UDP 使用 XUDP/MUX，拒绝普通 UDP 命令和 MUX TCP；出站 `xtls-rprx-vision-udp443` 仅调整 UDP/443 策略，线上 flow 仍为 `xtls-rprx-vision`。Zero 私有 `zero-aead-v1` 保留显式迁移用途。

每个 VLESS 入站用户可配置 Xray 兼容的 `testseed` 四项参数，用于该用户通过鉴权后的 Vision 下行填充。默认值为 `[900, 500, 900, 256]`；不足四项时整体使用默认值，超过四项时忽略其余值。该参数跟随已鉴权用户进入普通 TCP、Vision MUX 和 Rvs 连接，不由通用运行时解析。

XHTTP 入站由 `zero-transport` 执行 HTTP/1.1 和 HTTP/2 请求，支持 `packet-up`、`stream-up`、`stream-one`，`auto` 接受三种模式。每个 GET 下载或 stream-one 请求形成独立的中立字节流；通用运行时负责并发任务和回收，`protocols/vless` 逐流完成鉴权并返回 TCP、UDP 或 MUX 路由。会话配对和序号重排留在传输层，详细线协议、容量与参考版本见 [XHTTP 契约](./xhttp.md)。

## 数据路径

- TCP 请求进入通用 stream route。
- UDP-over-stream 请求通过协议所有的 relay 封装交给通用 UDP 路由。
- MUX TCP/UDP 子流通过中立的 MUX relay 契约交给运行时。

VLESS Encryption 的 `decryption` 配置与会话语义见 [Encryption](encryption.md)。

WS/HTTPUpgrade 首包、Host 与自定义头配置见 [HTTP carrier Early Data](early-data.md)。

多规则回落、Unix 目标和 PROXY 转交见 [Fallback](fallback.md)。

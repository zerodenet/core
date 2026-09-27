# Zero 与正式发布内核的实用协议实现差异

日期：2026-09-12。检查对象：`de97b051d97b15fd148c304d4a830bb4e85e549c` 加当前工作区的 VMess AuthID / SS2022 重放修复。修复尚未提交，不能只凭 HEAD 重现本报告状态。承接任务 `01a09103-46e3-7ac3-a320-64f1a05b2a49`；旧审计只用于定位，本报告重新读取配置、注册、协议执行路径、固定上游源码与相关测试。

## 评估口径

- 实现完成度只讨论目标能力是否存在、执行是否接通，以及已知正确性、状态生命周期和兼容行为是否完整。
- 测试和受控官方互通作为判断证据独立记录；缺少测试执行证据不自动等于缺少实现。
- **生产验证不进入实现完成度分母，不是剩余开发百分比；它是实现与受控验收完成后的最后上线步骤。**
- 功能缺失、已有功能缺陷、有意策略差异、基线管理问题分别列出。没有完成全部状态机的逐行等价审计，因此不编造协议百分比，也不把未审计部分判成缺失。
- 不以 RFC 全集、旧密码套件、第三方 CLI/面板/配置格式为必需范围。SOCKS5 BIND/FRAG、旧 alterId、Trojan-Go 专用管理能力不用于扣分。

## 固定参考

沿用前次对话选择的正式版本，不追随开发分支，也不宣称这些是今天最新版本。本轮通过 GitHub commit API 重新确认以下 tag 与 SHA 全部一致，并通过 release API 确认七个版本均为 `draft=false`、`prerelease=false`。下载源码在 `/tmp/zero-protocol-audit-20260911/`；长期引用使用不可变 SHA。

| 参考 | 版本 | commit | 定位 |
|---|---|---|---|
| Xray-core | v26.3.27 | `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0` | VLESS/VMess、HTTP、XHTTP 实用能力比较；现有互通脚本使用此版本 |
| sing-box | v1.13.14 | `25a600db24f7680ad9806ce5427bd0ab8afe1114` | HTTP/Mixed 及主流扩展 |
| Mihomo | v1.19.30 | `ac017cdd246ce8bd547653d927e7bf77d7ee73d5` | HTTP、Trojan 节点承载 |
| Hysteria | app/v2.6.1 | `401ed5245d9bdfe0a35d629a8d977f256da07a75` | 与 HY2 包声明相同版本的实际缺项比较 |
| Mieru | v3.33.0 | `48ddb69d5d343d76c9004c5054ee36609579ba13` | 多会话、可靠 UDP、连接池策略 |
| Shadowsocks-Rust | v1.21.2 | `a03006a753486e64717d6e3afa91e0c6d043c557` | AEAD/2022 及 UDP 状态生命周期 |
| Trojan-Go | v0.10.6 | `2dc60f52e79ff8b910e78e444f1e80678e936450` | 包声明参照及 smux 扩展边界，不代表所有 Trojan 实现 |

版本管理仍有两项差异：

1. VLESS/VMess 的 Cargo 包写着 Xray `25.3.1`。本轮 `git ls-remote --tags ... 'v25.3*'` 只返回 `v25.3.3`、`v25.3.6`、`v25.3.31`，没有所声明 tag。应正式选定真实 tag/SHA，再统一 package、lockfile、参考说明、默认互通和 CI；本报告没有替项目升级基线。
2. HY2 包为 `2.6.1`，互通 CI 和部分传输实现参照 `2.12.2`。下文 obfs/hop/CA/pin 在 **2.6.1 已存在**，并非拿新版本功能要求旧版本。BBR/profile 等机制已有实现，但不能据此宣布整包严格对齐 2.6.1 或 2.12.2。

## 各协议结论

| 协议 | 当前已落地范围 | 确认的实现差异 | 完成度判断 |
|---|---|---|---|
| SOCKS5 | 双向 CONNECT、UDP ASSOCIATE、用户名密码认证、上游 association | BIND/FRAG 明确不支持；Xray/Mihomo 同样不实现这些完整能力 | 本轮未确认新的常用功能欠账；不由此宣称所有异常路径完全等价 |
| HTTP | 入站 CONNECT、普通 HTTP 正向代理 | 无 HTTP/HTTPS CONNECT 出站；HTTP 入站无用户认证 | 常用产品能力确有缺口 |
| Mixed | 同端口 HTTP/SOCKS5，SOCKS5 UDP 和认证 | HTTP 分支不使用 SOCKS5 用户认证；sing-box 两分支共用 authenticator | 入口能复用，但统一认证没有实现 |
| Shadowsocks | AEAD/2022 六种 cipher、EIH、多用户、TCP/UDP | 重放窗口边界已修；UDP 历史会话/客户端/重放窗口映射未见细粒度过期或容量回收 | 主要已有功能欠账是资源生命周期，不能继续写“2022 未实现” |
| VMess | AEAD、TCP/UDP、TLS/WS/gRPC、MUX，AuthID 时效/CRC/重放已接通 | `zero` 存在专门的 Xray 拒绝互通测试；可配置承载集合少于 Xray 的整体 transport 集合 | 旧 P1 已修，不能重复扣分；`zero` 和承载兼容范围须明确 |
| VLESS | 普通流、Vision、REALITY、MUX/XUDP、fallback、多种承载 | XHTTP 两种上传模式、auto 选择与官方不同；无完整 XHTTP/H3 接入；QUIC CA 配置未消费 | 主协议与多种承载已实现，但不能标成完整 XHTTP 兼容 |
| Trojan | TLS TCP、流式 UDP、多用户、MUX、relay TLS | 无 WS/gRPC 配置执行入口；Zero Mux.Cool 与 Trojan-Go smux 不同 | 基础代理已落地，生态承载缺项与专用扩展区别对待 |
| HY2 | H3 认证、TCP stream/UDP datagram、分片、池化、会话隔离、带宽协商、Brutal/BBR/Reno、伪装 | 无 Salamander、端口跳跃、私有 CA/pin 接入；参考版本不统一 | 传输核心并非基础空壳，剩余是明确功能与版本契约差异 |
| Mieru | TCP/原生 UDP underlay、多逻辑会话、池化、ACK/重传/乱序/CUBIC、背压和 traffic pattern | 连接池增长和退休策略不同于官方 preset | 本轮未确认需补的核心机制；策略差异不当作缺失，也不声称完全等价 |

注册入口：[register.rs](/Volumes/tool/rust/zero/crates/proxy/src/register.rs:16)。配置能力：[outbound.rs](/Volumes/tool/rust/zero/crates/config/src/model/outbound.rs:21)、[inbound.rs](/Volumes/tool/rust/zero/crates/config/src/model/inbound.rs:27)。

## 需要开发的具体差异

### 1. SS UDP 状态回收仍是已有功能的实现欠账

`ShadowsocksInboundUdpCodec` 持有按 client session ID 索引的 `replay_windows`；每个合法新 session 都会插入。responder 又持有 `proxy_sessions`、`proxy_clients`、`proxy_users`。本轮检查仍只看到按已移除用户裁剪用户 session 的路径，没有每个历史 flow/session 按 idle、结束事件或容量回收的路径。外层流退出并不会自动清掉这几张协议内部映射。

- Zero：[codec](/Volumes/tool/rust/zero/protocols/shadowsocks/src/udp/inbound.rs:202)、[responder state](/Volumes/tool/rust/zero/protocols/shadowsocks/src/udp/inbound.rs:362)。
- 官方：[SS-Rust UDP lifecycle](https://github.com/shadowsocks/shadowsocks-rust/blob/a03006a753486e64717d6e3afa91e0c6d043c557/crates/shadowsocks-service/src/server/udprelay.rs#L46) 用有过期时间、可选容量的 LRU，并周期 cleanup。
- 实际影响：活跃监听器持续接收新合法会话时，历史状态保留会增长。这里确认的是代码生命周期缺口，没有测定 RSS 增幅。
- 完成条件：状态有界并能回收，重放保护保留期与 flow 生命周期分开；不能简单删窗口导致旧包重新获准。受控时间推进和大量短会话即可验收，不需要等生产才算实现完成。

### 2. VLESS XHTTP 存在实际线路契约差异

Zero `packet-up`、`stream-up` 都走同一个旧式 POST/GET 双连接，使用固定 path 和 `X-Session-Id`。固定版 Xray 有独立上传模式，并在 packet-up 中按序号分批发送 POST。名称相同不能推出可互换。

此外，Zero `auto` 总是归为单连接 `stream-one`；Xray v26.3.27 的 auto 默认 `packet-up`，配置 REALITY 时改 `stream-one`，再有 download settings 则改 `stream-up`。因此默认模式语义也不同。这不表示所有 auto 连接必然失败，而是不能直接照搬官方配置预期。

- Zero：[mode](/Volumes/tool/rust/zero/crates/transport/src/split_http/stream_one.rs:22)、[dispatch](/Volumes/tool/rust/zero/protocols/vless/src/transport/outbound/direct.rs:95)、[paired wire](/Volumes/tool/rust/zero/crates/transport/src/split_http/legacy.rs:17)。
- 官方：[Xray auto / upload dispatch](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/transport/internet/splithttp/dialer.go#L381)。
- 独立 QUIC transport 不是 HTTP/3 XHTTP；已有 stream-one 实现与测试不证明其余模式已对齐。
- 完成条件：实现对应模式的 session/sequence/request contract，或准确限制和命名现有能力。若目标承诺完整 XHTTP，则限制声明只解决误导，不等于补齐完整功能。

### 3. VLESS QUIC 自定义 CA 字段未生效

profile 保存 `ca_cert_path`，但 `open_vless_quic_transport` 没有传递，通用 QUIC client 只选择公共根或 insecure。配置私有 CA 不会改变信任根，属于配置承诺与执行不一致。

Zero：[profile](/Volumes/tool/rust/zero/protocols/vless/src/transport/profile.rs:62)、[connect](/Volumes/tool/rust/zero/protocols/vless/src/transport/outbound/direct.rs:15)、[TLS roots](/Volumes/tool/rust/zero/crates/transport/src/quic/client.rs:10)。补接 CA 或明确拒绝该字段均可消除无效配置；是否继续维护该承载是另一项产品选择。

### 4. HTTP/Mixed 是两个独立的常用功能缺项

HTTP 出站要求 Zero 连接 HTTP/HTTPS 上游代理并建立 CONNECT 隧道；当前出站 ADT 与 registry 都不存在该能力。HTTP 入站认证是另一件事：Mixed 的 HTTP 分支使用默认 handler，只有 SOCKS5 分支消费 `socks5_users`。

- Zero：[Mixed branch](/Volumes/tool/rust/zero/crates/proxy/src/adapters/mixed/inbound.rs:65)。
- 官方：[sing-box HTTP outbound](https://github.com/SagerNet/sing-box/blob/25a600db24f7680ad9806ce5427bd0ab8afe1114/protocol/http/outbound.go#L35)、[Mixed shared authentication](https://github.com/SagerNet/sing-box/blob/25a600db24f7680ad9806ce5427bd0ab8afe1114/protocol/mixed/inbound.go#L128)。
- 必要性：上游企业代理/链式代理需要出站；将入口开放给多个用户需要认证。两项均为真实开发，不是等待上线验证。

### 5. HY2 的剩余节点兼容功能明确

官方 app/v2.6.1 已将 Salamander 包装到 UDP carrier，端口范围交给 UDP hop socket，并在 TLS 配置中消费 CA 和 pinSHA256。Zero 当前 HY2 出站只有单 port、password、SNI/insecure/fingerprint 与传输参数，没有这些接线。

- 官方：[Hysteria client](https://github.com/HyNetworks/hysteria/blob/401ed5245d9bdfe0a35d629a8d977f256da07a75/app/cmd/client.go#L229)。
- Zero：[HY2 config](/Volumes/tool/rust/zero/crates/config/src/model/outbound.rs:73)、[QUIC TLS](/Volumes/tool/rust/zero/crates/transport/src/quic/client.rs:10)。
- 影响分别是不能直接使用启用 Salamander 的节点、不能按官方方式端口跳跃，以及不能按指定 CA/pin 验证对应节点。公开证书、无混淆固定端口节点不因此变为未实现。
- 已有池化、single-flight、reload 退休、UDP 单读者分发与独立有界队列，以及拥塞选择都能在执行代码中确认：[pool](/Volumes/tool/rust/zero/protocols/hysteria2/src/transport/pool.rs:13)、[dispatch](/Volumes/tool/rust/zero/protocols/hysteria2/src/udp/dispatch.rs:25)、[congestion](/Volumes/tool/rust/zero/protocols/hysteria2/src/transport/congestion.rs:22)。不能重复列为待开发。

### 6. Trojan 扩展按对端区分

Mihomo v1.19.30 的 Trojan 出站有 WS、gRPC 分支；Zero Trojan ADT 无相应字段。要连接这类节点，就确实需要新增承载。

Zero Trojan MUX 使用 Mux.Cool，Trojan-Go v0.10.6 则调用 xtaci/smux，二者不是同一线协议。因此“有 MUX”不代表兼容 Trojan-Go MUX；不需要该扩展的部署也不应被算作缺失核心功能。

证据：[Mihomo Trojan](https://github.com/MetaCubeX/mihomo/blob/ac017cdd246ce8bd547653d927e7bf77d7ee73d5/adapter/outbound/trojan.go#L80)、[Trojan-Go smux](https://github.com/p4gefau1t/trojan-go/blob/2dc60f52e79ff8b910e78e444f1e80678e936450/tunnel/mux/server.go#L34)、[Zero MUX classifier](/Volumes/tool/rust/zero/protocols/trojan/src/mux.rs:498)。

## 已完成与有意不同

VMess AuthID 已先解密验证 CRC、检查 ±120 秒时间窗口，然后在等待后续头之前登记共享重放状态。缓存以 credential + AuthID 为键，保留 241 秒，有 262144 容量，满时拒绝新请求而不是驱逐尚有效 ID。它和 Xray 的具体缓存组织不同，但已具备本次关注的时效与防重放能力；全局容量及互斥是 Zero 的运行策略，不是逐字复刻官方实现。

SS2022 已将保留集合改为包含左边界，0 和边界重复包不会被刚插入就删除。这两项不再列为未完成。证据：[VMess AuthID](/Volumes/tool/rust/zero/protocols/vmess/src/auth.rs:9)、[early reservation](/Volumes/tool/rust/zero/protocols/vmess/src/inbound.rs:422)、[SS replay](/Volumes/tool/rust/zero/protocols/shadowsocks/src/shared.rs:936)。

Mieru 官方按 multiplex factor 概率选择已有 underlay，并有 idle/流量触发退休；Zero 每身份最多 4 条、75% 负载扩容、30 分钟年龄轮换。这会改变连接数和调度表现，但已有完整池机制，不是“未实现多路复用”。ACK/重传/乱序、方向检查、逻辑关闭及退休 session 也有代码，未发现足以要求重新开发这些机制的证据。

证据：[official pool](https://github.com/enfein/mieru/blob/48ddb69d5d343d76c9004c5054ee36609579ba13/pkg/protocol/mux.go#L697)、[Zero pool policy](/Volumes/tool/rust/zero/protocols/mieru/src/client/pool/policy.rs:5)、[UDP receive lifecycle](/Volumes/tool/rust/zero/protocols/mieru/src/packet/driver/receive.rs:4)。这不代表所有吞吐/拥塞行为已证明与官方等价。

VMess `zero` 的现有测试明确期望 Xray 拒绝：[test](/Volumes/tool/rust/zero/crates/proxy/tests/vmess_xray_interop.rs:118)。本轮未执行外部测试，因此只确认代码将它列为负向互通范围，不声称重新测得拒绝；不应把本地互通成功模式一律标成官方兼容。

## 本轮验证与后续顺序

本轮执行：

```sh
RUST_MIN_STACK=16777216 cargo test -p vmess -p shadowsocks --all-features --lib --test replay_window
```

结果：23 passed、0 failed、0 ignored。其中 VMess AuthID 9 项，SS replay 3 项，其余是同命令带入的校验/MUX 测试。没有修改运行代码，没有重跑全 workspace、官方二进制互通或生产验证。前次对话报告的全量 1666 通过属于前次执行，不作为本轮新结果。

建议开发顺序：先完成 SS 状态回收及 VLESS 无效配置/模式契约；选定真实 Xray/HY2 基线；按目标节点范围补 HTTP/Mixed、HY2 obfs/hop/CA/pin、Trojan WS/gRPC。对应改动以受控互通、异常与资源有界测试验收。**这些实现与验收完成后，才执行最后的生产验证；生产验证始终单列，不倒算为协议开发欠账。**

# 协议实用能力与成熟度审计

审计代码：`de97b051d97b15fd148c304d4a830bb4e85e549c`，2026-09-11。开始时工作区干净。

本报告基于当前执行代码、测试断言和本次取得的上游源码，不使用旧进度文档或历史记忆作为实现证据。目标是本地代理客户端、节点入站、普通 TCP/UDP 转发及现有链式代理的实用能力。未取得实际用户配置分布，因此功能缺项的业务优先级是建议；没有把某个扩展自动列为所有部署的必需品。

## 结论

不能将所有协议统一归类为“基本实现”，也不能因为已支持 TCP/UDP 就认定成熟。当前核心功能覆盖较广，Mieru/HY2 在并发、背压、连接复用和传输控制方面已有实质工作；本轮更重要的发现是 VMess、SS2022 的确定性重放缺陷，以及部分能力名称与真实兼容范围不一致。

建议先修已承诺能力的正确性，再补常用场景功能，最后考虑协议扩展。SOCKS5 BIND/FRAG、旧密码套件、第三方应用的配置格式和管理功能不自动计为欠账。

## 参考源码及基线问题

| 参考 | 本次固定版本 / commit | 用途 |
|---|---|---|
| Xray-core | v26.3.27 / `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0` | 使用仓库现有 interop 下载脚本的版本进行补充比较；不重新定义 VLESS/VMess 基线 |
| sing-box | v1.13.14 / `25a600db24f7680ad9806ce5427bd0ab8afe1114` | 使用现有 HY2 CI 参考版本比较常用产品能力 |
| sing-vmess | `3aed155119a174c9290a4099841049f0cb275e02` | 上述 sing-box 实际锁定的 VMess 实现 |
| Mihomo | v1.19.30 / `ac017cdd246ce8bd547653d927e7bf77d7ee73d5` | 固定版本的常用节点承载参考，不声称是最新版 |
| Hysteria | app/v2.6.1 / `401ed5245d9bdfe0a35d629a8d977f256da07a75` | 与 Zero HY2 包声明相同的参考版本 |
| Mieru | v3.33.0 / `48ddb69d5d343d76c9004c5054ee36609579ba13` | 与协议包、配置子包及 interop 脚本一致 |
| Shadowsocks-Rust | v1.21.2 / `a03006a753486e64717d6e3afa91e0c6d043c557` | 与 Zero SS 包声明一致 |
| Trojan-Go | v0.10.6 / `2dc60f52e79ff8b910e78e444f1e80678e936450` | 核对其与 Zero Trojan MUX 的实际差别 |

基线存在两个实际问题：

1. VLESS/VMess 包注释声明 Xray `v25.3.1`，但本次 GitHub commit 查询不能解析此 ref，`git ls-remote --tags ... 'v25.3*'` 返回 v25.3.3、v25.3.6、v25.3.31，没有 v25.3.1。必须先明确真实不可变基线，不能以此版本号证明对齐。现有 `scripts/prepare-xray-interop.ps1:8` 使用 v26.3.27。
2. HY2 包声明 2.6.1，但 `.github/workflows/hysteria2-interop.yml` 的官方二进制、BBR 和窗口向量来源为 2.12.2。这是参考版本管理问题，不直接证明数据通路错误。本轮没有修改任何版本。

完整参考索引：[references.json](protocol-audit-2026-09-11/references.json)。下载的源码位于 `/tmp/zero-protocol-audit-20260911/`，永久证据链接使用上述 commit。

## 必须优先处理的正确性问题

### F1 / P1：VMess 入站缺 AuthID 时效和重放防护，已复现

- 执行点：`protocols/vmess/src/inbound.rs:421` 读取 AuthID，直接尝试用用户密钥打开 AEAD 长度；`:456` 打开 payload 后解析命令。入站 profile 只有用户集合，没有共享重放状态。AuthID 没有经过解密后的时间及 CRC 校验。
- 本次在同一个 `VmessInboundProfile` 上，把 Zero 出站生成的同一完整握手输入两次，两次均成功接受。
- 使用现有加密函数将有效请求重新封装成 timestamp=1（1970）的 AuthID，也成功接受。没有修改协议逻辑或系统时钟。
- 影响：已有合法握手的重放和过期请求不能在认证边界被拒绝。这是已支持协议的入站防护缺陷，不是功能偏好。本轮证明的是接受握手，未向业务目标发送重放负载。
- 上游：Xray 和 sing-vmess 都检查时间并维护重放过滤器。[Xray AuthID](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/proxy/vmess/aead/authid.go#L100)、[sing-vmess](https://github.com/SagerNet/sing-vmess/blob/3aed155119a174c9290a4099841049f0cb275e02/service.go#L195)。
- 验收：首次合法握手成功，重复握手、过期/超前 AuthID、无效 AuthID 校验值失败；并发与用户更新下共享防重放状态有明确生命周期。

### F2 / P1：SS2022 UDP 重放窗口边界错误，已复现

- 执行点：`protocols/shadowsocks/src/shared.rs:935`。检查落后范围使用 `>`，清理集合却使用 `s > cutoff`，接受窗口与保留窗口边界不一致。
- 本次结果：`0 -> 0` 两次接受；`4096 -> 2048 -> 2048` 三次接受。普通 `1 -> 1` 则正确拒绝第二次，作为对照。
- 原因：包号 0 首次接受后立刻被 cutoff=0 的清理移除；左边界包号通过范围检查，却在插入后立即被清除，因此可重复接受。
- 真实调用：`protocols/shadowsocks/src/udp/inbound.rs:257` 在成功解密后调用该过滤器。复现直接调用真实过滤器；没有宣称完成网络攻击演示。
- 上游：[Shadowsocks-Rust PacketWindowFilter](https://github.com/shadowsocks/shadowsocks-rust/blob/a03006a753486e64717d6e3afa91e0c6d043c557/crates/shadowsocks-service/src/net/packet_window.rs#L47) 使用位图记录已接收 ID；UDP server 在分发前验证 packet ID。
- 验收：零号、左右边界、跳跃、乱序、重复及高包号测试；过滤器和实际 codec 均拒绝重复数据包。

### F3 / P2：VLESS 的 packet-up/stream-up 实际不是完整 Xray XHTTP 模式

- `protocols/vless/src/transport/outbound/direct.rs:95` 将非 single-connection 模式交给 `connect_split_http`。
- `crates/transport/src/split_http/legacy.rs:17` 使用固定路径、`X-Session-Id`、持续 POST + GET；两个模式共用该实现，没有 packet-up 的逐 POST 序号/分片请求流程。
- 固定 Xray 的实现有独立 stream-up 和 packet-up 路径、session/sequence 元数据及 upload queue。[Xray dialer](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/transport/internet/splithttp/dialer.go#L486)、[server](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/transport/internet/splithttp/hub.go#L206)。
- 判断：stream-one 有实际 Xray interop 测试；不能将其成功扩展为 packet-up/stream-up 通用互通。现有 paired 实现最多按独立兼容范围描述。此项为源码契约差异，本轮未运行外部负向互通。
- 验收：要么限制配置/能力声明到真实支持范围，要么补固定 Xray 双向 packet-up、stream-up、请求合并/分片、序号和连接中断用例。

### F4 / P2：VLESS QUIC 接受 ca_cert_path，但出站未使用

- `protocols/vless/src/transport/profile.rs:62` 保存 CA 路径，`protocols/vless/src/transport/outbound/direct.rs:24` 调用 `connect_quic` 时不传此字段；后续通用 QUIC client 使用公共根或 insecure。
- 影响：用户配置私有 CA 后仍无法按指定 CA 验证。不是默默跳过所有证书校验，而是配置未生效。
- 即使决定不再扩展旧式 VLESS QUIC，也应明确拒绝未支持字段，不能保留无效承诺。此项为执行路径确认，未做真实 TLS 连接复现。

### F5 / P2：SS UDP 会话相关状态缺少细粒度回收路径

- `protocols/shadowsocks/src/udp/inbound.rs` 中 `replay_windows` 按 client_session_id 插入；`proxy_sessions`、`proxy_clients`、`proxy_users` 随成功分发增长。
- 同文件唯一 `sessions.retain` 根据活跃用户清理；未见按流结束、时间或容量清理上述每用户映射的路径。responder 由 listener 的 UDP relay 持有。
- 影响判断：长期监听器在用户仍有效时，会持续保留历史会话状态；本次确认状态生命周期代码，没有进行长时间 RSS 压测，因此不报告具体内存增幅。
- 上游 SS-Rust server 的 association map 有过期回收。[server lifecycle](https://github.com/shadowsocks/shadowsocks-rust/blob/a03006a753486e64717d6e3afa91e0c6d043c557/crates/shadowsocks-service/src/server/udprelay.rs#L230)。
- 验收：持续创建/关闭 UDP association 后状态数量稳定；重放保护保留时间与流回收分开设计，不能简单删除状态后允许旧包重放。

## 实际功能差距与适用场景

| 协议 | 当前可确认的实用覆盖 | 真正应考虑的差距 | 处置 |
|---|---|---|---|
| SOCKS5 | 双向 CONNECT、UDP ASSOCIATE、用户名密码、链式上游 | 本轮没有发现必须新增的常用命令；进一步成熟度看 association 来源绑定、退出及 idle 清理 | 维持现有功能范围；不补 BIND/FRAG 来凑完整度 |
| HTTP | 入站 CONNECT、HTTP/1.0/1.1 正向代理和消息转发 | HTTP 出站；HTTP 用户认证 | HTTP 上游需求列为下一批常用功能；对外提供认证入口时优先补认证 |
| Mixed | 同端口 HTTP + SOCKS5，SOCKS5 认证和 UDP | 只有 socks5_users，HTTP 分支无认证 | 不应把它当作“整个端口都认证”；需要共享入口认证时补齐 |
| Shadowsocks | 6 种 AEAD/2022 cipher、EIH、多用户、TCP/UDP | F2/F5 是成熟度问题；旧 AEAD replay 策略可选 | 先修已有 2022 和状态生命周期，再讨论额外策略 |
| VMess | AEAD TCP/UDP、TLS/WS/gRPC、MUX | F1；各 cipher/MUX 互通范围须独立验证 | 防护先于扩充 transport；不补旧 alterId 来增加功能数 |
| VLESS | 普通流、Vision、REALITY、MUX/XUDP、多个承载和 fallback | F3/F4；XHTTP H3 无完整接入，不能把 raw QUIC 当 H3 | 先纠正模式兼容范围；是否补 H3 取决于实际节点需求 |
| Trojan | TLS TCP、UDP-over-stream、多用户、MUX、relay TLS | 缺 WS/gRPC；现有 MUX 不是 Trojan-Go smux | 要接入 WS/gRPC 节点才补承载；MUX 必须说明对端类型 |
| HY2 | HTTP/3 auth、QUIC TCP/UDP、分片、池化、会话隔离、Brutal/BBR、伪装站点 | Salamander、端口跳跃、私有 CA/pin；参考版本管理 | 这些是实际生态功能，按节点配置需要纳入；不是当前无混淆节点全部失效 |
| Mieru | 双向 TCP/UDP 底层、多逻辑会话、池化、ACK/重传/CUBIC、traffic pattern | 池策略不是官方 preset；不能据此判缺陷 | 当前优先固定版本互通和状态机可靠性，未发现必须复制官方池策略的理由 |

### HTTP/Mixed 的边界

`crates/config/src/model/inbound.rs:33` 的 HTTP 没有 users；Mixed 只有 socks5_users。`crates/proxy/src/adapters/mixed/inbound.rs` 将 HTTP 分支交给默认 HTTP handler；SOCKS5 用户不会认证 HTTP 请求。这是接口明确存在的能力缺口，本报告不把它描述为绕过一个已经实现的 HTTP 认证器。

sing-box 相同端口的两个分支均使用同一个 authenticator。[sing-box Mixed](https://github.com/SagerNet/sing-box/blob/25a600db24f7680ad9806ce5427bd0ab8afe1114/protocol/mixed/inbound.go#L128)。HTTP CONNECT 出站在 Xray、sing-box、Mihomo 均有执行路径。[Xray HTTP client](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/proxy/http/client.go)、[sing-box HTTP outbound](https://github.com/SagerNet/sing-box/blob/25a600db24f7680ad9806ce5427bd0ab8afe1114/protocol/http/outbound.go)、[Mihomo HTTP outbound](https://github.com/MetaCubeX/mihomo/blob/ac017cdd246ce8bd547653d927e7bf77d7ee73d5/adapter/outbound/http.go)。

### Trojan 扩展不能按名称混算

Zero `protocols/trojan/src/mux.rs` 使用 Mux.Cool 域名与帧协议；Trojan-Go v0.10.6 的 mux server 使用 `xtaci/smux`。二者不是同一扩展。Zero 目前没有 WS/gRPC 配置入口，而 sing-box 的 Trojan transport 配置和 Mihomo 的 WS/gRPC 执行分支存在。

[Trojan-Go smux](https://github.com/p4gefau1t/trojan-go/blob/2dc60f52e79ff8b910e78e444f1e80678e936450/tunnel/mux/server.go#L34)、[Mihomo Trojan transport](https://github.com/MetaCubeX/mihomo/blob/ac017cdd246ce8bd547653d927e7bf77d7ee73d5/adapter/outbound/trojan.go#L80)。缺少 Trojan-Go 专有扩展不自动算欠账，但不能用已有 MUX 证明该扩展对齐。

### HY2 的生态缺项已有固定上游证据

Hysteria app/v2.6.1 的 `app/cmd/client.go` 已实际接入 Salamander、UDP hop、CA 和 pin，并非拿未来版本的新功能要求旧基线。sing-box 同样存在 obfs 和端口相关配置。Zero 现有 `Hysteria2` 出站 ADT 和 `transport/connection.rs` 没有对应接线。

[Hysteria client](https://github.com/HyNetwork/hysteria/blob/401ed5245d9bdfe0a35d629a8d977f256da07a75/app/cmd/client.go#L231)、[sing-box HY2 options](https://github.com/SagerNet/sing-box/blob/25a600db24f7680ad9806ce5427bd0ab8afe1114/option/hysteria2.go)。

## Mieru/HY2 已有成熟度工作，不应重复列为缺失

- Mieru：`client/pool` 有池身份、负载与退休策略；`inbound/multiplex/reader` 有单会话和连接总背压预算；`packet` 有 ACK/window/重传、乱序、关闭 tombstone 和 CUBIC。已有零窗口、重复/乱序、重传耗尽、close 与 flush、短写/取消等针对性测试。
- Mieru 官方包 harness 有 8 个 ignored interop 入口，覆盖双向 TCP/UDP 底层、多会话与停滞隔离、池复用、MTU/traffic pattern。这里是官方包构建的受控 harness，不等同完整官方 CLI 全部功能验收。
- HY2：池 key 包含认证、TLS 身份、参数和出口 generation；有 single-flight、reload 退休与活流 guard；UDP 由单读者按 session ID 分发；有 BBR、Brutal 补偿和自适应窗口的实际接线。
- HY2 的官方 interop 子模块已含并发 TCP/UDP 单次认证、匹配丢包环境下带宽比较、BBR profile 和高 RTT 窗口测试。上一轮如果只读顶层 echo 用例，会低估已有验证设计。
- 以上是源码和测试内容确认，本轮未执行这些外部互通、弱网或长期压力测试，不代表它们在本次 HEAD 上已经通过。

## 不列为必补项，以及撤回的候选问题

- SOCKS5 BIND/FRAG：主流工具也不提供完整实现，FRAG 还是 RFC 可选能力。维持明确拒绝即可。
- Shadowsocks 旧 AEAD replay：本次确实复现同一 TCP 握手被两次接受，但 SS-Rust v1.21.2 的默认策略也允许，Detect/Reject 为可选策略。因此不与 F1/F2 同列为确定性缺陷。[SS-Rust policy](https://github.com/shadowsocks/shadowsocks-rust/blob/a03006a753486e64717d6e3afa91e0c6d043c557/crates/shadowsocks/src/context.rs#L80)。
- Mieru 的 Zero 池策略：不同于官方 multiplexing-factor preset 是有意设计，不等于协议错误。
- VMess 旧 alterId、SS 旧 stream cipher、自定义 zero-aead、Trojan-Go 特定管理面：没有本项目实际需求证据，不自动进入必补清单。
- REALITY 的所有指纹/伪装特性、XHTTP 全部高级配置、所有节点组合：本轮没有完成逐项行为等价证明，也不因存在配置字段便声称完整对齐。

## 本次验证与复现

在协议各自 tests 目录临时放置最小探针，执行：

```sh
RUST_MIN_STACK=16777216 cargo test -p vmess -p shadowsocks --all-features --test audit_replay_20260911 -- --nocapture
```

4 个审计探针成功复现预期观察，另有复用 VMess validation 源文件带入的 2 个校验测试通过。这些探针故意断言“缺陷行为确实发生”，所以测试显示 passed 不表示协议正确。日志中的关键观察：

```text
VMess identical captured handshake attempt 1: ACCEPTED
VMess identical captured handshake attempt 2: ACCEPTED
VMess AuthID timestamp=1 (1970): ACCEPTED
SS2022 packet id 0: accepted twice
SS2022 left boundary packet id 2048 after 4096: accepted twice
Control ordinary packet id 1: duplicate correctly rejected
Shadowsocks aes-128-gcm identical captured handshake attempt 1: ACCEPTED
Shadowsocks aes-128-gcm identical captured handshake attempt 2: ACCEPTED
```

探针归档：[VMess](protocol-audit-2026-09-11/vmess-probe.rs)、[SS](protocol-audit-2026-09-11/shadowsocks-probe.rs)、[完整日志](protocol-audit-2026-09-11/verification.txt)。复现时分别复制到对应协议的 `tests/audit_replay_20260911.rs`；VMess 探针的相对 path 引用按该位置解析。审计结束已经移除临时 tests 文件，仅保留报告和证据，不改变运行代码。

未执行全 workspace 门禁、官方二进制互通、线上请求或部署检查。报告提供的是固定代码下的源码对照及最小复现，不是完整安全认证或所有协议等价证明。

## 建议交付顺序

1. 修 F1/F2，增加拒绝性回归，处理 F5 状态生命周期。已有协议的安全和有界运行优先于增加节点类型。
2. 明确真实 Xray/HY2 基线；纠正 F3/F4 的配置承诺，保留准确能力范围。
3. 按产品需要补 HTTP/Mixed 认证、HTTP 出站、HY2 obfs/hop/CA、Trojan WS/gRPC。不要一次把所有候选功能都判为当前必需。
4. 每批按实际模式执行固定对端的双向互通及异常用例。复用已有 Mieru/HY2 测试，不重新以“写过几个 echo 测试”作为成熟度结论。

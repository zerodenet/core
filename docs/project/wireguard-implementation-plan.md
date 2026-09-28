# WireGuard 实现计划

本文固定 Zero `v0.0.3-dev.*` 开发线中的 WireGuard 范围、分层、运行路径与验收门禁。实现以当前 Zero 能力边界、WireGuard 官方协议和固定互操作基线为准，不把 WireGuard 当成普通的流式代理协议。

## 1. 版本与基线

- Zero 开发线：`v0.0.3-dev.YYYYMMDDHHMM`，只从 `develop` 发布。
- 协议基线：WireGuard protocol v1，以[官方协议说明](https://www.wireguard.com/protocol/)和[官方白皮书](https://www.wireguard.com/papers/wireguard.pdf)为准。
- 行为与配置对照：Xray-core `v26.3.27`，commit `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`。该基线用于固定 TCP/UDP、IPv4/IPv6、peer、allowed IP、keepalive、MTU 和 endpoint 解析的互操作目标，不要求复制 Xray 的 Go/gVisor/kernel-TUN 内部结构。
- 第二个互操作基线：[官方 wireguard-go](https://git.zx2c4.com/wireguard-go/refs/) `0.0.20250522`，tag 指向 commit `f333402bd9cbe0f3eeb02507bd14e23d7d639280`。`tests/reference/wireguard_go` 固定该 commit，使用无特权 userspace netstack 执行 payload 测试。
- Rust 协议引擎候选：固定 GotaTun `0.9.2`（已替换 BoringTun `0.7.1`）。只允许精确固定版本或审计后的仓库内补丁；不得跟随 `master`。在完成消息上限、重放、cookie、定时器、密钥擦除和畸形包审计前，不进入默认 `full` feature。
- `protocols/wireguard` 的 package version 跟随最终选定并固定的 Rust 协议引擎版本；Zero 产品版本仍由 workspace version 管理。
- WireGuard 的日常进度提交不自动生成新的 dev tag 或 Release；只有在目标进度完成并收到明确发布指令后，才执行版本晋级和打标。

当前实现进度：M1 已建立独立 protocol crate、密钥/地址/endpoint/peer 校验、allowed-IP 最长前缀 peer 选择、按已认证 peer 反查源地址，以及 outbound/inbound 配置契约；配置中的 private/pre-shared key 在 Debug 中脱敏，配置对象销毁时擦除其 String 缓冲区。`runtime` feature 已接入固定 GotaTun `0.9.2`，实现握手、加解密、16 字节数据填充、定时器调用和保守消息上限。M2 的客户端 UDP 栈已有主动 IPv4/IPv6 UDP、临时端口、分片/重组与有界队列；客户端 TCP 栈已有主动握手、双向流、FIN、重传、闲置清理和独立跨网 RTO 策略。已认证的 ICMPv4/ICMPv6 错误现在可按引用的 TCP/UDP 四元组关联：UDP flow 收到不可达等错误后关闭；收到 Packet Too Big 后按该 socket 的目标地址降低后续 UDP 包的分片 MTU，并保持 flow 可用；半开 TCP 遇到硬不可达会提前失败。TCP 收到 Packet Too Big 后按连接降低后续新发 TCP 段的 MSS，之前已发包的重传仍按降低后的 IP MTU 分片。L3 入站在 auto 模式下按既有 Router 决策选择最少转换的数据路径：出站具备 PacketSink 时，IPv4/IPv6 内层包保留原始源/目标并经共享 WireGuard 设备转发，TTL/Hop Limit 减一，回包按原始源地址送回入站；auto 模式的 TCP/UDP 仅在出站没有 PacketSink 时进入原有 Flow 栈；flow 模式先终止为 Flow，再通过出站可执行的转换发送。direct 的 ICMP echo 仍使用宿主 raw/ping datagram socket 真实探测；仅支持 TCP/UDP 的出站对 ICMP 明确拒绝。fallback 保留候选顺序，阻断与能力不兼容的候选不会暗中绕到 direct。出站 MTU 不足时，IPv4 DF=0 按出站 MTU 分片，IPv4 DF=1 与 IPv6 返回 Packet Too Big。direct 的本机 IPv4/IPv6 loopback 探测、单段 IPv4 往返，以及两段 WireGuard 的 IPv4/IPv6 原源地址 Echo、TCP、UDP 往返和 1280 MTU 下的大 UDP 分片重组已通过；真实 TUN、外部目标及故障恢复仍待验收。M3 的 opt-in `wireguard` 出站注册原生 PacketSink；通用 Flow→Packet 转换使同一 outbound/peer 的 TCP 连接和 UDP flow 共享握手、外层 socket 和 IP 栈。设备在启动、reload 或物理出口 generation 变化时按 peer 预备：保留身份及 egress 未变的设备，新设备构造和初始握手包发送成功后才发布；预备或后续 reload 阶段失败则关闭候选并保留旧状态。发布后的旧设备拒绝新 flow，最长 30 秒后强制终止任务；活动设备上限 128、退役队列上限 128。这里的“预备成功”不代表远端完成认证握手或业务可达，密钥材料也只能在最后一个持有者释放时擦除，不能宣称即时清除所有副本。M4 入站已实现 UDP bind、协议层握手公钥/receiver index peer 分发、allowed-IP 源验证、已认证 endpoint roaming，以及现有用户态 TCP/UDP 栈与通用路由接入；Zero ↔ Zero 的 TCP/UDP 双向 payload（含超 MTU 分片）和新端口绑定失败时旧会话继续可用，已通过本地集成测试。同地址同端口的 WireGuard 入站配置更新现在复用原 UDP socket；密钥与计时身份未变的同位置 peer 保留认证会话，改变身份的 peer 重新握手。Zero outbound 到 Xray `v26.3.27` inbound 的 TCP/UDP echo，以及 Xray outbound 到 Zero inbound 的 TCP/UDP echo，均已在 macOS 上通过。wireguard-go 的双向 payload、两个入站 peer、约 134 秒持续收发及本地端口 roaming已在旧引擎上通过，故障恢复场景、其余平台和生产安全审计尚未完成，不能将当前能力视为完整 WireGuard 或生产可用。

双向端点模式已通过 `inbound_tag` 接线：一份 peer Noise 状态和一个入站 UDP socket 同时服务入站 Packet 与出站 Packet/派生 TCP/UDP。本地两端点双向 TCP/UDP、超 MTU UDP 及单端点两个 peer 之间的原始 Packet 转发已通过。共享端点同端口的 peer 配置变更在设备预备完成后提交，失败的候选监听端口绑定保留旧端点；`outer_udp_proxy` 也可用于共享端点。密钥轮换时业务连接是否全程无丢包、外部网络故障恢复仍待验证。

以下 BoringTun 审计段落保留为 2026-09-24 至 2026-09-26 的历史决策记录，不代表当前依赖。当前依赖及未关闭的门禁见下方“2026-09-26 GotaTun 迁移”。

引擎审计阻断项（截至 2026-09-24）：候选 BoringTun `0.7.1` 有未关闭的[会话单向失联报告](https://github.com/cloudflare/boringtun/issues/495)和[未按 16 字节填充传输包的报告](https://github.com/cloudflare/boringtun/issues/494)；[消息数拒绝上限补丁](https://github.com/cloudflare/boringtun/pull/478)仍未合入。当前 opt-in runtime 在送入引擎前自行填充数据包，并对出站数据消息数与入站数据 counter 都采用更保守的 `2^32` 拒绝界限，但这些措施不等于引擎整体审计完成。本地检查的 `boringtun-0.7.1/src/noise/session.rs` 中，`Session` 持有两份 `ring::aead::LessSafeKey`，未见显式 `Drop` 擦除；其 `ring-0.17.14/src/aead/chacha.rs` 内部 key 存为 `[u32; 8]`，也未见显式擦除。因此 Zero 外层配置密钥的 zeroize 不能证明引擎会话密钥即时擦除。报告尚须在固定版本上独立复现和审计；不可仅凭 issue 判定全部部署都会触发。在修复、替换或取得充分反证之前，不开放默认 `full`。`0.7.1` 仍只是候选基线，不是已审计通过的最终选型。

安全门禁逐项记录：

| 项目 | 已有证据 | 尚未关闭的部分 |
|---|---|---|
| 数据包填充 | Zero 发送前按 16 字节补齐；本地长度与 wireguard-go payload 测试通过 | BoringTun `0.7.1` 自身仍不填充；若未来绕过 Zero 包装器，不能继承此结论 |
| 数据消息上限与重放 | Zero 出入站采用 `2^32` 保守上限；接收边界与重复密文拒绝有聚焦测试 | 上游消息数补丁未合入；完整计数轮转、cookie 压力和畸形包覆盖未完成 |
| 定时器与重协商 | 25 秒 keepalive 下约 134 秒本地双向 payload 持续通过 | 会话单向失联报告尚无固定版本反证；真实外部网络与故障注入未完成 |
| 密钥擦除 | Zero 配置和预备 profile 的 key 缓冲区使用 zeroize | `Session` 持有的 `ring::aead::LessSafeKey` 缺少可证明的即时擦除路径；不得以外层 zeroize 声称整条持有链已擦除 |

因此本轮不判定 BoringTun 安全审计通过，也不改变 `wireguard` opt-in 状态。下一步需先形成可审计的引擎补丁或替换候选，再复测上述门禁；仅增加互操作时长无法解决密钥擦除缺口。

2026-09-25 复核：固定的 `boringtun=0.7.1` 本地源码仍在 `Session` 中保存两个 `ring::aead::LessSafeKey`，没有可见的会话密钥擦除实现；上游 #494、#495 仍为开放状态，#478 消息上限补丁未合入。Zero 包装层的 16 字节填充与保守计数器拒绝已有测试，但不能证明引擎会话密钥擦除，也不能排除上游 #495 报告的失联场景。保持 opt-in 和 Experimental；不能据此开放默认 `full`。

2026-09-26 引擎候选复核：固定 BoringTun `0.7.1` 的 `RateLimiter` 在所有来源之间共用握手计数，cookie 输入只有来源 IP；因此 Zero 的直连 UDP 入站数据路径现已保留完整外层 `SocketAddr`，并传入协议层，避免在中性 raw-IP 边界丢失来源端口。经 `outer_udp_proxy` 的 carrier 当前只能提供配置目标地址，不能提供经代理观察到的真实来源，须在引擎迁移时单独处理 cookie 的地址来源。当前 BoringTun API 仍只接收 IP，这次接口修正本身**没有**修复 cookie 行为。[GotaTun `0.9.2`](https://github.com/mullvad/gotatun/blob/v0.9.2/CHANGELOG.md) 是后续替换候选：其 `RateLimiter` 对来源 IP 独立计数，cookie 使用 IP 与端口，并在引擎内处理填充和消息拒绝上限；但其 [2026-02 审计](https://github.com/mullvad/gotatun/blob/v0.9.2/audits/2026-02-17-Assured.md)早于 `0.9.2`，报告明确将外部依赖排除在范围外，并指出敏感数据仍缺少完整内存保护。GotaTun 的纯协议 `Tunn` 接口还改为拥有 packet buffer 和随机 receiver index，不能仅替换 Cargo 依赖，否则会破坏当前多 peer demux。引擎迁移须在 `protocols/wireguard` 内完成固定版本的索引映射、收发及定时器适配，再以 wireguard-go/Xray、cookie 压力、重放、失联故障注入和密钥生命周期测试验收；在此之前不改变默认 feature 或生产能力声明。

剩余工作按性质区分：出站 Health 查询已投影按 peer 的未启动、endpoint 未解析、未握手、最近握手、数据可达、退化和停止状态；DNS 解析失败仍使启动/reload 返回错误，旧 device 可达时保留旧状态并单独标记最近一次解析失败，后续解析成功会清除该标记。wireguard-go 固定版本的 Zero 出站 IPv4/IPv6 TCP/UDP 双向 payload、wireguard-go 入站 IPv4 TCP/UDP 双向 payload，以及经 Zero 在两个 wireguard-go peer 间转发的 IPv4/IPv6 payload 均已在 macOS 本地通过。出站测试设 25 秒 keepalive，45 轮约 134 秒持续双向收发跨越通常的 rekey 时间窗口；这证明该场景持续可用，并不单独证明所有 rekey 行为。已认证 Zero 入站 peer 从一个外层 UDP 源端口迁移到另一个端口，且响应抵达新端口的本地测试通过。真实 TUN、外部目标、多 peer 故障恢复、三平台构建和全 workspace 门禁仍待验收。GotaTun 安全审计仍是默认启用前的阻断项，审计结论可能要求引擎补丁。任意 TCP/UDP 代理出站无法承载原始 ICMP；外层 UDP 的具体 Relay 组已接线，但动态组和不支持 packet-path 的成员仍不可用。kernel-TUN/`wg-quick` 不在首期范围。

| 项目 | 当前状态 | 还需完成 |
|---|---|---|
| L3 入站经 WireGuard PacketSink | 已实现自动路径；两段 IPv4/IPv6 Echo、TCP、UDP 与大包分片重组通过 | TUN 真实设备、外部目标及故障验收 |
| 出站健康状态中的握手/数据可达性 | 已实现按 peer 投影；未启动、endpoint 未解析、认证收包后可达、停止及退化/新握手状态转换有聚焦测试 | 长时间运行验收 |
| WireGuard 外层 UDP 经 packet-path 出站 | `outer_udp_proxy` 支持单个具体出站或具体成员构成的 Relay 组；SOCKS5 单跳及 SOCKS5→Shadowsocks 多跳的双向 TCP/UDP payload 本地通过；代理端重启后保持 WireGuard 出站设备并恢复 UDP 往返的本地测试通过 | 其余故障类型、动态组、其余 carrier 和跨平台验收 |
| wireguard-go 双向互操作 | 固定 commit userspace peer 的 IPv4/IPv6 出站、IPv4 direct 入站、IPv4/IPv6 两 peer Packet 转发 payload 已在 macOS 本地通过；同一 Zero 入站接受两个独立 wireguard-go peer 的 IPv4 TCP/UDP payload；25 秒 keepalive 配置下约 134 秒持续收发通过 | 多 peer 真实失联或重协商故障、真实网络与其余平台验收 |
| GotaTun 默认启用 | 安全审计未完成，仍为 opt-in | 关闭审计阻断项后再评估默认 `full` |

数据平面采用 Packet/Flow 双平面与 capability graph：Engine 先确定候选出站及 fallback 顺序，Proxy 只在当前候选内部寻找成本最低的可执行路径。当前运行时注册 Packet→Stream（TCP）、Packet→Datagram（UDP）、Stream→Packet（TCP）和 Datagram→Packet（UDP）；后两条边仅在 PacketSink 提供共享用户态栈转换操作时可执行。WireGuard 注册原生 PacketSink，并通过通用边为 SOCKS 等 Flow 入站提供 TCP/UDP；本地 SOCKS→WireGuard→WireGuard 入站的双向 payload 已在 `route.final_mode=packet` 下通过。配置层支持规则 `mode` 与 `final_mode` 的 `auto`、`packet`、`flow`、`translate`，默认 `auto`；强制路径不存在时明确失败，不悄悄转 Direct。通用 Direct PacketSink 尚未实现；Direct 的特定 ICMP Echo 探测仍是独立宿主 socket 路径。只有具备 PacketSink 的出站接收完整 IP 包；仅具备 Stream/Datagram 的出站不能接收 ICMP。候选失败才按配置顺序尝试下一个，Block 与能力不兼容终止本次 Packet 选择。WireGuard 出站以原始源地址发送 Packet，需要远端 AllowedIPs 与回程路由覆盖该源；同一共享设备上来自不同入站的重叠源地址被拒绝，不隐式执行 NAT。外层 UDP 可通过 `outer_udp_proxy` 引用具备双向 packet-path 的具体出站，或引用成员均为具体出站的 Relay 组；SOCKS5 单跳和 SOCKS5→Shadowsocks 多跳的 TCP/UDP 本地往返已通过。

通用 Direct PacketSink 是 Packet/Flow 总方案中的独立扩展，不计入 WireGuard 端点的完成门槛。当前 Direct 只有 Stream/Datagram 能力及特定 ICMP Echo 探测；L3 入站选中 Direct 时，TCP/UDP 可走 Packet→Flow，其他无法转换的 IP 协议明确失败。如果以后要求将原始 IP 包直接送入宿主网络并收回包，Zero 需要新增可执行的 Direct PacketSink 和平台 L3 收发适配；这项独立工程不由 WireGuard 实现代办。宿主转发、NAT、防火墙和回程路由由操作系统及部署环境负责，Zero 不因选择 Direct 隐式改动它们。WireGuard 原生 PacketSink、WireGuard 入站到出站的 Packet 路径，以及现有 TCP/UDP Direct Flow 路径均不依赖这项扩展。

## 2. 范围决定

发布 `v0.0.2-rc.*` 后，仓库进入两条长期并行线：

- `main` 只维护 `0.0.2-rc.*`，接收 RC 缺陷修复、兼容性修正和优化；后续候选版本继续使用递增的 `v0.0.2-rc.YYYYMMDDHHMM`。
- `develop` 只推进 `0.0.3-dev.*`，WireGuard 及其所需的通用网络栈、trait 和 adapter 工作不得提前进入 `main`。
- 同时适用于两条线的修复先按发布风险选择落点；从 `main` 进入的通用修复必须受控 forward-port 到 `develop`。不得再次将浮动 `develop` 整体合入 `main`，也不得让未完成的 WireGuard 随修复反向进入 RC。
- 两条线分别执行版本契约、CI 和发布标签；`main` 的 RC 成功不代表 `develop` 的 WireGuard 已验证，`develop` 的 dev 成功也不改变 RC 的发布范围。

WireGuard 是承载 IPv4/IPv6 包的三层隧道，不是建立一条应用层 TCP 字节流的协议。因此实现分为两个方向：

1. **出站**：Zero 的 TCP/UDP 会话通过一个共享的 WireGuard peer/device 进入远端网络。必须具备可主动建立 TCP 和 UDP 会话的用户态客户端网络栈。
2. **入站**：UDP socket 收到的 WireGuard 包在鉴权解密后产生 raw IP 包，再交给现有 `zero-stack` 终止 TCP/UDP，并进入统一入站路由与会话观测流水线。

首个可合并里程碑先完成出站；入站在同一 `v0.0.3-dev.*` 开发线中作为独立里程碑实现和验收。任何里程碑都不得以“能握手”替代双向 payload、重协商、keepalive、MTU 和故障恢复证据。

首期明确不包含：

- 操作系统 kernel WireGuard 接口、`wg-quick`、路由表或 iptables 管理；
- 从 WireGuard 配置自动修改宿主机 DNS；
- 未经定义的 Xray 私有字段透传；
- 将 WireGuard 密钥、握手包或解密后的 raw IP 内容写入事件、状态快照或普通日志；
- 在没有 packet-path 证据时宣称 WireGuard 外层 UDP 可以经过任意 relay chain。

## 3. 分层与所有权

### `protocols/wireguard`

- 拥有 private/public/pre-shared key、peer、allowed IP、reserved bytes、握手、cookie、重放窗口、rekey、keepalive 和 WireGuard 包编解码。
- 提供 `validation` 与 `runtime` feature；配置层只依赖 `validation`。
- 接收抽象时钟、随机源和 UDP/raw-packet I/O，不直接创建 Tokio socket、TUN 设备、路由表或进程任务。
- 不依赖 `zero-config`、`zero-engine`、`zero-proxy` 或具体控制面。

### `crates/traits`

- 只在确有跨运行时需求时增加最小的客户端网络栈或 raw-IP device trait。
- trait 不出现 WireGuard、peer、key、allowed IP 等协议名或协议私有字段。
- 不把 Tokio 类型带入 `zero-traits`。

### `crates/stack`

- 现有 `UserNetworkStack` 保持 raw-IP 入站终止职责。
- 新增独立的客户端网络栈：接收 WireGuard 解密后的 IP 包，主动提供 TCP connect 与 UDP socket，并把生成的 IP 包送回 WireGuard device。
- TCP 状态机、UDP 端口分配、分片/MTU/ICMP 错误属于网络栈，不进入协议 crate 或 proxy adapter。
- 客户端栈与现有入站栈共享纯 packet 解析/构造代码，但不把两个方向的生命周期混成单一 facade。

### `crates/config`

- `OutboundProtocolConfig::Wireguard` 和 `InboundProtocolConfig::Wireguard` 分别表达客户端与服务端配置。
- 同一个设备需要双向工作时，出站的 `inbound_tag` 明确引用一个 WireGuard 入站；两侧必须具有相同的 private key、MTU 和有序 peer 表。出站保留本地地址和 peer endpoint，入站保留监听地址。该引用只合并设备与 socket 所有权，不改变 Engine 的路由决策。
- 只保存配置 ADT、结构/引用校验，并调用 `protocols/wireguard` 的 validation API 校验密钥和协议私有值。
- 不复制 base64/hex key parser，不产生运行态 peer/device，也不持有明文派生密钥。

### `crates/platform/tokio`

- 实现协议/栈所需的 UDP socket、timer、task 和网络解析适配。
- socket 的 egress/bind 行为继续走现有 platform/runtime 服务，不由协议 crate 直接 `UdpSocket::bind`。

### `crates/proxy`

- `WireguardAdapter` 只做协议配置投影、出站共享 device 生命周期、入站协议设备到中立 raw-IP listener 的薄桥接和显式 capability 实现。
- TCP 使用 `TcpOutboundCapability` 的同步 preparation 边界；执行期返回客户端网络栈建立的 stream，通用 runtime 继续负责 relay、accounting、结果归一和错误映射。
- UDP 使用 `UdpFlowCapability`；协议 adapter 不复制通用 UDP session/cache/manager。
- reload 时以配置身份复用或替换共享 device；密钥、peer、endpoint、allowed IP、MTU 变化必须构建新状态并原子切换，失败保留 last-known-good。
- 不在 runtime 根、`tcp_dispatch` 或 `udp_flow` 中增加对 `OutboundProtocolConfig::Wireguard` 的 match。

### `zero-engine` 与控制面

- 继续只接收通用 session、stats、health 和事件，不识别 WireGuard 密钥或包格式。
- 能力发现可以报告 TCP、UDP、IPv4、IPv6、inbound/outbound 和编译 feature；不得把一次握手成功解释为完整健康。

## 4. 配置契约

出站配置的目标形状如下，最终字段名以 config tests 固定：

```yaml
outbounds:
  - tag: wg-out
    protocol:
      type: wireguard
      private_key: BASE64_OR_HEX_32_BYTES
      addresses:
        - 172.16.0.2/32
        - fd00::2/128
      mtu: 1420
      peers:
        - public_key: BASE64_OR_HEX_32_BYTES
          pre_shared_key: OPTIONAL_BASE64_OR_HEX_32_BYTES
          endpoint: example.com:51820
          allowed_ips:
            - 0.0.0.0/0
            - ::/0
          keepalive_secs: 25
          reserved: [0, 0, 0]
```

多 WireGuard 出站的网段编排由 Zero 配置层完成：`route.auto_outbounds` 列出参与自动路由的具体出站标签。配置编译时从这些出站的已校验 `allowed_ips` 生成普通 IP 路由规则，按最长前缀排序；用户写在 `route.rules` 的显式规则先执行，`route.bypass` 仍优先于规则。不同出站声明同一个规范化前缀时拒绝配置，不能隐式依赖标签或数组顺序。未列入 `auto_outbounds` 的出站不参与自动路由；其他协议将来可通过配置层的前缀投影扩展，Engine 和 Proxy 路由运行时不读取 WireGuard 私有字段。

例如两条出站 `wg-a`、`wg-b` 分别声明 `10.10.0.0/24, 192.168.0.0/23` 和 `10.0.0.0/8` 时，使用 `"auto_outbounds": ["wg-a", "wg-b"]`，`10.10.0.0/24` 会优先于 `10.0.0.0/8`。入站流量仍需由 TUN 或其它入站进入 Zero；`auto_outbounds` 不修改系统路由表。`mode=direct/global` 会按已有模式语义覆盖规则模式。

DNS 与 IP 目标路由独立。只有需要使用内网 DNS 的出站才在 `runtime.dns.servers` 定义带该出站 `detour` 的服务器，并在有序 `dispatch` 中用域名或域名规则集选择它；其它域名由 `default_server` 处理。本例只把 `192.168.1.180` 绑定到 `wg-a`，不从历史 `wg-b` 配置引入 `1.1.1.1`。需要把 `policy.node_server` 指向不经过该 WireGuard 出站的解析器，以避免 endpoint 解析递归；机密域名不能设置会回退到公共解析器的 fallback。带 detour 的普通 UDP DNS 优先使用出站提供的一次性 Datagram 能力；WireGuard 通过共享 Packet device 执行 UDP 53 查询。出站不支持该能力，或响应带截断标志时，才尝试原有 TCP 53 路径。固定 wireguard-go 对端的 A DNS 只监听 UDP 53，双出站测试已验证此路径。

校验要求：

- private/public/pre-shared key 解码后必须恰好 32 bytes；错误信息不得回显密钥。
- 至少一个本地 tunnel address 和一个 peer；地址必须带 prefix。
- outbound peer 必须有 endpoint；域名解析沿用 Zero DNS 与 egress 策略。
- `allowed_ips` 不能为空，peer 选择使用 longest-prefix match；相同优先级的冲突必须拒绝，不能依赖配置顺序静默覆盖。
- `reserved` 只能为空或 3 bytes；非零值是显式兼容扩展，不属于标准 WireGuard 能力。
- MTU 默认 1420；IPv6 启用时不得低于 1280。上限、封装开销与分片行为必须由测试固定。
- keepalive 使用秒，`0` 表示禁用；范围必须适配最终协议引擎的无损类型。
- 私钥和 peer 公钥不得相同；重复 peer、公钥或完全重复 allowed-IP 声明必须拒绝。

入站配置使用相同的 `private_key`、`mtu` 和 peer key/allowed-IP 规则，但 peer 不设置 `endpoint`；监听地址和端口使用通用 `inbounds[].listen`。入站最多 128 个 peer，只有完成协议认证的报文才可更新该 peer 的外层 endpoint。

双向端点在出站配置增加 `inbound_tag: wg-in`，并配置对应的 `inbounds[].tag: wg-in`。此模式下中性 raw-IP listener 独占外层 UDP socket，协议层仅维护一份多 peer Noise 状态；出站 Packet/主动 TCP/UDP 经有界内层包队列进入该 listener，加密后用同一 socket 发出。入站解密后的包先按活动 Packet 回程、TCP/UDP 客户端 flow 匹配；未匹配的包继续走既有入站 Packet/Flow 路由。没有 `inbound_tag` 的配置仍使用独立出站和入站设备，保持原有兼容性。
共享端点同端口变更会预备新的协议设备、peer 栈与可选外层 carrier；配置提交时复用监听 socket 和有界内层包队列，切换 peer 状态与设备池。候选阶段失败时丢弃新栈并保留旧端点。一次更新先切换 WireGuard 监听端口、再让另一个 TCP 入站遇到端口占用时，协调器会恢复整批旧入站；本地双入站回归中原共享会话继续完成 UDP 往返。同端口 keepalive 变更后双向 UDP、SOCKS5 外层 packet-path 下共享端点的 TCP/UDP 往返已通过。外层代理端重启后，共享端点保持同一进程并在重建 carrier 后恢复 UDP 往返；单次外层发送错误只丢弃该报文，不退出整个监听任务。运行中的 linked ↔ unlinked 双向切换已有本地 UDP 回归测试；成功的端口迁移和跨密钥轮换的长连接连续性尚未通过验收。

固定 wireguard-go 参考端的 IPv4/IPv6 TCP、UDP 与 45 轮持续双向收发已改用共享端点模式运行，本地测试 1/1 通过，持续阶段耗时 133.72 秒；候选外层 packet-path 缺少双向载体时的 reload 被拒绝，旧共享会话继续完成 UDP 往返。该记录验证共享端点的参考互操作与局部回滚，不证明外部网络故障恢复或安全审计完成。

## 5. 运行路径

### 出站 TCP

```text
route decision
  -> ProtocolRegistry / WireguardAdapter prepare
  -> shared WireGuard device
  -> client network stack TCP connect
  -> encrypted IP packets over UDP carrier
  -> generic TCP relay/accounting
```

### 出站 UDP

```text
UDP session
  -> UdpFlowCapability prepare
  -> client network stack UDP socket
  -> raw IP packet
  -> WireGuard encrypt / peer selection
  -> UDP carrier
  -> response decrypt / client stack / generic UDP response path
```

### 入站

```text
UDP listener
  -> WireGuard cookie/handshake/auth/decrypt
  -> allowed-IP source validation
  -> raw IP packet
  -> zero-stack TCP/UDP termination
  -> common inbound route/session/accounting pipeline
```

外层 endpoint 的 DNS、socket 创建和 egress 选择属于 runtime/platform 服务。内层目标的 DNS 是否在 tunnel 内解析必须是明确配置与测试结论，不能因系统 resolver 行为偶然成立。

## 6. 生命周期与安全不变量

- 一个配置身份对应一个共享 device；不得为每条 TCP 连接重新握手或创建独立 peer 状态。
- 空闲时仍运行 rekey、cookie reset、keepalive 和 session expiry 定时器。
- 只在通过鉴权的包上接受 endpoint roaming；先校验 receiver index、MAC、AEAD、counter/replay window，再更新 endpoint。
- ingress raw IP 的 source 必须匹配已认证 peer 的 allowed IP；egress destination 必须通过 longest-prefix peer 选择。
- 遵守 WireGuard 的 message/time/key rejection limits；达到上限必须 rekey 或拒绝，不能继续复用旧 keypair。
- 所有 key material 使用可擦除容器；reload、失败和 shutdown 都要清除旧状态。
- 有界化 handshake queue、decrypt queue、fragment buffer、UDP association 和 per-peer 状态；不得让未认证包创建无界任务或会话。
- 配置、Debug、错误、metrics、snapshot、event 和 panic 路径都不得输出 private/pre-shared key。
- reload 先完整校验和构建新 device，再原子替换；失败不影响旧 device。旧 device 有界 drain 后终止。
- outbound health 至少区分：未启动、endpoint 未解析、未握手、最近握手、数据可达、退化和停止。目前 Health 查询按 peer 报告这些状态；数据可达只由通过来源校验的解密 IP 包触发。域名解析失败仍使启动/reload 返回错误；如旧 device 仍可用，保留它的状态并在独立字段标记候选 endpoint 解析失败。

## 7. 实现里程碑

### M0：开发线与契约

- 开启 `v0.0.3-dev.*`；
- 固定本文、基线 commit、feature 名和配置样例；
- 完成 Rust 协议引擎依赖/许可证/安全审计结论。

### M1：协议与验证

- 新建 `protocols/wireguard`；
- key/peer/allowed-IP validation 与最长前缀 peer 选择、已认证 peer 源地址反查；
- handshake、transport data、cookie、replay、timers 的单元与向量测试；
- 私密字段 redaction 和 zeroize 测试。

### M2：客户端网络栈

- 在 `zero-stack` 增加主动 TCP/UDP 栈；
- 已完成主动 TCP/UDP 的独立内存包收发及 opt-in WireGuard device 接线；ICMPv4/ICMPv6 错误可反馈给匹配 UDP flow 和 TCP 连接，Packet Too Big 可降低后续 UDP/TCP IP 分片大小并缩小后续新发 TCP 段的 MSS；L3 入站自动选择 PacketSink 或 TCP/UDP Flow；两段 WireGuard 的 IPv4/IPv6 原源地址 Echo、TCP、UDP 往返及小 MTU 分片重组通过，真实 TUN、外部目标与故障恢复仍待验收；
- IPv4/IPv6、ICMP、MTU、fragmentation、TCP close/reset、UDP error 的确定性测试；
- 使用纯内存 raw-IP device 测试，不依赖特权 TUN。

### M3：出站 adapter

- config variant、feature wiring、registry registration；
- prepared TCP/UDP operations、共享 device、reload/reconcile、按 peer 的 Health 查询；解析失败记录只在匹配配置身份时投影，保留活跃旧 device 的可达状态；
- direct outer UDP；显式 `outer_udp_proxy` 可复用单个双向 packet-path 出站或由具体出站构成的 Relay 组，SOCKS5 单跳与 SOCKS5→Shadowsocks 多跳的本地双向 payload 已通过；代理端重启后的 UDP 恢复已通过本地测试，其余故障类型仍需验收。

### M4：入站 adapter

- UDP bind/preparation 进入 `InboundListenerCapability`；
- neutral raw-IP peer operation 进入 runtime-owned listener/task 生命周期；
- 复用 `zero-stack` 与 common inbound route，不建立 WireGuard 专用路由流水线。

### M5：生产验收

- 全 workspace fmt、check、clippy、all-features tests 和 release build；
- 固定端口外部套件串行执行；
- 双向 TCP/UDP payload、IPv4/IPv6、DNS、MTU/大包、keepalive、rekey、endpoint roaming、多 peer、reload 和故障恢复；
- Linux/macOS/Windows 构建；运行态特权测试与非特权用户态测试分别报告；
- 产出 capability matrix，分开标记已实现、已互操作验证、仅单元验证、缺失和明确排除。

## 8. 外部互操作矩阵

最低接受矩阵：

| 方向 | 对端 | TCP | UDP | IPv4 | IPv6 | keepalive/rekey |
|---|---|---:|---:|---:|---:|---:|
| Zero outbound | wireguard-go 固定 commit | 必须 | 必须 | 必须 | 必须 | 必须 |
| Zero outbound | Xray `v26.3.27` | 必须 | 必须 | 必须 | 必须 | 必须 |
| Zero inbound | wireguard-go 固定 commit | 必须 | 必须 | 必须 | 必须 | 必须 |
| Zero ↔ Zero | 同版本 | 必须 | 必须 | 必须 | 必须 | 必须 |

每个通过项都必须验证请求与响应 payload，而不是只验证 handshake、socket 建立或进程存活。外部套件共享固定端口时串行运行，timeout/reset 必须隔离重跑后再归因。

### 2026-09-24 本地 capability/验收记录

| 能力或门禁 | 状态 | 证据与边界 |
|---|---|---|
| Zero outbound ↔ wireguard-go | 已实现且已互操作验证 | 固定 `f333402b` userspace reference，IPv4/IPv6 TCP、UDP 请求与响应 payload；45 轮约 134 秒持续运行，25 秒 keepalive 配置 |
| wireguard-go → Zero inbound direct | 已实现且已互操作验证 | 两个独立 peer 的 IPv4 TCP、UDP payload；IPv6 direct 未单独验证 |
| wireguard-go → Zero → wireguard-go | 已实现且已互操作验证 | 两个 peer 的 IPv4/IPv6 Packet 路由 payload |
| Zero ↔ Xray `v26.3.27` | 已互操作验证一部分 | macOS 本地 IPv4 TCP/UDP 双向 echo；IPv6、长期运行待验收 |
| 双 WireGuard 出站与内网 DNS 分流 | 已实现且已互操作验证一部分 | 两个固定 wireguard-go peer 同时运行；`10.10.0.0/24` 覆盖 `10.0.0.0/8`，两边 TCP/UDP payload 成功；A 的域名经仅监听 UDP 53 的 DNS 解析再建立 TCP。新版 B 三 peer 配置无需测试前缀覆盖，同一 Zero 进程中 B Web 返回 HTTP 200 且 A UDP DNS 有效；B 第三 peer 的外层握手没有收到回复，长期运行待验收 |
| Zero outbound endpoint 漫游 | 已实现且有本地集成验证 | 对端从第二个 UDP 端口发送认证握手响应，Zero 随后将业务包发送到第二端口；伪造报文没有抢占 endpoint；跨 IPv4/IPv6 地址族移动及外部故障注入尚未验证 |
| 非 Echo ICMP 经 WireGuard PacketSink | 已实现且有本地集成验证 | IPv4 Timestamp Request 从 Zero WireGuard 入站经另一 WireGuard 出站，到独立 peer 解密后保持源/目标与 ICMP 负载，TTL 减一；回程、IPv6 非 Echo 和真实 TUN 待验收 |
| Endpoint 解析失败 Health、同端口 reload、ICMP/PTB、MSS、资源上限、认证后入站端口 roaming | 已实现且有本地测试 | 单元或本地集成验证；roaming 测试从第二个 UDP socket 收到返回 payload，仍不能替代外部网络故障注入 |
| 多 peer 故障隔离、真实 TUN/外部目标 | 部分验证 | 双 peer 正常 payload 已通过；协议层注入一个 peer 的损坏密文后，另一个已认证 peer 仍能双向收发；真实失联、重协商故障、TUN 和外部目标仍待验收 |
| 外层 UDP Relay 组、kernel-TUN/`wg-quick` | Relay 组局部实现 | `outer_udp_proxy` 支持具体成员组成的 SOCKS5→Shadowsocks 组，代理端重启后 UDP 恢复已通过本地测试；动态成员、其余故障类型和系统接口未接线 |
| GotaTun 安全审计、默认 `full` | 阻断 | 固定版本的密钥生命周期与完整互操作审计未关闭；保持 opt-in |

测试命令：先在 `tests/reference/wireguard_go` 运行 `go build -mod=readonly -o /tmp/wireguard-go-reference .`，再设置 `WIREGUARD_GO_BIN`、`WG_SUSTAINED_ROUNDS=45`，运行 `RUST_MIN_STACK=16777216 cargo test -p zero-proxy --features wireguard --test wireguard_go_interop -- --ignored --test-threads=1 --nocapture`。CI 的独立 workflow 在 Linux 上重复固定外部参考测试，但 CI 配置本身尚不能算通过记录。

本轮本机验证：`cargo fmt --all -- --check`、`git diff --check`、`cargo check --workspace`、`wireguard` runtime 10 项（含新增损坏 peer 隔离）、API forward-compat 7 项、Proxy raw-IP 10 项、WireGuard 入站 3 项、Packet 路由 7 项、wireguard-go 互操作 3 项（其中持续场景约 134 秒）、Go module verify/build/test，以及 `wireguard` 和上述 Proxy 集成目标的定向 Clippy 均通过。后续复核发现 Packet 路由与 Direct ICMP 的 live 用例曾以环境开关提前返回却计作通过；现已改为明确的 ignored 测试，CI 使用 `--ignored` 显式运行。修改后，Packet 路由 7/7、Direct ICMP 探测 1/1 和 WireGuard 入站 Direct ICMP 1/1 均以 `--ignored` 真实执行并通过；默认测试明确报告 ignored，不能把它们计作默认套件的已执行项。Direct 非 Echo ICMP 不再占用 Echo 路径的会话固定状态，聚焦回归测试通过；同一 Zero 入站的两个 wireguard-go peer IPv4 TCP/UDP payload 也通过。全工作区 `cargo clippy --workspace --all-targets` 运行约 12 分钟后按时间边界中止，未得到完整结果；`cargo test --workspace --all-features` 的首次尝试在 721.5 秒时主动中断，第二次在 900 秒上限中断（退出码 -2），日志中 68 个测试目标已结束且无失败，但全套件没有通过/失败结论。release build、Linux/Windows 与真实 TUN 验收未执行。以上缺口继续阻止 M5 验收。

后续聚焦验证：`wireguard_packet_non_echo` 的非 Echo IPv4 Packet 转发测试在 `wireguard` feature 和 Proxy all-features 两种形态下均真实执行 1/1 通过；`wireguard` 协议层的损坏 peer 密文隔离测试真实执行 1/1 通过。首次隔离测试命令带 `--exact` 仅匹配到 0 项，已用实际匹配的命令重跑，不计入通过数。独立 CI workflow 已加入这两项，但尚无远端 CI 执行记录。固定 BoringTun `0.7.1` 的本地源码再次显示 Session 持有两份 `ring::aead::LessSafeKey` 且无显式 Drop 擦除；其填充逻辑有上游 TODO，Zero 的发送前填充有本地测试，仍不关闭密钥擦除或会话失联审计。上述测试只缩小 M5 缺口，不改变 opt-in 和安全阻断结论。

同日追加验证：双 WireGuard 出站固定 wireguard-go 测试真实执行 1/1 通过，覆盖重叠网段最长前缀、两边 TCP/UDP payload 和 A 的 UDP-only DNS 分流。出站 endpoint 漫游集成测试真实执行 1/1 通过，协议认证及重放测试 1/1 通过。两份用户提供的原生 WireGuard 配置仅在临时目录转为 Zero 内核 JSON，未增加客户端转换代码、系统路由或第二份 DNS；配置校验通过。首轮真实端点试验中，A 的 `192.168.1.180:53` 连续收到匹配查询 ID、RCODE 0、2 条答案的 DNS 响应，HTTP Health 显示 `wg-a=reachable`；B 显示 `wg-b=recently_handshaken`，当时仅证明认证握手。第一次临时进程在 15 秒内未启动 SOCKS 监听，随后多次独立启动和 A DNS 查询均约 0.2 秒成功；该首次启动延迟尚未归因。临时配置和进程已清理。真实配置试验仍不构成长期运行、故障恢复、安全审计和跨平台验收。

用户停用本机 WireGuard 后给出 B 的业务目标 `16.10.1.2:8000`。桌面历史 B 文件的 `AllowedIPs` 实际只有 `10.0.0.0/8`，不覆盖该目标；复测只在临时 Zero 内核配置的 B peer 上增加 `16.10.1.2/32`，不改原文件。使用原文件其余密钥、地址和 endpoint，同一 Zero 进程中的 B SOCKS CONNECT 成功，目标 Web 返回 `HTTP/1.1 200 OK`；随后 A 的 `192.168.1.180:53` 返回匹配查询 ID 且 RCODE 0 的 DNS 响应。该业务测试验证 B TCP 与 A UDP DNS 能在双出站下实际分流，不证明 B 全部 `16.0.0.0/8` 可达，也不替代长期运行或安全审计。`cargo check --workspace`、`cargo fmt --all -- --check` 和 `git diff --check` 通过。

随后收到新版 B 三 peer 配置：本地地址 `16.10.68.1/32`，首 peer 声明 `16.0.0.0/8` 与 `16.1.1.1/32`，另两个 peer 分别声明 `16.1.1.4/32`、`16.1.1.2/32`。这次临时内核配置使用新版 B 原有 AllowedIPs，没有添加测试专用前缀；原生 `ListenPort` 属于系统监听设置，不用于 Zero 出站。双出站配置校验通过；同一 Zero 进程内，B 的 `16.10.1.2:8000` 返回 `HTTP/1.1 200 OK`，A 的 `192.168.1.180:53` 返回匹配查询 ID、RCODE 0 的 DNS 响应。启动约 0.5 秒时 B peer 0 为 `reachable`、peer 1 为 `recently_handshaken`、peer 2 为 `awaiting_handshake`；单独延长至 5 秒的 B 健康检查仍显示 peer 2 等待握手。前两个 peer 的握手状态不能证明其各自业务网段可达，第三个 peer 等待握手也尚不能区分远端状态与网络原因。测试未把用户密钥或临时 JSON 写入仓库。

第三个 B peer 后续诊断：临时 UDP 转发器只改该 peer 的外层 endpoint 为本机转发端口，保持密钥、内层地址和对端目标不变。25 秒内观察到 Zero 发出 5 个 WireGuard type 1 握手发起包，收到 0 个对端报文，Health 持续为 `awaiting_handshake`。同样的转发器用于 B peer 0 时，观察到 type 1 发起、type 2 响应和 type 4 keepalive，Zero 进入 `recently_handshaken`，说明本机转发观测路径可工作。再以固定 wireguard-go reference 和相同客户端密钥、第三个 peer 公钥及 endpoint 发起独立握手，约 15 秒内发出 3 个 type 1 报文，也收到 0 个回复。由此可确认“等待握手”的直接原因是这次测试未收到第三个 endpoint 的 UDP 响应；证据不能区分网络过滤、远端未监听、远端公钥配置不一致等情况，也不能证明该 peer 的业务网段可达。所有临时进程与含密钥配置均已清理。

进一步排除本地来源端口与目标端口问题：本机 UDP 51820 可绑定，使用该端口转发第三个 peer 的握手，15 秒内发出 3 个 type 1 报文，仍无回复；将目标端口临时改为常见的 UDP 51820，10 秒内发出 2 个 type 1 报文，仍无回复。目标主机 ICMP ping 两次均回应，而本机没有该地址的已知 SSH 配置或可用的远端管理入口。以上观察只证明目标 IP 可达、客户端发包、所测 UDP 端口没有返回 WireGuard 报文，不能证明服务端进程、其公钥配置或远端防火墙的具体状态。需要远端 `wg show`、UDP 443 监听状态及服务端抓包，才能区分报文未到达与到达后被服务端丢弃；在这些证据出现前不应修改 Zero 的路由或静默回退到其它 peer。

用户补充说明原生 macOS WireGuard 的 `mac-home` 配置可启动并生效。提供的截图显示三位 peer、接口公钥、`16.10.68.1/32` 和 `ListenPort=51820` 与新版 B 配置一致，但截图时隧道状态为“未激活”；本机随后查询 `scutil --nc status mac-home` 为 `Disconnected`，`16.1.1.2` 仍走 `en5` 默认路由。因此不能用该截图确认第三位 peer 已握手，也不能再把 Zero/wireguard-go 试验未收到 UDP 回包直接归因为远端故障。原生客户端激活后应单独核对 `16.1.1.2/32` 所属 peer 的最新握手及实际数据收发，并与同一时段的 Zero 测试对照。当前 Zero 出站按 peer 分配独立外层 UDP socket，原生接口配置则指定单个 `ListenPort`；这是待比较的实现差异，尚未证明它是故障原因。

用户随后激活 `mac-home`：系统状态变为 `Connected`，`utun3` 持有 `16.10.68.1/32`，`16.1.1.2/32` 明确走 `utun3`，而第三 peer 的外层 endpoint 仍走物理 `en5`。原生扩展确实绑定 UDP 51820；经隧道访问 `16.10.1.2:8000` 返回 HTTP 200，`16.1.1.1` 和 `16.1.1.4` 各 2/2 收到 ICMP Echo 回复，证明前两个 peer 对应的网段可用。`16.1.1.2` 三次 Echo 请求均超时，不能单凭 ICMP 判定第三 peer 未握手；仍需原生应用显示的该 peer 最新握手时间和收发字节，或同目标已知服务的业务请求，才能与 Zero 的握手观测作同类对照。

用户随后提供已激活状态的逐 peer 统计截图：前两个 peer 都显示接收字节和约 1 分 34 秒前的最新握手，第三 peer（`47.120.34.17:443`，`16.1.1.2/32`）仅显示已发送约 2.75 KiB，没有接收字节，也没有最新握手。由此可确认原生 WireGuard 的整体连通只覆盖前两个 peer；第三 peer 在原生客户端上同样未完成握手，与 Zero 和固定 wireguard-go reference 各自发出握手发起包却未收到响应的结果一致。Zero 按 peer 建立外层 socket 与原生共用 UDP 51820 的差异不是该现象的充分解释。下一步需在第三 peer 所属服务端或其入口网络上核查 UDP 443 到包、监听状态、服务端 peer 公钥/允许地址以及防火墙回包；当前证据仍不能区分这些远端/路径原因，不应针对 Zero 路由或握手实现作无证据修改。

用户确认第三 peer 所属设备已离线，因此其未握手属于预期的外部状态，当前 B 实网验收只关注前两个在线 peer。原生客户端已验证两者各自的 `16.1.1.1`、`16.1.1.4` ICMP 响应和最新握手；Zero 现有记录仅确认 peer 0 的 `16.10.1.2:8000` HTTP payload，以及 peer 0/1 的握手状态，尚需对 Zero 经 peer 1 的 `16.1.1.4` 实际请求与响应做聚焦复测。第三 peer 不参与此次两在线 peer 连通性判断，也不因其离线状态修改 WireGuard 路由或协议实现。

原生 `mac-home` 断开后，使用新版 B 原样三个 peer（含已知离线的第三 peer）生成权限为 0600 的临时 Zero 内核配置，仅启动 SOCKS 入站和 `wg-b` 出站，不引入客户端配置转换。`zero validate` 通过；经 Zero SOCKS 发起两个独立 HTTP 请求，peer 0 覆盖的 `16.10.1.2:8000` 返回 HTTP 200 和 95 字节 body，peer 1 的更具体 `16.1.1.4/32` 返回 HTTP 200 和 1033 字节 body。两条真实业务路径均得到响应，第三 peer 离线未阻止前两者工作。临时配置目录与 Zero 进程已清理。本次验证关闭“Zero 对第二个在线 peer 缺少 payload 证据”的局部缺口；不等同于 M5 的长期运行、故障恢复、安全审计或跨平台验收。

DNS 复核：先以 A 单隧道经 Zero SOCKS UDP ASSOCIATE 向 `192.168.1.180:53` 查询 `example.com`，收到匹配查询 ID、QR=1、RCODE 0、2 条答案的 61 字节 DNS 响应。随后用 A+B 同时运行、B 保留三个 peer 的临时 Zero 配置测试 `runtime.dns`：`example.com` 指向 `wg-a-dns`（UDP 53、`detour=wg-a`），未匹配域名由 `system` 处理，`node_server=system`，无 fallback。通过 `diagnostics.dns_lookup` 实测 `example.com` 返回 4 个地址，查询记录显示 `server_tag=wg-a-dns`、`transport=udp`、`outbound=wg-a`、`success=true`；`iana.org` 返回 1 个地址，记录为 `system`、`direct`、成功。同一进程经 SOCKS 访问 B 的 `16.10.1.2:8000` 和 `16.1.1.4:80` 均为 HTTP 200，分别收到 95 和 1033 字节。临时配置与进程已清理。该验证覆盖真实 A DNS 应答、域名选择与默认解析隔离，以及 B 同时工作；不证明用户尚未指定的其它域名规则集已经配置或验收。

首次调用 HTTP `diagnostics.dns_lookup` 时发现同步 `CommandService::execute` 在异步 `execute_acknowledged` 内直接 `block_on`，触发 Tokio 嵌套运行时 panic。现将非配置替换命令移到 `spawn_blocking` 执行，保持同步命令契约并允许诊断在 HTTP/IPC/gRPC 异步入口调用；新增 `acknowledged_dns_lookup_runs_from_async_control_plane` 回归用例，聚焦执行 1/1 通过。修复后上述 HTTP DNS 诊断和双隧道实网用例通过。

本轮按仓库规范尝试 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features`，UTC 08:11:20 开始，约 13 分 51 秒完成编译并开始测试，UTC 08:26:20 达到预设 900 秒上限后停止。日志中 3 个已结束的测试目标共 34 项通过、0 失败，其余目标未执行完成；这不是全量测试通过记录。完整测试日志保留在 `/tmp/zero-wg-dns-full-20260924.log`，含明确开始、结束、状态和退出码。
`cargo check --workspace`、`cargo fmt --all -- --check` 和 `git diff --check` 均通过。

用户指定目标复核：在原生 A/B 隧道均停用时，同一临时 Zero 进程保留 A+B 出站与新版 B 三 peer，用精确域名规则将 `jms.yt.starmerx.com` 交给 A 的 `192.168.1.180:53`。`diagnostics.dns_lookup` 返回 `192.168.1.119`，查询尝试均显示 `server_tag=wg-a-dns`、`transport=udp`、`outbound=wg-a`、`success=true`。同进程经 Zero SOCKS 访问 B 的 `16.10.1.2:8000` 得到 HTTP 200 和 95 字节 body；经 A 到 `192.168.1.235:5432` 的 SOCKS TCP CONNECT 返回成功。数据库测试仅确认 TCP 连接建立，未做协议识别、认证或查询。临时配置与进程已清理，用户原生隧道仍停用。

2026-09-25 双向端点收尾：共享 `inbound_tag` 端点的 peer、MTU、密钥、endpoint 或外层 egress 变更已在候选准备成功后发布；同监听地址复用 socket，失败的监听协调会恢复已切换的旧监听。共享端点的外层 UDP 可以走已有双向 packet-path，SOCKS5 单跳及代理端重启后的 UDP 恢复已由本地 Zero ↔ Zero payload 测试覆盖。运行中的 linked 与 unlinked 模式相互切换仍要求重启进程；密钥轮换期间既有业务流全程无丢包、真实 TUN 和外部网络故障恢复仍未验收。固定 wireguard-go `f333402b` 的共享端点 IPv4/IPv6 TCP、UDP 请求与响应持续 45 轮，133.72 秒，测试进程退出码 0。`wireguard_inbound` 在 Proxy all-features 下 5 项通过、1 项需要 live ICMP socket 而默认 ignored；`wireguard_packet_non_echo` 2 项通过；协议 runtime 11 项通过；配置 WireGuard 13 项通过。`cargo check --workspace`、WireGuard feature 的最小构建、格式与 diff 检查以及 CI scope 的 20 项单元测试通过。新增 Linux/macOS/Windows CI WireGuard feature 构建门禁，但远端三平台结果尚未产生。本机首次交叉检查 `aarch64-apple-darwin` 在依赖编译阶段达到 300 秒上限并以 124 退出，不能作为 ARM 构建通过记录；Linux/Windows 缺少目标 C 交叉编译器。完整 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features` 以 1800 秒显式上限运行，98 个已结束测试目标均通过且无失败，到 `connector_webhook` 目标开始时到达上限并以 124 退出；这不是全工作区通过。随后单独执行该目标的 4 项测试全部通过，进一步确认它不是导致全量套件停住的单独故障。日志分别保留在 `/tmp/zero-wg-linked-go-interop-20260925.log`、`/tmp/zero-wg-inbound-all-features-20260925.log`、`/tmp/zero-wg-packet-all-features-20260925.log`、`/tmp/zero-connector-webhook-isolated-20260925.log`、`/tmp/zero-wg-macos-arm-check-20260925.log` 和 `/tmp/zero-wg-full-verified-20260925.log`。固定 BoringTun 引擎的安全审计和默认启用门禁仍未关闭。

2026-09-26 增量验证：直连 raw-IP listener 将 UDP 源 `SocketAddr` 传至 `protocols/wireguard`；固定 BoringTun 分支暂以 `source.ip()` 调用旧引擎，保持运行行为。协议 runtime 11/11、Proxy WireGuard 入站 4/4（另 1 项 live ICMP 默认 ignored）、`cargo check --workspace` 通过。按规范运行完整 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features`，1200 秒上限内 79 个已结束测试目标全部通过、无失败；达到上限时刚开始 `tun_route_reconcile_e2e`，退出码 124，进程组已终止，因此不能称全量通过。随后单独运行该目标成功编译，但目标本身报告 0 项测试，不能据此补全剩余全工作区测试。日志位于 `/tmp/zero-wg-socketaddr-runtime-20260926.log`、`/tmp/zero-wg-socketaddr-inbound-20260926.log`、`/tmp/zero-wg-socketaddr-workspace-check-20260926.log`、`/tmp/zero-wg-full-socketaddr-20260926.log` 和 `/tmp/zero-wg-tun-route-isolated-20260926.log`。

2026-09-26 GotaTun 迁移与端点热切换：协议层固定 `gotatun=0.9.2`，替换旧 BoringTun 引擎；迁移了多 peer receiver-index 映射、握手、加解密、定时器与已认证包分发，仍保持 WireGuard 协议状态位于 `protocols/wireguard`，中性 Packet/Flow 与 raw-IP listener 留在 proxy runtime。外层 packet-path carrier 现向端点传递解码出的 UDP 源地址和端口；不提供来源的 carrier 仍只传配置中的 endpoint，因此不能把该 fallback 当作真实来源证明。关联与非关联端点可经 reload 双向切换；当入站协议身份不变时保留 Noise 会话，候选监听绑定失败时回滚到旧路径。协议 runtime 定向测试 12/12、Proxy all-features 入站 5/5（另 1 项需 live ICMP socket 而默认 ignored）、非 Echo Packet 测试 2/2 已通过。固定 wireguard-go `f333402b` 的四个新引擎互操作目标在最终 all-features 二进制中实际执行 4/4 通过，含 45 轮约 144 秒的关联端点持续收发；固定 Xray `v26.3.27` 官方包经 SHA-256 与提交号核验，最终 all-features 二进制的双向 TCP/UDP 互操作 3/3 通过。GotaTun 入站和出站握手限速器在接收时重置过期计数；超过阈值出现 cookie 挑战、一秒后恢复的聚焦测试通过。故障恢复、真实 TUN、跨平台与 workspace 全量门禁需另行验收。GotaTun 的[公开审计](https://github.com/mullvad/gotatun/blob/v0.9.2/audits/2026-02-17-Assured.md)在 `0.9.2` 之前完成，排除外部依赖，并指出敏感材料缺少内存保护；本地 `ring` 后端也没有可证明的会话密钥即时擦除路径。因此保留 opt-in，不能宣称已关闭生产安全门禁。

同日全工作区门禁：`RUST_MIN_STACK=16777216 cargo test --workspace --all-features` 从 UTC 15:19:45 运行至 15:49:45，编译成功，47 个已结束测试目标均通过且无失败；1800 秒上限到达时刚开始 `vless/tests/mux.rs`，退出码 124，Cargo 与其测试进程组均已停止。日志为 `/tmp/zero-wg-full-gotatun-final-20260926.log`。这不是全工作区测试通过记录；WireGuard 的固定参考互操作、协议与 Proxy 定向结果见上一段。

最终定向日志：`/tmp/zero-wg-runtime-final-20260926.log`、`/tmp/zero-wg-rate-limit-test-20260926.log`、`/tmp/zero-wg-all-features-inbound-binary-20260926.log`、`/tmp/zero-wg-all-features-packet-binary-20260926.log`、`/tmp/zero-wg-go-final-all-features-20260926.log` 和 `/tmp/zero-wg-xray-final-all-features-20260926.log`。测试二进制来自上述全工作区 all-features 构建；固定 Xray 官方包及校验输出保留在 `/tmp/zero-wg-xray-download-20260926.log`。

2026-09-27 外层来源语义修正：中性 raw-IP carrier 现在保留 `Option<SocketAddr>`，只在载体实际报告来源时向 GotaTun 提供该来源并更新已认证 peer 的漫游 endpoint；来源不明时不再把配置 endpoint 冒充为收到报文的来源。关联入站仍可用配置 endpoint 向该载体回包，协议适配器仅使用固定的未知来源哨兵调用握手限速器，因此来源不明的载体仍不具备可证明的真实来源漫游能力。WireGuard key 解析的临时 32 字节缓冲区改为退出作用域时擦除；这不证明 GotaTun / `ring` 内部会话密钥即时擦除。能力描述移除已失效的“关联/非关联热切换需重启”，并列明不透明外层载体的来源限制。协议 crate 的路由 3/3、运行时 12/12、校验 13/13，Proxy all-features WireGuard 库测试 10/10、入站集成 5/5（另 1 项 live ICMP 默认 ignored）已通过；日志见 `/tmp/zero-wg-lib-source-20260927.log` 与 `/tmp/zero-wg-inbound-source-20260927.log`。真实外部连通、完整工作区门禁与安全审计仍未由这些定向测试关闭。

本轮 `cargo check --workspace` 在约 36 秒内以 0 退出，`cargo fmt --all -- --check` 与 `git diff --check` 通过。按仓库规范尝试 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features`，UTC 17:19:27 开始，420 秒显式上限时仍在编译测试目标，进程组已停止并以 124 退出；没有已完成的测试目标，不能称全量通过。开始、结束与退出码记录在 `/tmp/zero-wg-full-source-20260927.log`，工作区检查日志在 `/tmp/zero-wg-workspace-check-20260927.log`。

2026-09-27 固定引擎密钥持有补丁：根工作区通过 `[patch.crates-io]` 使用仓库内的 GotaTun `0.9.2`，原包 SHA-256、许可证、变更范围与剩余风险见 `vendor/gotatun/ZERO-PATCH.md`。Zero 的 `zeroized-rustcrypto` 特性让会话 AEAD key 由实现 `ZeroizeOnDrop` 的 RustCrypto `ChaCha20Poly1305` 持有，同时启用 Poly1305 临时 MAC 状态的擦除；GotaTun 静态私钥启用 `x25519-dalek/zeroize`，持有中的握手链式密钥、限速器 secret 和预共享密钥在销毁或替换时擦除，握手 Debug 遮蔽敏感字段。最终特性组合下，补丁自身测试 50/50、Zero WireGuard 路由 3/3、运行时 14/14、校验 13/13、固定 Xray `v26.3.27` 双向互操作 3/3、固定 wireguard-go `f333402b` 互操作 4/4 均通过。切换 AEAD 后的 wireguard-go 45 轮持续 IPv4/IPv6 TCP/UDP 收发也 4/4 通过，持续目标约 144 秒；随后启用 Poly1305 擦除特性，又以最终构建重跑了 4/4 基础互操作。公开 GotaTun 审计报告过 RustCrypto 相对 `ring` 的吞吐下降，Zero 工作负载尚未测量。临时栈副本、外部依赖内部状态、真实 TUN、故障恢复、跨平台和全工作区门禁仍未关闭，保持 opt-in。

同日全工作区门禁再次尝试：`RUST_MIN_STACK=16777216 cargo test --workspace --all-features --offline` 从 UTC 06:40:31 运行至 06:50:31，600 秒上限到达时仍在编译 `zero` 测试目标，退出码 124；已完成测试目标数为 0，进程组已停止。日志 `/tmp/zero-wg-full-final-20260927.log` 包含开始、结束、退出码和耗时。这不是全工作区通过记录；后续应通过更长的 CI 运行完成门禁，避免把本机短时间上限的重复尝试当作验收。

补丁后 Proxy all-features 入站集成测试 5/5 通过（另 1 项需 live ICMP socket 而默认 ignored），非 Echo Packet 集成测试 2/2 通过，覆盖共享端点双向业务、漫游、SOCKS5→Shadowsocks 外层 Relay 与 peer 间 Packet 转发。`cargo check --workspace --offline`、协议 crate 定向 Clippy、工作区格式和补丁文件格式检查通过。

新版 B 实网复测：以用户提供的三 peer 参数构造权限为 0600 的临时内核配置，`zero validate` 退出码 0；本轮构建的 `zero` 仅监听临时本机 SOCKS 端口，peer 0 覆盖的 `16.10.1.2:8000` 返回 HTTP 200 / 95 字节，peer 1 更具体的 `16.1.1.4:80` 返回 HTTP 200 / 1033 字节。已知离线的 peer 2 不计入验收。测试前确认这两个外层 endpoint 均走物理 `en0`，原生 A 隧道仍处于连接状态、原生 B 仍断开；Zero 测试进程与含密钥临时目录均已清理。这证明两条在线业务路径在当前网络和本轮二进制上可用，不验证来源不明的外层代理载体、长期故障恢复或引擎内存安全。

新增来源不明载体的本地双向 UDP 回归：载体接收时始终返回 `None` 来源，WireGuard 仍完成握手，两个先后打开的 UDP flow 均得到真实加密往返 payload；测试 1/1 通过，日志 `/tmp/zero-wg-opaque-payload-20260927.log`。这只覆盖来源不明时的正常数据收发，不宣称可从该载体获知真实来源或支持真实来源漫游。

2026-09-27 安全边界增量：`protocols/wireguard` 的 runtime 显式启用 `x25519-dalek` 的 `zeroize` feature，编译期测试要求 GotaTun 使用的 `StaticSecret` 具备 `Zeroize`；该类型在固定依赖源码中通过 `zeroize(drop)` 擦除。来源不明的外层载体仍以固定哨兵进入 GotaTun 的 MAC1/限速校验，但达到 cookie 阈值后直接拒绝握手，不发出也不接受能被误认为“真实来源持有证明”的哨兵 cookie；计数器一秒后可恢复。真实来源载体保留原有 IP:port cookie 行为。此修补不覆盖 GotaTun `Session` 中的 `ring::aead::LessSafeKey`、握手临时 key 和其它外部依赖的内存持有链；默认启用门禁仍未关闭。

上述修改后的协议 crate 路由 3/3、运行时 14/14、校验 13/13，Proxy all-features WireGuard 入站 5/5（另 1 项 live ICMP 默认 ignored），`cargo check --workspace`、格式与 diff 检查均通过。按规范再次尝试 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features`，UTC 04:41:45 开始，1800 秒上限时终止进程组，退出码 124；75 个已结束测试目标共 613 项通过、0 失败、7 项按定义跳过，到时刚进入 `phase1_port_conflict`。完整日志 `/tmp/zero-wg-full-source-limit-20260927.log` 含开始、结束与退出码。这是局部通过证据，不是全工作区门禁通过。

同轮 `cargo check -p zero-proxy --no-default-features --features wireguard` 通过；该最小 feature 组合仍输出其它条件编译路径的大量 unused/dead-code 告警。macOS ARM `aarch64-apple-darwin` 的 `cargo check -p wireguard --features runtime` 在 279.8 秒内退出码 0，日志 `/tmp/zero-wg-arm-check-20260927.log`。这只是协议 crate 交叉类型检查；完整 Proxy 的 ARM 构建、Linux/Windows 和 ARM 实机 payload 仍未验收。

## 9. 合并门禁

WireGuard feature 进入默认 `full` 之前必须同时满足：

1. 协议引擎安全审计项关闭，依赖精确固定；
2. config/runtime 边界测试确认 generic runtime 不匹配 WireGuard config variant；
3. 出站 TCP/UDP 的 wireguard-go 与 Xray 固定基线互操作通过；
4. reload/last-known-good、密钥 redaction、资源上限和 shutdown 测试通过；
5. 三平台编译与 workspace 全量门禁通过；
6. 文档明确列出未经外部验证的 inbound、未实现的 relay-chain 或 kernel-TUN 能力；未完成项不得由 capability discovery 报告为生产支持。

## 2026-09-28 TUN 接入回归

本机应用测试发现，命令启动 TUN 后 egress generation 改变，但已发布的
WireGuard 设备只在启动和配置 reload 时预备。设备池因此拒绝新 TCP、UDP 和
DNS 请求，报 `raw-IP peer device is not active`。保持 TUN 运行并重新应用配置后，
域名 DNS detour 和 SOCKS/HTTP 到远端隧道内业务恢复，证明这不是远端离线。

修复在中性物理出口控制中增加 generation 通知，由现有 Proxy orchestration
串行预备和发布当前配置的 outbound devices。拓扑在异步预备期间再次改变时
丢弃候选，失败时保留旧池并按 1 至 30 秒退避重试；不把协议逻辑移入 TUN。
启动预备完成后保留 watch 通知交由运行循环处理，避免检查 generation 与
确认通知之间的竞态吞掉更新。

原始 Packet 路径继续保留来源地址，不隐式执行 NAT。普通主机 TUN 的地址与
WireGuard 分配地址不同，而远端只允许后者时，TCP/UDP 可配置规则 `mode=flow`：
入口先通过 Packet→Flow 转换终止连接，再通过已注册的 Flow→Packet 操作使用
隧道本地地址。这是允许的组合路径；强制 Flow 不应因最终 sink 是 Packet 而
拒绝已实现的转换。ICMP 没有 Flow 转换，强制 Flow 仍拒绝 ICMP；需要面向普通
主机的 TCP/UDP 与 ping 混合接入时，使用下述显式 `translate` 模式。

路径锁定按 TCP/UDP 四元组隔离，保留旧连接的平面，同时允许新端口连接采用
更新后的规则。此前按源 IP、目标 IP 和协议锁定会阻止同地址的新连接切换平面。

此外，TUN port-53 interception 只处理实际进入 TUN 的查询。系统解析服务器的
物理路由保护可能使应用 DNS 绕过 TUN；内核主动 DNS 查询成功不能证明系统
应用已接管 DNS。宿主 DNS 指向或域名级 resolver 的部署、停止后的恢复由平台
控制者负责，不在选择 WireGuard 出站时自动修改宿主 DNS。

新增 topology generation、强制 Flow 到 Packet 出站、独立 TCP 连接平面锁定、
WireGuard TCP/UDP 无配置 reload 网络恢复的回归测试。测试使用的宿主 IPv4
可以通过 `ZERO_TEST_HOST_IPV4` 指定，必须是本机实际持有的非 loopback 地址；
默认路由的探测在已有 TUN 运行时可能返回 TUN 地址，不能据此构造宿主 echo 服务。
本机初轮全量执行发现这一问题：探测返回 `10.0.0.1`，导致 AllowedIPs 重复或
流量重新进入宿主 TUN。显式使用物理地址后，入站 5/5（另 1 项 live ICMP 默认
ignored）与新增 TCP/UDP 三轮网络恢复 1/1 均通过，断言没有调整。

工作区测试使用临时 APFS runner 复制并签名已有测试程序后执行，保留原参数、
环境和源码目录；避免本机接近容量上限的 HFS+ 工作卷启动测试程序时长时间等待。
完整工作区 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features`
最终启动通知竞态修正后，在 UTC 06:30:22 至 07:10:00 完成，
退出码 0，耗时 2378.0 秒：
308 个已完成目标、2185 项通过、0 失败、155 项按定义 ignored。
默认 ignored 的官方互操作、live ICMP 或特定平台测试不计入已验证能力。
日志为 `/tmp/zero-wireguard-tun-regression-final4-20260928.log`。
最终 `cargo check --workspace` 与 `cargo clippy --workspace --all-targets`
均退出码 0；Clippy 仍有既有 dead-code、参数数量和类型复杂度等告警。
日志为 `/tmp/zero-wireguard-tun-final-gates-final-20260928.log`。
优化版 `cargo build --release --all-features --locked` 在 UTC 06:56:33
完成，退出码 0；桌面签名后产物的
`--version` 在 2.655 秒内成功，配置校验在 0.189 秒内成功。
这是含未提交修复的本地验证构建，尚未替换客户端正在运行的内核，
域名级 resolver 也尚未应用；普通应用通过真实 TUN 的访问仍待用户切换后验证。


### 2026-09-28: explicit Packet address adapter for native application ping

用户已在实际客户端确认 A 的内网数据库和 yt 网站可访问。这是用户侧 TCP/域名
访问验收；不等同于 ICMP 验收。

原方案的 Flow 是 TCP Stream / UDP Datagram。新增 `mode=translate`（也支持
`route.final_mode=translate`）显式组合：TCP/UDP 使用既有 Flow 转换路径；ICMP Echo
使用当前选中 PacketSink 提供的源地址转换适配器。严格 `flow` 仍不接受 ICMP。
`auto` / `packet` 的原生 Packet 路径保留原始来源地址，不改变为隐式 NAT。
无 Packet 地址适配器的出站明确不支持 ping；不会另选 Direct。

通用栈 `zero-stack::echo_translation` 负责有界请求状态和回包关联；
`zero-stack::packet::icmp::translation` 负责纯地址、标识、校验和以及 ICMP 错误引用
还原。中性 raw-IP 设备负责队列、回包通道、关闭和周期清理。WireGuard 适配层
只提供既有协议计划、地址和设备池；握手、密钥、peer 索引和 AllowedIPs 仍由
`protocols/wireguard` 所有。Capability graph 保持 Packet / Stream / Datagram 三个节点。

IPv4/IPv6 Echo 的源地址转换为选中 peer 的隧道分配地址，返回时还原原始来源、
Echo identifier 和 payload；不同入站即使原始源、identifier、sequence 相同，也分配
独立的在途标识。ICMP 不可达、超时和 MTU 错误引用匹配请求时，还原被引用的
原始 IP/Echo 头，保留错误类型、代码、MTU 和路由器来源。请求上限 1024、原始及
转换包合计 2 MiB、超时 30 秒；回包或关闭释放，设备定时器清理超时和已关闭入口。
新 ping identifier 独立锁定路由平面；纯 IP/传输身份提取也在 stack 的 packet 层。
这不是任意 IP 协议的通用 NAT：非 Echo ICMP 不在 `translate` 支持范围，原生 Packet
仍可承载它们。WireGuard 远端也必须支持 Echo/允许对应目标。

审查记录见 `wireguard-packet-flow-audit-20260928.md`。本次额外将旧 ICMP 内联测试
移入独立 tests 文件，并将共享设备根文件拆为 driver / health / translation 模块。
新增纯栈测试、协议加密往返和中性边界测试。最终源码快照的全工作区测试在
UTC 08:23:17 至 08:52:40 完成，退出码 0，耗时 1763.4 秒：312 个测试目标，
2201 项通过、0 失败、155 项按定义 ignored。日志为桌面
`zero-wireguard-ping-test-20260928.log`；格式与 diff 检查均退出码 0。
`cargo check --workspace` 在 UTC 09:00:56 完成（495.6 秒、退出码 0），
`cargo clippy --workspace --all-targets` 在 UTC 09:11:32 完成（636.3 秒、退出码 0）；
已有告警仍保留。优化构建 `cargo build --release --all-features --locked` 在
UTC 09:49:14 完成（2122.4 秒、退出码 0），构建目录为
`target/wg-tun-build-20260928`。桌面最终产物签名验证通过，版本启动 1.70 秒、
新 JSON 校验 0.17 秒，均退出码 0。50 个 Rust 源文件快照与测试源码一致。
产物 SHA-256：`1aaf5b23456622d62617f61896187503932ccb15567965cd0b1d43bc7abcfc38`。
源码和构建记录见桌面 `zero-wireguard-ping-manifest-20260928.json`。本轮未提交、
推送或替换运行内核；A/B 真实网络 ping 待用户切换新内核验收。

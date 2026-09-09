# Hysteria2 官方实现对齐

本清单以 2026-09-08 确认的官方稳定版
[app/v2.12.2](https://github.com/HyNetworks/hysteria/releases/tag/app/v2.12.2)
和[协议规范](https://v2.hysteria.network/docs/developers/Protocol/)为对照。
本地 crate 的 `2.6.1` 是历史包版本，不是完整兼容性声明。
每项能力分别记录实现和可复现的外部验收；单元测试通过不能替代官方二进制互通证据。

对齐目标是线协议互通和明确需要的可观察行为，不要求复制官方应用的配置树、字段名或 Go 内部实现。
先检查 Zero 已有能力：语义等价时直接复用；仅表达方式不同则在 config 与拥有该协议的适配边界映射；
只有缺少的机制才增加实现。`protocols/hysteria2` 保持独立的协议策略与编解码，
不反向读取 Zero 配置，也不建立第二套路由、用户限速或控制面。

## 分层与验收

| 对象 | 实现状态 | 归属与后续验收 |
| --- | --- | --- |
| TCPRequest/TCPResponse、增量 varint 和长度边界 | 已实现 | `protocols/hysteria2`；确定性分段/粘包测试，官方 v2.12.2 与 sing-box 双向互通 |
| UDPMessage、分片/有界重组 | 已实现，部分外部验收 | 协议模块；官方端双向 1600 字节通过；补乱序、重复、超时、MTU 和多跳大包外部验收 |
| TLS 校验、`insecure` 与独立 `server_name` | 已实现 | 通用 `zero-transport::quic`；HY2 适配器传递策略，TCP/UDP/packet-path 一致 |
| 认证响应 UDP 能力、接收带宽解析 | 已实现 | 协议拥有解析和 UDP 拒绝；缺失/异常头按参考实现默认值处理 |
| 认证取消与 HTTP/3 任务回收 | 已实现 | 协议连接 guard；认证等待取消后对端可观察连接关闭 |
| 私有 CA、证书固定、客户端证书、ECH | HY2 尚未完整接通 | 已有 `ClientTlsConfig` / `QuicConfig.ca_cert_path` 和通用 TLS CA 加载；优先复用并接通 QUIC profile；证书固定、客户端证书、ECH 仍需逐项检查载体支持，不能据控制面 mTLS 推定 HY2 已支持 |
| 带宽配置、Brutal、BBR 配置 | 已实现，配置已统一 | Zero 只暴露 `up_bps/down_bps`；适配器映射协议收发参数；协议拥有协商与 Brutal，不再隐式附加 QUIC 硬上限或逻辑流预算；BBR 三档映射到共享采样与控制器，详细边界见 [BBR 对齐](bbr.md) |
| Salamander、Gecko | 待实现 | 协议拥有混淆编解码，transport 提供中立 datagram 包装；官方端双向和边界测试 |
| QUIC 窗口、保活、PMTU、连接复用/恢复 | 自动接收扩窗、参数和共享认证连接池已实现，有边界回归 | 固定/初始/最大窗口、RTT 驱动增长和流/连接协调见[窗口对齐](windows.md)；保活、PMTU；TCP/UDP/packet-path 共用认证连接、单次建连、重建、重载和空闲回收见[连接统一](connections.md)；长稳仍待验收 |
| HTTP/3 伪装站点/代理 | 已实现，有回归与互通证据 | 静态文件、固定 HTTP/HTTPS 源站代理、固定内容；认证前后持续 HTTP/3 分流；有界请求与任务回收 |
| 端口跳跃、Fast Open、Mimic、Realm | 待逐项设计和实现 | 根据官方稳定版源码列明载体依赖、平台限制和可测行为 |
| 官方应用的 ACL、DNS、TUN、管理/统计 API | 映射已有 Zero 能力 | 复用 engine/router/dns/tun/api；不在 HY2 内建立第二套应用或控制面 |

## 剩余工作按能力归属拆分

| 类别 | 剩余工作 | 实现原则 |
| --- | --- | --- |
| 已有内核能力复用 | 业务限速、用户策略、路由、DNS、TUN、统计 | 继续使用现有内核接口；HY2 带宽协商不能替代 principal 共享限速，详见[速率语义](transport.md#统一字段与连接带宽) |
| 共享传输能力接通 | 私有 CA 等 TLS 策略、QUIC 窗口与载体参数 | 复用配置/profile 和 `zero-transport`；当前 QUIC 参数执行已共享，HY2 配置表达仍独立；需要公共配置时扩展通用契约，不为每种协议复制 TLS/QUIC 执行代码 |
| 拥塞与窗口行为差异 | 载体调度及长稳性能 | BBR 三档和自动接收扩窗由共享传输层执行，均有固定官方状态参考和受控链路对照；Quinn 与 quic-go 的调度并非逐包等价，长稳仍待验收 |
| 协议与连接机制缺失 | Salamander/Gecko；端口跳跃、Fast Open、Mimic、Realm | 逐项界定协议策略和载体依赖，再实现与验收；共享认证连接与 UDP 分发已实现；任意网络恢复及长稳仍需单独验收 |
| 伪装服务边界 | 额外 HTTP/1、HTTP/2 监听、源站连接策略 | 当前只有同一 QUIC 端口上的 HTTP/3 伪装；若需要其他入口或共享出口策略，复用监听/HTTP 载体契约，不能仅因官方有配置便在 HY2 内另建应用服务 |
| 配置边界已统一 | `up_bps/down_bps` 的单位与范围 | 字节/秒整数，0/省略表示不声明本地固定带宽；不继承官方应用服务端的 65,536 字节/秒最小值；协议保留可表示范围验证，低速连接仍受超时约束 |
| 验收不足 | 受控丢包/延迟下的吞吐、长稳、UDP 乱序/重复/超时、多跳大包 | 补外部证据；不把未验收等同于未实现，也不把基础互通视为性能和全部功能等价 |

上述应用功能不构成逐项照搬的开发承诺。每个新增配置都应说明已有字段为何不能表达其语义，
并明确默认值、作用范围、方向、单位、重载行为和兼容规则。

## 推进顺序

1. 连接身份、认证能力协商、资源回收与缓存隔离。
2. 收发带宽与拥塞控制、QUIC 参数和连接恢复。
3. 混淆、伪装、其他载体能力及配置验证。
4. 官方稳定版与 sing-box 双向 TCP/UDP、packet-path、多跳和长稳故障验收。

所有开发提交在 `develop` 上线性追加，吸收 `main` 时先核对祖先关系，避免 merge commit。
协议或运行时行为变动执行工作区全量测试及分层回归；能力摘要保持 `partial`，直到对应外部验收闭环。

## 可重复外部验证

`Hysteria2 Interop` 工作流固定官方 Hysteria `app/v2.12.2` 和 sing-box `v1.13.14`，
下载后校验发布资产 SHA-256，再运行官方端基础四项、Brutal 双向扩展两项、受控 Brutal 丢包对照一项、三档 BBR 受控链路对照一项、接收窗口高时延对照一项、并发 TCP/UDP 单次认证一项和 sing-box 六项互通测试。
QUIC pacing、逐包反馈和接收窗口扩展执行独立依赖单元回归；固定官方源码生成 BBR 和接收窗口状态向量并校验仓库结果。
HY2、共享传输和代理相关改动自动触发，也可手动运行。
本地以 `HY2_BIN`、`SING_BOX_BIN` 指定同版本程序，执行该工作流中的同一 Cargo 命令。

2026-09-08，代码提交 `2932c882` 的[自动互通验收](https://github.com/zerodenet/core/actions/runs/34245710843)
通过全部 10 项测试（官方 4 项、sing-box 6 项），官方服务端保留默认 SNI 防护。
这份证据覆盖基础会话互通，不代表上表其余未完成项已经实现。

详细字段、默认值、连接恢复边界及与官方剩余差异见[传输与伪装配置](transport.md)。

2026-09-09，提交 `328a0344` 的[扩展互通验收](https://github.com/zerodenet/core/actions/runs/34250434280)
通过官方 6 项与 sing-box 6 项测试，包含配置固定带宽后双向 TCP/UDP 的 Brutal 协商路径。
独立 QUIC pacing 回归 6 项通过，HY2 运行时 11 项回归覆盖双向协商、丢包补偿、认证取消、
UDP 保活回收、TCP 连接池、持续 HTTP/3 分流、静态文件和固定源站代理。
这些结果不替代受控丢包、延迟、带宽整形下的长时间吞吐验收。

同一代码提交的[工作区 CI](https://github.com/zerodenet/core/actions/runs/34250434333)
通过 1477 项测试（87 项显式忽略）、严格 Clippy、Linux/macOS/Windows 平台检查和 musl 构建；
[特权 TUN 验收](https://github.com/zerodenet/core/actions/runs/34250434334)在三个平台均通过。
本轮额外执行 HY2 外部程序互通与特权 TUN 用例，不计入普通工作区通过数量；其余忽略项不标记为已验收。

2026-09-09，提交 `998caade` 将 Zero 配置统一为 `up_bps/down_bps`，并增加共享传输层的
QUIC 连接发送上限。[工作区 CI](https://github.com/zerodenet/core/actions/runs/34259033921)
通过 1483 项测试（87 项显式忽略）、严格 Clippy、三平台检查及 musl 构建；新增回归覆盖
无 principal 时多流与 datagram 共用连接上限、协议协商后保留上限、配置方向映射和预算取消。
[外部互通](https://github.com/zerodenet/core/actions/runs/34259033916)通过官方 6 项、sing-box 6 项及
独立 pacing 6 项；Zero 使用统一字段，官方端保留自己的 bandwidth 配置。
[特权 TUN 验收](https://github.com/zerodenet/core/actions/runs/34259033943)三个平台均通过。

### 带宽语义修正

`998caade` 的测试证明了硬上限机制生效，但该绑定会截断官方默认的 Brutal 丢包补偿，
不能作为官方拥塞行为一致的证据。本批解除 HY2 带宽与通用 pacing 上限、逻辑流默认预算的
隐式绑定；保留统一字段和显式认证条目的独立预算。

验收区分三类要求：`0`/`auto` 的协商含义属于线协议；默认启用的丢包补偿及其样本计算
属于固定版本的官方控制器行为；关闭补偿属于显式选项。参考版本是发布标签 `app/v2.12.2`
（提交 `619a6f856b69fb7ee6a7a379e810e68b84004605`），不是名为 `app` 的补偿分支。

新增回归通过真实 QUIC 参数和 HTTP/3 认证检查双向协商后的控制器，向控制器快照注入丢包反馈，
确认默认补偿可超过目标值、关闭补偿保留目标值、`auto`/零接收声明选择自适应算法。
另有 TCP/UDP 会话预算回归，确认顶层带宽不再作为默认预算，显式认证预算及其他协议默认值保留。

2026-09-09，提交 `7de4579d` 的 [Linux 外部验收](https://github.com/zerodenet/core/actions/runs/34306685930)
通过官方 7 项、sing-box 6 项、独立 QUIC pacing 6 项测试。参考程序为 SHA-256 验证过的官方
v2.12.2 发布资产；本地 macOS 的官方源码构建程序也通过同一对照。CI 的受控链路参数如下：
每方向延迟 10 ms，上行短头大数据包按每四包一次的规则丢弃，目标 262,144 字节/秒，
单次上传 2 MiB。以下发送速率统计经过测试链路的 UDP 有效载荷（含 QUIC 开销和被丢弃包，
不含 IP/UDP 头），不代表网卡总线速。

| 场景 | 官方发送/业务吞吐（字节/秒） | Zero 发送/业务吞吐（字节/秒） |
| --- | --- | --- |
| 无丢包，默认补偿 | 262,602 / 255,819 | 261,319 / 255,122 |
| 受控丢包，默认补偿 | 324,478 / 237,284 | 324,459 / 237,937 |
| 同样丢包，关闭补偿 | 261,346 / 191,116 | 259,644 / 190,047 |

可复现入口为 `hysteria2_official_interop` 中
`official_and_zero_bandwidth_under_matched_loss_and_compensation_settings`（需 `HY2_BIN`，显式运行 ignored）。
该测试确认两端补偿会提高发送速率，且此链路的业务吞吐差异在 30% 容差内；
它不是严格性能等价、任意网络或长稳证明。官方发送器的 12 组包数/补偿开关参考向量也已运行核对，
对应 Rust 确定性回归位于 `transport/tests/brutal.rs`。

同一代码提交的[工作区 CI](https://github.com/zerodenet/core/actions/runs/34306685949)
通过 1487 项测试（88 项显式忽略）、严格 Clippy、分层回归、最小特性检查、三平台检查及 musl 构建。
新增受控丢包用例由独立外部验收显式执行；不把其余忽略项计作通过。
本地全工作区运行在 CI 全量通过后停止，不记作本地全量通过；本地 HY2 14 项运行时回归、
validation-only 检查和六场景外部对照已完成。

附加的[特权 TUN 验收](https://github.com/zerodenet/core/actions/runs/34306685919)未全部通过：
Linux/macOS 成功，Windows 首次在直连 IPv6 到 IPv4 的 TLS 回退用例收到连接重置（10054）；
原提交重跑又在直连 HTTP 冒烟用例读取 `104.20.23.154:80` 时收到同类重置，未执行到首次失败用例。
这两条路径均未经过 HY2，但根因尚未确定，不能标记为环境问题或已解决；本批未修改 TUN。

### BBR 三档与共享控制器

2026-09-09，提交 `6d8e32b5` 实现 `transport.congestion.bbr_profile` 的三个官方档位。
协议解析并映射名字，共享 `zero-transport::quic::bbr` 执行采样、启动/排空、恢复、
带宽探测与 RTT 探测；vendor 只增加中立逐包反馈。统一 `up_bps/down_bps` 和 Brutal 协商含义保持不变。

[工作区 CI](https://github.com/zerodenet/core/actions/runs/34310417881)通过 1501 项测试
（89 项显式忽略）、严格 Clippy、三平台检查和 musl 构建；三档共 252 个官方状态检查点匹配。
[外部验收](https://github.com/zerodenet/core/actions/runs/34310417906)通过官方 8 项、sing-box 6 项、
Quinn 266 项回归，并从固定官方源码重新生成和校验参考数据。六种受控 BBR 场景全程吞吐为官方的
97.2%–100.1%，启动与传输末段吞吐也通过预设门槛；详细链路、指标和边界见 [BBR 对齐](bbr.md)。

同次 [特权 TUN 验收](https://github.com/zerodenet/core/actions/runs/34310417863)中 Linux/macOS 通过，
Windows 的 `privileged_windows_ipv4_only_tun_falls_back_trusted_ipv6_domains` 仍出现
`10054 ConnectionReset`。这一失败单独保留在 [Windows TUN 记录](../../project/windows-tun-reset.md)，
不纳入 HY2 通过范围，也不据本次运行推断其根因。本批未修改 TUN 实现或该用例。

### QUIC 自动接收扩窗

2026-09-09，提交 `d2f3e159` 增加共享接收窗口策略和中立载体反馈。
旧固定窗口配置保持兼容，可选最大窗口启用初始值、自动增长、上限和流/连接协调；
HY2 只映射参数，不增加另一套官方配置树。方向、单位、默认值和重载边界见[窗口对齐](windows.md)。

[工作区 CI](https://github.com/zerodenet/core/actions/runs/34312975137)通过 1507 项测试
（90 项显式忽略）、严格 Clippy、分层回归、三平台检查及 musl 构建。
[外部验收](https://github.com/zerodenet/core/actions/runs/34312975214)通过官方 9 项、sing-box 6 项、
Quinn 268 项回归，重新生成并匹配 147 个窗口与 252 个 BBR 官方检查点。
受控高时延链路中，Zero/官方接收器的自动扩窗吞吐分别为小固定窗口的 3.25/3.55 倍，
两端自动窗口吞吐比为 1.003；这不替代任意网络性能等价或长稳验收。

同次[特权 TUN 验收](https://github.com/zerodenet/core/actions/runs/34312975151)三个平台全部通过，
[Connector 验收](https://github.com/zerodenet/core/actions/runs/34312975204)也通过。
本批未修改 TUN；此次通过不撤销此前 Windows 重置证据，也不宣称已修复其根因。

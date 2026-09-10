# 跨协议传输机制分析（2026-09-09）

分析快照：`develop` / `0f19a27a`。分析阶段只增加本报告，没有修改生产代码、配置、能力声明或 Git 历史；后续 HY2/Mieru 接入边界的实施与验证记录在文末。

检查范围：八个协议的配置、适配器、实际传输调用、复用状态和互通测试入口。以下是定向分析，不是所有线协议、密码学、平台行为的完整审计，也不是新的全量互通验收。不能据此给全部协议计算统一完成率。

## 结论

Zero 已有 TLS、QUIC、WS、gRPC、H2、HTTP Upgrade、XHTTP 等共享载体，也已有 VLESS/VMess/Trojan 的协议 MUX 和 HY2 共享认证连接。主要问题是载体能力深度、配置到执行的接通程度、连接池隔离契约及兼容基线声明不一致。

优先处理会破坏已有使用路径的缺陷，再补协议扩展。当前最明确的跨协议问题是 H2/gRPC 的单请求入站、未池化出站和未向上游传递的发送背压。Mieru 的底层复用与 UDP 载体则是独立的较大实现缺口。

## 对照来源与边界

| 对象 | 本次参考 | 使用边界 |
| --- | --- | --- |
| VLESS/VMess、gRPC、XHTTP | Xray `v26.3.27`，与仓库 `scripts/prepare-xray-interop.ps1` 的固定版本一致 | 比较选定载体行为，不把 Xray 的全部应用功能算作协议必需项 |
| Mieru | 官方 `v3.33.0`，与本地 crate 标注版本对应 | 固定源码对照；不声称该版本是当前最新版，不把本地包版本当互通证明 |
| Trojan | trojan-go `v0.10.6`，与本地 Cargo 注释及 metadata 声明对应 | 区分基础 Trojan 与实现族的复用扩展 |
| HY2 | 已有固定 `app/v2.12.2` 对齐清单及验收记录 | 本次复核共享层边界，没有重跑上一批官方验收 |
| SOCKS5、HTTP CONNECT、Shadowsocks | 当前公开能力、实现与已有 RFC/SIP 基线声明 | 没有扩展成新一轮全部 RFC/SIP 章节审计 |

## 横向现状

| 协议 | 已有真实路径 | 本次确认的缺口或边界 | 主要归属 |
| --- | --- | --- | --- |
| SOCKS5 | TCP CONNECT、UDP ASSOCIATE，TCP 控制连接与 UDP 数据通道 | 本轮未发现与 Mieru 同类的必需底层 MUX 缺失；不可把缺少自定义 MUX 当 RFC 缺陷 | 保持协议帧与共享 socket/runtime 边界 |
| HTTP | HTTP CONNECT 入站 | HTTP 出站在 metadata 中明确不支持；HTTP/2 CONNECT/MASQUE 不属于当前入站 HTTP/1 CONNECT 承诺 | 另立能力需求，不从 H2 载体存在推导 HTTP 代理已支持 |
| Shadowsocks | AEAD/2022 TCP、原生 UDP、会话隔离、重放保护及链式载体 | 自定义 MUX/插件能力不等同于 SIP022 缺口；主动探测/重放对抗仍有外部验证边界 | 协议 crypto/framing 与通用 UDP 生命周期 |
| VLESS | Raw/TLS/REALITY、WS/gRPC/H2/Upgrade/XHTTP、历史 QUIC、MUX/XUDP | 受 H2/gRPC 单流路径影响；XHTTP 模式覆盖不完整；QUIC 私有 CA 字段未执行；MUX 池身份表达不完整 | 共享载体 + 协议策略/适配接通 |
| VMess | Raw/TLS、WS/gRPC、TCP/UDP、Mux.Cool | 受 gRPC 单流路径影响；MUX 池未完整区分 TLS/出口身份；H2/XHTTP 等共享载体未暴露为 VMess 配置 | 先补已有路径，再决定是否增加载体组合 |
| Trojan | TLS TCP/UDP、Mux.Cool、重载清池及回程预算 | 现有 MUX 不是 trojan-go 的 smux；WS 未接入；私有 CA 未由现有协议配置/profile 表达 | 兼容基线明确 + 共享 TLS/WS 接通 + 协议复用格式 |
| Mieru | 每次建立 TCP 加密隧道，在其中执行 CONNECT/UDP ASSOCIATE | 底层多 session 分发与池缺失；官方 UDP 底层载体未接入；当前 UDP 业务支持不能代表 UDP 载体支持 | 协议拥有 underlay/session/可靠性策略；共享层提供中立 socket/生命周期能力 |
| HY2 | TCP/UDP/packet-path 共用认证 QUIC；协商/Brutal/BBR/扩窗；H1/H2/H3 网站 | 混淆、跳跃、TLS 扩展及长稳边界仍见协议清单；不能将 HY2 的认证池直接套到其他协议 | 保留协议认证状态，复用中立 QUIC/TLS 能力 |

## 重点发现

### 1. H2/gRPC 入站拒绝同连接的第二条请求（本地已复现）

实际调用为 `inbound_stack::accept_inbound_stream_stack` → `grpc::accept_grpc` / `h2::accept_h2`。
两条路径取出首个请求后，把同一连接的后续请求直接回复为 503。
`grpc::serve_grpc` 虽有遍历请求的实现，当前源码搜索仅发现定义和注释，没有生产调用。
不能因为存在这个函数就认定运行时已支持多流，也不能把它注释里的 MultiMode 等同于 Xray 的 gRPC MultiHunk 服务模式。

本地 macOS 临时探针直接调用共享载体函数，保持首条流打开，再在同一 H2 连接发第二条请求，结果：

```text
H2 same connection: first=200, second=503
gRPC same connection: first=200, second=503
```

这是载体级复现，不是完整 VLESS/VMess + 外部 Xray 端到端复现。生产调用链已由源码确认。
H2 版本同时被 XHTTP stream-one 使用，影响范围不止裸 H2。

代码：`crates/transport/src/inbound_stack.rs:105`、`grpc.rs:202`、`h2.rs:124`、`split_http/stream_one.rs:81`。

落地方向：共享传输层暴露一条连接接收多条逻辑流的接口；通用 runtime 管理子任务、取消和关停，逐条流再进入协议握手。不能简单在协议适配器里调用带独立 spawn 循环的函数，绕开当前生命周期分层。

### 2. H2/gRPC 出站连接生命周期与背压不完整

`connect_grpc` 与 `connect_h2_request` 每次都对传入的新 stream 执行 H2 client handshake，没有保留可复用的 H2 client。
协议 MUX 可减少部分建连，但它与“一条 HTTP/2 连接承载多条请求”是两种不同机制。
Xray 固定版本的 gRPC 使用缓存的 ClientConn，并设置重连退避、保活、窗口等策略。[官方源码](https://github.com/XTLS/Xray-core/blob/v26.3.27/transport/internet/grpc/dial.go#L70)

两个载体的发送端都使用 `mpsc::unbounded_channel`，`poll_write` 入队即返回，写任务直接 `send_data`，未通过容量等待把网络流控反馈给调用方。
这不表示底层 h2 没有执行线上的流控；问题是 Zero 包装层没有给上游施加有界等待。
gRPC 的部分错误路径还会关闭读通道，让调用方观察到 EOF；需与 H2 已有的错误通道一起规范。

本地探针让对端保持请求但不读取 body，分别向 H2/gRPC 写入 8 MiB 并 flush，两者都在 500 ms 门槛内返回成功。
结合无界队列实现，这确认调用方可持续向缓冲提交数据；这不是实际发送完毕、内存峰值测量或已发生 OOM 的证明。

代码：`crates/transport/src/grpc.rs:85,268,304,526`、`h2.rs:78,191,232`。
落地方向：复用共享 H2 连接执行器与有界流队列，分别保留 gRPC/XHTTP 的请求和帧规则。
现有 `http_client` 的源站 HTTP/1 连接池可以参考生命周期，但不能直接当作代理双向 H2 流的完整实现。

### 3. Mieru 缺少官方底层复用及 UDP 载体

官方 Mux 拥有 underlay 列表；启用复用时按策略选择既有连接，再建立新的 session；支持 stream 和 packet 两种底层载体。
默认/禁用复用时可能选择新建连接，不能说官方永远强制复用。[固定源码](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/mux.go#L326)

Zero 的 `MieruTransportLeaf::open_tcp_stream` 每次直接打开 socket；入站每条 socket 仅 accept 一个加密 tunnel/session。
普通 UDP 也通过 `MieruManagedUdpFlowResume::open_direct_connection` 打开 `TokioSocket` 并在流内 UDP ASSOCIATE。
ACK/window 等字段存在于元数据，不构成 UDP 可靠载体状态机已经落地的证据。[官方两种载体及 UDP 业务封装说明](https://github.com/enfein/mieru/blob/v3.33.0/docs/protocol.md)

代码：`protocols/mieru/src/transport.rs:153`、`transport/managed_udp.rs:55`、`crates/proxy/src/adapters/mieru/inbound.rs:18`。

这不能通过“给当前 stream 套一个 HY2 连接池”解决：TCP 下的加密/nonce 属于底层连接，多条 session 必须由单个读写执行器按 session ID 分发；UDP 下还要有协议自身可靠性、分片和窗口执行。
建议拆为 TCP underlay 多 session + 复用策略、UDP underlay 两批；通用内核继续处理每条逻辑流的路由、统计和可选预算。

### 4. 配置存在与执行接通并不一致

共享 TCP TLS 已通过 `ClientTlsProfile::ca_cert_path` 加载私有根证书。
共享 QUIC client builder 只接受 insecure、指纹、ALPN、datagram buffer，使用公共根证书，没有接收私有 CA。
VLESS 的 `quic.ca_cert_path` 从配置传入了 `VlessQuicClientProfile`，但实际 `open_vless_quic_transport` 没有传递它。配置能存储不等于握手会信任该 CA。
Trojan 的 `OwnedTrojanResolvedTlsProfile::ca_cert_path()` 固定返回 None，其现有出站 ADT 也没有统一 TLS 配置入口。

代码：`crates/config/src/model/transport.rs:41,311`、`crates/transport/src/tls.rs:188`、`quic/client.rs:10`、`protocols/vless/src/transport/outbound/direct.rs:15`、`protocols/trojan/src/outbound.rs:418`。

落地方向：先统一共享 TLS profile 的执行能力与映射，优先接通 CA；再分别检查 pin、客户端证书、ECH 的底层支持。保留已有兼容字段的映射，不把 TLS 实现复制进协议；控制面 mTLS 不作为代理数据面已支持的证据。

### 5. 已有 MUX 池需要统一隔离和生命周期要求

VLESS/VMess/Trojan 都已经有 MUX 池、并发槽位、空闲处理和回程预算，不应推倒重做。
但三个池都采用“查缓存 → 解锁异步建连 → 插入”的路径，没有像 HY2 一样对同 key 的首次建连进行串行合并；并发冷启动可重复建连。
各 key 对配置身份的表达也不一致：

- VLESS 的 TransportKey 只有 Raw/Tls/Reality，缺少完整载体路径/配置和 TLS 信任选项；
- VMess 纳入 WS path/gRPC service/SNI，但没有完整 TLS 信任、WS headers 和出口代次；
- Trojan 纳入 insecure/指纹等，但没有 outbound tag、出口代次，且原有 TLS 配置能力较窄。

各协议适配器共享其 runtime 内的池，不能以“每个出站自然有独立池”为由忽略 key 问题。
这些是源码确认的身份缺口和竞态路径；本次没有执行跨 TLS/跨出口误复用的完整复现，具体失败与隔离影响需要定向测试确认。

代码：`protocols/vless/src/mux_pool.rs:35,416`、`protocols/vmess/src/mux.rs:43,405`、`protocols/trojan/src/mux.rs:40,210`；各协议 `transport/runtime.rs`。

落地方向：先明确同配置隔离、建连合并、容量预留、活跃借用、重载退役、错误传播的共同契约；协议保留帧和认证身份，共享层只承载确实通用的池机制。不能仅为减少重复代码而先造一个包办所有协议的 Pool。

### 6. XHTTP 的同名配置不代表官方完整行为

Zero `auto` 固定映射 stream-one，stream-one 使用 H2/H2C；两个其他模式共同走一个 legacy POST/GET 配对实现，以 `X-Session-Id` 关联。
官方固定版本会根据载体/配置选择 auto 模式，区分分包上传与流式上传，并提供 H3、XMUX、独立下载等机制。[官方选择与载体实现](https://github.com/XTLS/Xray-core/blob/v26.3.27/transport/internet/splithttp/dialer.go#L326)

因此 Zero 当前 `packet-up` 和 `stream-up` 不能仅凭字段名字算作官方两种模式已完整对齐。stream-one 的已有互通证据也不能覆盖其余模式。
VLESS 的历史 `quic` 是原始 QUIC stream 载体；即使 ALPN 为 h3，也不代表已实现 XHTTP-over-H3 请求协议。

代码：`crates/transport/src/split_http/stream_one.rs:40,81`、`split_http/legacy.rs:17`、`protocols/vless/src/transport/outbound/direct.rs`。
建议先固定各模式可观察行为，再补共享 H2/H3 会话与 XHTTP 模式实现；不把配置字段相同作为兼容完成条件。

### 7. 兼容能力声明需要与实现族分开

Trojan metadata 声明 `compatibility_baseline: trojan_go`、MUX supported、limitations 空；实际是 Mux.Cool 格式，而 trojan-go `v0.10.6` 的复用使用 smux。
本地 README 也仍明确列出 trojan-go smux 与 WS 未实现。[官方 smux 实现](https://github.com/p4gefau1t/trojan-go/blob/v0.10.6/tunnel/mux/client.go)

不能从“支持 Trojan MUX”推导“兼容 trojan-go MUX”。需要先选择并记录基础 Trojan、Mux.Cool、trojan-go smux 各自承诺，再判断哪些是新增实现；不直接替换已有可用路径。

Mieru metadata 同时写 transports tcp/udp、MUX unsupported、limitations 空，也未清楚区分业务 UDP 与底层 UDP。
`docs/protocols/incomplete.md` 目前只列 SS 与 HY2，无法完整表达本轮确认的载体缺口。
本次仅记录问题，不擅自改 capability 状态或对外承诺。

## 建议落地顺序与验收

| 批次 | 消除的差异 | 用户配置 | 所在层 | 证明方式 |
| --- | --- | --- | --- | --- |
| 1 | H2/gRPC 第二流被拒、发送无有界等待 | 基础多流正确性不新增开关；窗口/保活确需配置时复用通用传输字段 | transport H2 session；runtime 中立流任务管理；协议桥接逐流握手 | 同一连接并发多个请求均成功；取消一条不影响其他；慢读端限制队列与内存；错误可见、关停回收；VLESS/VMess 官方多连接业务压入同一 H2 |
| 2 | 既有 MUX 的连接身份与并发建连不一致，H2 出站缺复用 | 先沿用既有并发/超时字段；隔离条件不是给用户增加的新负担 | 中立执行契约 + 协议构建的 opaque identity | 同 key 冷启动建连合并；不同 TLS/path/出口不共用；满载扩容；重载保留活跃借用、退役后不再分配 |
| 3 | 私有 CA 等已有能力未接通 | 统一 TLS profile，旧字段映射 | config/traits/transport + owning adapter | 私有 CA 成功、错误 CA 失败；TCP/UDP/packet-path/relay 信任策略一致 |
| 4 | Mieru 官方底层机制缺失 | 先复用内核已有并发/空闲/载体概念；官方枚举在协议边界映射 | Mieru underlay/session；通用 socket 与运行时 | 官方客户端与服务端双向、多 session 同连接、业务 TCP/UDP；再验 UDP 载体的丢包/乱序/重传 |
| 5 | XHTTP 模式差异、Trojan 扩展基线、HY2 混淆/跳跃 | 按独立能力映射，避免加入整套官方应用配置树 | 对应协议策略与共享载体 | 固定版本、各模式分别互通；不能只验证 Zero→Zero 或首个请求 |

批次可继续拆小，不要求先完成一个大规模传输层重构。每批协议/运行时变更仍执行项目全工作区和分层回归，长期专项可以后排，但不能把资源回收、背压、错误传播视为以后才需要的稳定性工作。

## 分析阶段验证记录

- 静态核对：配置 → profile/adapter → 实际 carrier 调用，复用 key 和 runtime 持有关系，测试入口与工作流。
- 官方证据：以上固定发布标签源码；未启动第三方程序，不宣称新增官方端到端验收通过。
- 本地探针：macOS，直接编译当前 `zero-transport`，使用 H2 内存双工连接；同连接首请求 200、次请求 503，H2/gRPC 均复现。
- 慢读探针：H2/gRPC 对端均不读取 body，8 MiB 的 write + flush 在 500 ms 内完成；未测量长期内存增长。
- 探针使用仓库锁定依赖和本地 Quinn patch；源码在临时目录，生产源码和 Cargo.lock 保持不变。
- 本地探针目录：`/var/folders/5g/3js9c9b11c3fmfd930s4vkdm0000gn/T/zero-transport-audit-ijjg_re4`，`src/main.rs` 包含两类复现；运行方式为 `cargo run --offline --manifest-path <探针目录>/Cargo.toml --target-dir /Volumes/tool/rust/zero/target`。该目录是临时证据，不是仓库的永久回归用例。
- 分析阶段没有生产代码、配置解析或运行时行为变更，因此未重跑全工作区测试。后续接入边界变更的验证单独记录在下节。


## 后续实施：优先收紧 HY2 / Mieru 接入边界

按本次讨论后的优先级，先完成分层修正，再推进上表中的功能扩展。
内核现有帧式 MUX 契约保留；新增原生多流及可选数据报的中立契约，由 HY2 协议实现。
通用 runtime 消费契约并管理连接任务，QUIC 桥只保留载体接入。
Mieru 已实现 `InboundStreamRoute`，适配器将协议返回的 route 交给内核，不再匹配协议 TCP/UDP 变体。
具体职责已同步到 [架构说明](architecture.md#mux-与原生多流接入) 与根 AGENTS.md。
本批不改变配置和线格式，也不宣称已补齐 Mieru 底层 MUX、UDP underlay 或 H2/gRPC 多流；这些仍按上文独立推进。

### 接入边界验证

- `cargo check --workspace`、`cargo fmt --all -- --check`、`git diff --check` 通过。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` 通过。
- `zero-core --no-default-features` 及 `zero-proxy --no-default-features` 的 `hysteria2`、`mieru`、`vless` 单协议构建通过；裁剪功能组合仍有 unused/dead-code 警告，不能把编译通过描述为这些组合零警告。
- `runtime_boundary` 的 183 项分层检查通过，其中新增 3 项原生多流和 Mieru 路由归属检查。
- HY2 现有真实 QUIC 连接池测试改为消费中立契约，认证共享、会话隔离、断线重建和重载保留检查通过；Mieru 新增 2 项路由分发测试通过，既有握手与身份归属回归也通过。
- `RUST_MIN_STACK=16777216 cargo test --workspace --all-features --no-fail-fast` 通过：包含文档测试在内，1,576 项通过、0 失败、94 项按默认条件忽略。功能组合和 16 MiB 测试栈沿用仓库现有 `.github/workflows/ci.yml`；根 AGENTS.md 的本地全量命令同步为这一设置。
- 首次默认配置运行在 `timed_out_reload_waits_for_last_known_good_rollback` 出现一次 2 秒监听就绪超时。该 direct 监听测试及其超时设置保持原样，单项、11 项整组及后续全量复测中的该组均通过；未据此宣称已定位首次超时的根因。
- 未设置 CI 测试栈的复测在 SOCKS5→VLESS UDP 链路发生栈溢出；使用现有 CI 的 16 MiB 测试线程栈后，同一可执行文件的 25 项 UDP 矩阵全部通过。未修改协议代码或测试断言来绕过失败。
- 此处的本地测试不替代固定官方版本的双向互通验收。

## 后续实施：Mieru TCP 入站多会话

继接入边界整理后，已接入协议拥有的 TCP underlay 多会话实现。
内核通过中立 `InboundRouteMultiplexer` 分别执行逻辑 stream 的握手与路由；
Mieru 拥有共享 cipher/nonce、会话分发、有界队列和单会话关闭。
这落实了上表第 4 批中的入站 TCP 底层多会话部分，出站连接池和原生 UDP underlay 仍待推进。
具体功能、官方固定版本与本地验收记录见 [TCP 多会话](../protocols/mieru/multiplex.md)。

### 2026-09-10 后续实现：Mieru 出站池与 UDP 载体

已增加协议拥有的 TCP/UDP 共享出站池，包含并发建连合并、凭据/出口身份隔离和重载退役；
原生 UDP 载体接入双向监听/出站，包含 ACK/窗口、重传、乱序去重与有界会话回收。
`transport` 默认 `tcp`，可选 `udp`。MUX 导出为 supported，整体仍因长稳验证缺口为 partial。
官方 v3.33.0 参考端双向 TCP/UDP 载体均已取得本机互通通过记录；详细矩阵、
最新全量检查状态和限制见 [Mieru 多会话与载体](../protocols/mieru/multiplex.md)。
上文缺口是审计时快照，不代表本轮实现后的能力状态。

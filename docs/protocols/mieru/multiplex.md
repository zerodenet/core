# Mieru 多会话、共享连接池和原生 UDP 载体

本批修复同一 TCP 底层连接只能处理一个逻辑 session 的缺陷。
官方客户端可以在同一连接上交错开启 TCP CONNECT 和 UDP ASSOCIATE，会话的加密 nonce
属于整条连接。旧入站在首个握手后把后续帧全部交给一个 stream，既不能分发后续 open，
也不能隔离不同 session 的业务数据。

## 实现边界

- `protocols/mieru::inbound::multiplex` 拥有连接级 cipher pair、单一读帧入口、单一写帧入口和 session 注册表。
- 每个逻辑 stream 只读自己的 payload，关闭一个 stream 不关闭底层连接或其他会话。
- 协议实现 `zero_core::InboundRouteMultiplexer`。出队 stream 与隧道内握手分开，运行时并发执行每条 stream 的协议握手，再通过已有 `InboundStreamRoute` 分发 TCP/UDP。
- 适配器只映射配置和交接连接对象，不运行会话循环，不解析帧或 session ID。
- runtime 拥有逻辑任务、30 秒握手超时、principal/device 策略、用户清退和关停回收；连接任务取消会中止协议 driver。

现有配置默认使用 TCP。原生 UDP 可在入站/出站的 `protocol` 对象中设置 `"transport": "udp"`。
Mieru 用户、principal、监听和路由设置继续生效。
一条底层连接绑定首帧认证用户，后续 session 继承该身份，不能在同一 cipher 状态内切换用户。

## 成帧、关闭与资源

首个 open 的 payload 可以携带隧道请求。首帧和后续帧均消费声明的 padding；
分段输入未到齐时不推进隐式 nonce，不返回超出输入的消费长度；合并到达的后续帧保留。
发送端每个数据帧最多 32 KiB。`flush` 等待此前数据进入底层 socket 并完成其 flush，
不能把进入应用队列视为底层发送完成；它也不表示远端应用已经读取。

每条 TCP 连接最多 256 个活跃 session 和 256 个待接受 stream，发送队列最多 64 条命令，
每 session 接收队列最多 8 个 payload。TCP 队列满后转入每会话有界待投递缓冲，默认持续 30 秒无消费进展或超过缓冲上限时关闭该会话；共享读取继续分发其他会话。配置与取舍见 [策略](policies.md)。
持续无法消费时以明确错误关闭该 session 并发送 close request，不静默丢弃可靠字节。
这一秒内共享 TCP 接收会暂时等待；独立发送任务继续工作，不承诺共享 TCP 完全没有队头阻塞。
这些是当前实现的资源策略，不声明与官方缓冲大小或所有过载行为等价。

未知数据 session 会收到关闭请求；重复活跃 ID 和保留 ID 0 导致连接失败。
单会话关闭处理 request/response 并回收注册和发送序号。旧 stream 延迟 drop 时按对象身份核对，
不能误删复用同一 wire ID 的新会话。连接错误会唤醒逻辑读写方并传播到运行时日志。

## 官方对照与复现

固定官方 `v3.33.0` / `48ddb69d5d343d76c9004c5054ee36609579ba13`：
[TCP underlay](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/underlay_stream.go)、
[session](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go)。
测试程序调用官方导出的 `StreamUnderlay`、`Session` 与 UDP 包装 API，未重新实现官方帧或加密。
准备脚本拒绝错误版本或有改动的参考源码 checkout。

```sh
python3 scripts/prepare-mieru-interop.py --version v3.33.0 --output /tmp/zero-mieru-reference-probe
MIERU_REFERENCE_BIN=/tmp/zero-mieru-reference-probe RUST_MIN_STACK=16777216 \
  cargo test -p zero-proxy --all-features --test mieru_official_interop -- --ignored --nocapture
```

`GO_BIN` 可指定 Go 工具路径，`--source` 可使用干净的固定版本 checkout。
脚本默认从协议包版本选择固定基线，当前为 v3.33.0；CI 同样只验收该包基线。
`--check-baseline` 检查包版本、锁文件和固定提交映射。此前 v3.36.1 的互通记录仅是
补充兼容性证据，不能替代 v3.33.0 的实现对照。当前能力差距与验收边界见 [parity.md](parity.md)。
新增 `Mieru Interop` 工作流在相关改动时显式运行这一外部用例；远端执行结果需推送后单独确认。
测试启动本机 Zero 入站和 TCP/UDP echo 目标，不依赖公共网站或线上节点。
参考端明确创建一条 underlay，并断言只拨号一次；运行 4 路 TCP、2 路 UDP，各两轮数据，
TCP 每轮 1,048,577 字节、UDP 每轮 1,600 字节；另保留一个未完成握手，关闭它后验证其他会话，
最后在相同 underlay 上再次新建会话。普通工作区测试默认忽略这一外部程序用例，显式执行时缺少程序直接失败。

确定性回归在 `protocols/mieru/tests/multiplex/`，覆盖增量 padding/nonce、TCP/UDP 独立分类、
慢握手、会话关闭、大写入、未知/重复/保留 ID、慢消费失败、ID 再用、截断连接、drop 回收及发送背压。

## 共享出站连接池

直连上游、TCP relay 末跳以及由 datagram relay carrier 承载的原生 UDP 末跳，都从协议连接池
开启独立逻辑 stream。TCP relay 池同时承载业务 TCP CONNECT 与 UDP ASSOCIATE；协议拥有
连接级 cipher、读写 driver 和 session ID；代理适配器只提供
遵循出口/relay 路径的 TCP 或 datagram carrier。
池身份隔离 outbound tag、服务端、凭据、载体、完整 relay 前缀和出口 generation，合并相同身份的并发建连。
池优先选择当前活跃流与待 OPEN 数量最少的载体；所有载体达到各自会话容量的 75% 后按需增长，
每个身份最多 4 条载体、缓存最多 512 个身份。载体达到 30 分钟最大年龄后不再接收新 stream，
已有 stream 继续完成；协议 driver 仍负责无会话空闲回收。重载清空缓存后，新请求建立新载体，
活跃 stream 保留旧载体直到结束。
失效连接退出缓存，新的 OPEN 可重新建连，已经发送的业务数据不会自动重放。

## 原生 UDP 载体

使用官方 v3.33.0 的 stateless XChaCha20-Poly1305 包格式，每次发送和重传生成新的 nonce。
认证后同一 peer 绑定用户；已认证 OPEN 的 nonce 进入跨 peer driver 的有界重放缓存。
该缓存最多 65,536 条、保留 6 分钟，容量满时拒绝新的 OPEN，已有数据会话继续工作。
每条载体最多 64 个活跃 session，独立累计 ACK、接收窗口、乱序缓冲、重复包抑制与超时重传。
报文分片最多 1,100 字节；ACK 窗口随消费进度前移，接收队列满时保持未确认状态等待重传，
乱序容量限制为 128 个待交付帧，而非 128 的序号范围；缓冲满时优先保留靠近缺口的帧，
未确认的远端帧依赖重传。不会把一次底层分片当成完整业务 UDP 报文。业务 UDP 的长度解析保存跨读取和读取取消状态。
发送使用 RTT 自适应 RTO、指数退避、快速重传和 v3.33.0 CUBIC 拥塞窗口；重试参数不宣称与官方
在所有网络条件下具有相同性能。UDP `flush` 等待此前可靠帧被对端确认，不代表目标应用读取。

UDP peer listener、任务上限（128 peers）、关停和逻辑路由任务由通用 runtime 拥有；
协议提供每包 1,500 字节和每 peer 256 包的接收预算，超大包在分发前丢弃，避免队列膨胀。
认证、密钥、重放缓存、可靠性与会话状态留在协议内。关闭请求/响应、超时和 tombstone
隔离各逻辑会话；不声明 TCP 式单向半关闭。SOCKS5 UDP ASSOCIATE 等 packet-path hop 可提供
原生 UDP 所需的 datagram carrier，并由 Mieru 池在其上复用可靠协议载体。仅提供字节流的 relay hop
不满足这一契约，该组合明确失败；默认 TCP 载体的 relay 末跳也按完整前缀链身份共享池化载体。

## 仍需验证

长期运行、网络切换、全量官方配置策略与过载/性能矩阵仍未完整对照。整体保持 `partial`，
MUX 与 TCP/UDP 载体按已实现能力导出。以下历史记录与本轮新增验证分开保存。

## 本批验证记录

2026-09-09，本地 macOS 首轮官方用例（每路 TCP 65,537 字节）通过，随后把同一用例扩大到
每路 1,048,577 字节，出现参考端 `EOF`。复查发现接收队列使用立即 `try_send` 失败策略，
可能将短暂积压视为持续过载；当时改为最多 1 秒的背压等待；当前进一步改为有界会话缓冲和可配置进展期限，保留较大的用例作为验收标准。
2026-09-10，修复后的较大用例通过（4 路 TCP 每路两轮 1,048,577 字节，另有 2 路 UDP）；
同时验证关闭隔离和同一 underlay 上的新会话。新增 9 项确定性回归全部通过。

`cargo fmt --all -- --check` 与 `cargo check --workspace` 通过。
工作区全特性测试编译完成，已执行 91 项、失败 0 项；多个测试程序在输出测试入口前长时间停滞，
最后的既有 `pbkdf2_verify` 进程超过 5 分钟仍未进入测试输出，故终止本轮并保留日志。
**工作区全量验证未完成，不能据此报告全量通过。** 本地现象不足以确定系统层根因。
单独执行能力导出测试 10 项通过；架构边界测试程序同样在启动前停滞超过 5 分钟，已终止，
其 Rust 测试执行结果尚未取得。
工作区严格 Clippy 同样在 `zero`/`zero-grpc` 构建脚本启动阶段停滞超过 11 分钟后终止，
该工作区检查未完成。`cargo check -p zero-proxy --no-default-features --features mieru` 通过，
该特性组合存在未使用代码警告，不能称为无警告构建。

受影响 crate 的严格检查通过：

```sh
cargo clippy -p zero-core -p mieru -p zero-proxy --all-targets --all-features -- -D warnings
cargo check -p zero-core --no-default-features
```

以上局部检查不替代尚未完成的工作区全量测试、架构边界测试执行和工作区 Clippy。

本轮日志保存在 `/tmp/zero-mieru-validation/`；较大官方互通日志为
`/tmp/zero-mieru-large-final.log`。这些是本机临时证据，远端工作流尚未执行。

## 出站池与 UDP 载体的后续验收（2026-09-10）

固定官方 v3.33.0 参考程序新增 server 模式，双向验证 TCP 和原生 UDP 两种载体。
Zero 出站用例并发运行 4 路 TCP（每路 131,073 字节）和 2 路 UDP（每路 1,600 字节），
随后再次新建 TCP 会话；官方服务端统计只有一条 TCP 连接或一个 UDP peer。
官方客户端到 Zero 的两类入站使用上述 4 路 TCP + 2 路 UDP、两轮大数据矩阵。

首轮 UDP 入站发现业务报文分片截断，已用取消安全的长度成帧修复。
后续压力复测又发现乱序后恢复停顿：接收容量曾被误当作序号范围，并且 peer 队列小于
一次收包调度突发。修正有界乱序保留规则与接收预算后，四项官方互通全部通过（14.20 秒）；
曾超时的 UDP 入站追加两次独立复测也通过（14.83 秒、13.12 秒）。这些耗时是本机测试记录，
不是跨网络性能承诺。用 `MIERU_INTEROP_TRACE=1` 可输出参考端会话队列和序号诊断。

本轮临时证据：`/tmp/mieru-bounded-reorder-interop.log`、
`/tmp/mieru-udp-repeat-1.log`、`/tmp/mieru-udp-repeat-2.log`。
最终版本工作区检查和测试结果见下文。

最终代码的 `cargo fmt --all -- --check`、`cargo check --workspace` 和
`cargo clippy --workspace --all-targets -- -D warnings` 均通过。
`cargo check -p zero-proxy --no-default-features --features mieru` 与
`cargo check -p zero-core --no-default-features` 通过；前者存在未使用代码警告，不能称为无警告构建。
协议单元测试 10 项、连接池/故障注入 3 项、报文成帧 1 项、配置 2 项、
架构边界 185 项及能力导出 10 项均通过。既有入站多会话 9 项也在最终代码全量轮次中通过。
文档测试命令完成，无失败，3 个示例按原标记忽略；没有可执行的非忽略文档示例。

工作区全特性初跑记录为 **674 通过、1 失败、17 忽略**，在未改动的 Connector 测试
`full_memory_workset_replaces_samples_without_evicting_facts` 中未观察到预期事件。
同一测试二进制的单用例复测通过，随后整组 17 项复测也通过；根因尚未确定，
不能把这轮记录描述为一次完整无失败的全量运行。原始证据保留在
`/tmp/mieru-workspace-final-source.log`、`/tmp/mieru-connector-isolated.log` 和
`/tmp/mieru-connector-binary-retest.log`。
被中断的其余工作区模块通过单独批次继续执行，日志为 `/tmp/mieru-workspace-remaining.log`。

其余模块续跑已完成：**924 通过、0 失败、81 忽略**，命令退出码为 0。
该批使用工作区 `--all-features --no-fail-fast`，排除首轮已完成的协议、根二进制、
API、配置与 Connector 包；文档测试另以工作区 `--all-features --doc` 完整执行。
首轮和续跑的忽略项属于原有显式环境/资格门禁；Mieru 的 4 项外部互通已如上单独执行。

代码尚未提交或推送；远端工作流、发布包及安装后验证未执行。

# Direct UDP 预算补充证据

本记录补充 [validation.md 的 PR01 资源预算要求](../../validation.md#保守资源预算的落地范围)。只增加测量脚本、原始证据和建议，不改变生产代码、配置、默认值或资源授权。新增最小可选字段已由完整设计授权；默认数值仍须代表性实测，当前未确定项为 `null`，不能据此激活新的默认限制。

后续补充的 [实际旧源 library task/generation 结果](old-library-results.md) 已覆盖 16 source 的真实 Tokio tasks 及 API generation0–16；它与下述 CLI 采样作用域分开。应用队列字节、精确 native acquisition 和当前源配对仍未验收，新增预算默认继续为 `null`。

## 固定输入与执行范围

- 旧内核来源：`2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`。
- 已构建二进制 SHA-256：`026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f`，原位置 `/workspace/core-baseline-artifacts/zero-release`。脚本先复制并核对实际字节，二进制不入仓库。
- 镜像：`python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`，`--pull=never --network=none --cpus=1 --memory=1073741824 --memory-swap=1073741824`。实际容器 `cpu.max=100000 100000`、`memory.max=1073741824`、`memory.swap.max=0`，保存在原始结果。
- 仅使用容器 loopback `127.0.0.1`、`127.0.0.2`、`::1`，本地 DNS 回答 `budget.invalid`，无系统 DNS、TUN、路由、安全设置变更。原生 echo/DNS/client socket 保留至工作线程接管或明确退出；监听端口由实际 port-zero socket 分配并从其 PID 所有的 inode 发现，没有先探测再抢端口。
- 内核 PID 的 RSS、VmHWM、FD、原生线程与内核 UDP 队列单独采样；Python echo/DNS/客户/采样器计入容器峰值。没有运行编译。父任务同时验证源码，因此这是资源基数与回收观察，不是隔离的 RS01 吞吐/延迟验收，也没有新目标二进制的配对结果。

生产拓扑核对：旧 `inbound/direct.rs` 构造 `TcpAndPeerDatagramInboundListenerOperation`，每个 UDP source peer 的 `DirectDatagramRelay` 使用配置中一个固定 target/port。它不能从同一原始 UDP payload 接收任意 wire target。普通 SOCKS/SS 的多目标 dispatcher 另有入口，共用旧 Direct socket 执行器不能证明它们已迁移到新 peer 模型。本次固定 domain 在真实策略/DNS变化后实际使用多个 IP/族；不把它写成原始客户端携带多个目标。

## 单 source、16 次策略轮转

实际执行命令（从该 worktree 根目录；输出目录必须不存在）：

```sh
python3 -B design/v0.0.3/implementation/direct-udp-budget/run.py \
  --binary /workspace/core-baseline-artifacts/zero-release \
  --expected-sha256 026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f \
  --source-commit 2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a \
  --output /workspace/core-next/direct-udp-budget/old-16x3 \
  --rotations 16 --trials 3
python3 -B design/v0.0.3/implementation/direct-udp-budget/summarize.py \
  /workspace/core-next/direct-udp-budget/old-16x3
```

每轮保持一个实际客户端源 socket；每阶段 3 个 1024 字节完整 payload 比对，静置 150ms。literal 在 `auto/only_ipv4` 切换；fixed domain 在 `only_ipv4/only_ipv6` 切换，A 在两个 loopback 地址间轮换、AAAA 为 `::1`。所有真实 IPC 请求/响应、DNS、echo 目标/源、payload 前缀、成功比对、错误、采样和退出状态均保留。

[汇总](old-16x3-summary.json) 与 [压缩原始证据索引](old-16x3-evidence.json) 固定这次执行。压缩包同时保存当时的六个脚本及其 SHA，后续扩展脚本不改变历史来源。外部原始目录为 `/workspace/core-next/direct-udp-budget/old-16x3`。

| 观察 | literal（3 次） | fixed domain（3 次） |
| --- | --- | --- |
| 稳态完整 payload | 153/153 | 153/153 |
| 每次观察到的活跃业务 flow 峰值 | 1 | 1 |
| 每次历史不同业务 flow ID / echo 关联业务 socket inode | 17 / 17 | 17 / 17 |
| 每次采样业务 FD 峰值 / 全进程 UDP FD 峰值 | 1 / 2 | 1 / 2 |
| 全进程 FD 峰值范围 | 13–14 | 14 |
| 内核 VmHWM（字节） | 30,261,248–33,239,040 | 33,636,352–34,086,912 |
| 内核原生线程峰值 | 5 | 5 |
| 内核所属 UDP RX queue 峰值（内核记账字节） | 0 | 2,304–4,608 |
| 实际 echo 目标 | `127.0.0.1` | `127.0.0.1`、`127.0.0.2`、`::1` |
| 实际观察到的 egress generation | 0 | 0 |

31/32/33 包突发合计 **575/576** 个唯一 payload 收到；一次 domain 的 33 包突发收到 32 个，其余突发全部收到。该缺失没有归因到某个用户队列，没有标成全通过。6 次均在 source peer 仍活跃时经父 stdin 关闭优雅退出，退出码 0，约 31–65ms。包含探针和 echo 的整个容器 `memory.peak=74,338,304` 字节，OOM/oom_kill 均 0；这些值不能称为内核专属堆。

17 是健康轮转中的历史创建下界，不是推荐 17 个并发槽。FD 峰值 1 也只适用于本 profile 的 10ms 采样，不能覆盖瞬时重叠、并发解析候选、失败/Unknown、其他协议和域名 fan-in。关键兼容结论是：把一次成功授权当作整个长期 peer 的终身总数会拒绝旧内核已成功执行的健康轮转；有界并发 lease 应仅在唯一 owner 的原生关闭确认后回收。

## 16 source 并发与实际 reload 重叠

补充执行 `run_fanin.py`，同一固定旧二进制、镜像和实际 1 CPU/1 GiB 约束，`--trials 3 --payload-bytes 1024`，输出 `/workspace/core-next/direct-udp-budget/old-fanin-16x3`。每轮 16 个真实源 socket，每个固定同一配置 domain；共 17 个阶段，A/AAAA 及族策略照上节轮转。reload 期间一个有界、实际 join 的 Python sender 继续发包，所有发送时间、IPC 时间窗与实际 echo 到达时间可核对。另测每 source 31/32/33 个包同时在途。

[汇总](old-fanin-16x3-summary.json) 和 [完整原始证据索引](old-fanin-16x3-evidence.json) 保存命令、脚本、全部逐包缺失与实际错误。该次 launcher 的 `--source-commit` 元数据抄写时漏掉三个字符；原命令和原 launcher 原样保留，`provenance-correction.json` 明确更正为固定旧提交。执行前核对的真实二进制 SHA 与 build-info 始终正确，没有使用其他内核。

| 观察 | 3 次实际结果 |
| --- | --- |
| 稳态完整 payload | **2448/2448** |
| reload 重叠发送 / 唯一响应 / 未观察到响应 | **4768 / 4015 / 753** |
| 实际 IPC 时间窗内发送 / echo 到达 | 480 / 831（允许已在途报文先发后到） |
| 每次活跃业务 flow 峰 / 业务 FD 采样峰 | 16 / 16 |
| 每次历史不同 flow ID / echo 关联 inode | 272 / 272，即 16×17 |
| 全进程 FD 峰 / UDP FD 峰 | 29 / 17 |
| 内核 VmHWM 范围（字节） / 原生线程峰 | 60,129,280–60,669,952 / 5 |
| 所属 UDP 内核 RX queue 采样峰（字节） | 29,952 / 36,864 / 288,000 |
| 31/32/33 per-source 突发发送 / 唯一响应 | **4608 / 2021**，2587 个未观察到响应 |
| 实际目标 / egress generation | 两个 IPv4 loopback 及 `::1` / 仅 0 |
| 原 ingress inode | 所有仍有 UDP socket 的快照中均保留 |
| 实际优雅退出 | 3/3，退出码 0，约 114–264ms |

没有把 reload/突发期间的缺失写成通过，也没有在缺少 observer 时归因到 ingress mpsc、内核接收队列、echo/client 队列或故障恢复中的某一个层。探针本身保留逐包记录和逐阶段 JSON，容器 `memory.peak=334,331,904` 字节包含其全部成本，OOM/oom_kill 仍 0；该容器峰值不能用于推算内核 per-peer 字节或新默认值。内核自己的 VmHWM 分开列出。16 个源的同一 domain 测量也不能冒充一个源携带任意 16 个 wire target。

复现命令使用正确完整的 source 元数据；历史原命令及元数据更正均在压缩包中：

```sh
python3 -B design/v0.0.3/implementation/direct-udp-budget/run_fanin.py \
  --binary /workspace/core-baseline-artifacts/zero-release \
  --expected-sha256 026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f \
  --source-commit 2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a \
  --output /workspace/core-next/direct-udp-budget/old-fanin-repeat \
  --trials 3 --payload-bytes 1024
python3 -B design/v0.0.3/implementation/direct-udp-budget/summarize_fanin.py \
  /workspace/core-next/direct-udp-budget/old-fanin-repeat
```

## 测量能证明什么

`/proc/PID/fd` 所有权过滤后的 inode 与 echo 实际源端口关联，排除已知 ingress inode；保留所有原始表行。10ms 非原子快照可能漏短峰值。inode 消失和端口历史不构成原生关闭 receipt。`VmHWM` 补充 RSS 高水位，但也不等于队列字节。内核 UDP queue 的 skb 记账不是 payload 长度或 Rust 分配。

`tokio_task_peak`、`user_queue_bytes_peak`、`exact_native_open_attempts`、真实 peer TaskId 和安全强制 egress-generation 轮转均为 `null`/`not_measured`。现有 CLI 没有权威任务 census 或可安全驱动 EgressControl 的入口；本次没有通过改系统路由伪造代次。后续由唯一构建 owner 编译独立 library 观察器：实际 control.invalidate_network、真实资源 reserve/acquire/close receipt 计数、每 owner 活跃/retiring/pending bytes、Tokio runtime metrics、真实 TaskExit；使用相同负载分别跑旧/新内核。

[既有 RS01 基线](../../baseline/resource-baseline.json) 的 124 FD 是 32 TCP stream + 16 UDP association 等固定混合负载的**全进程峰值**，不是单 peer cap。其 [task census](../../baseline/task-observation.json) 仅公开 proxy 库的 32 TCP stream：`0 → 3 → 35 → 3 → 0`，global queue depth 0，排除 CLI/IPC/Connector 应用任务。它不能确定 UDP peer 总预算或 pending bytes。现有 baseline 尚未提供这两项可用于定默认的实测容量值。

## PR01 预算缺口与配置建议

表中“建议”是作用域/耗尽规则；缺少代表性证据的数值保持 `null`。现有安全窗口、业务 idle 和可靠投递义务照旧。

[结构化缺口表](budget-gaps.json) 保留建议数值的 `null` 与作用域；[固定旧源码索引](source-references.json) 记录实际文件 SHA、行号与对应语句，避免把正在开发的目标实现当作旧默认来源。

| 实际所有者 / 资源 | 现有字段与缺省 / 实际使用 | 实测使用或仍缺证据 | 建议有限项、配置入口、耗尽行为与兼容影响 |
| --- | --- | --- | --- |
| Combined listener / source peer 数 | 无配置；旧硬编码 4096 个 `HashMap` peer；只统计 sender 未关闭的 peer，不是每 peer FD/flow 上限 | 本次 1 / 16 source；既有 RS01 16 UDP association 入口不同；容量耗尽和 retiring 占位仍缺 | 保留现有 admission，新增最小 optional 全局/每 listener peer cap 的建议值 `null`。必须包括 active+retiring；容量检查先于创建任务；满时拒新增 source，已有 peer 继续。不能由 4096 推导每 peer 上限 |
| 每 source peer / ingress 排队 | 旧硬编码 mpsc 32 项；listener 65535-byte scratch；`try_send` 满时丢新报文，先 `to_vec` 后尝试；没有整个 listener/进程 pending payload byte 上限 | 单 source burst 575/576；16-source burst 2021/4608；用户 bytes/丢弃归因未测 | 优先保留 32 项行为；最小 optional pending payload byte cap 可按 listener 聚合并保留 per peer 条目限制，建议字节值 `null`。入队前 reserve，消费/拒绝/取消实际释放；满只丢当前未承诺报文。先验证最大包、全局压力、GUI/面板缺省往返，不把 Packet 字段自动套到这里 |
| 每 dispatcher / Direct logical session | `UdpSessionFlows`、isolated-session 表没有总硬 cap；Combined wire target 固定但 fake-IP/sniff/route 可重投影；其他入口多目标 | 单 source 活跃 flow 峰 1、历史 ID 17；16-source 峰 16/历史 ID272；当前不证明多目标范围 | 最小 optional `max_logical_sessions` 建议 `null`，scope 为实际 dispatcher/peer，含 retiring 占位。满时拒新增逻辑目标，已有流不因统一算法任意逐出。固定 domain 权限必须校验 host 解析/route 结果，不能仅授权首次 literal 后宣称完整兼容 |
| 每 dispatcher / 原生 Direct FD | `DirectUdpSockets.sockets` 无总 FD cap；按 policy revision/family/egress/source/interface/association 隔离；stale 在旧路径真实移出原 socket | 每次历史 17 inodes、采样峰 1；egress generation0；失败与短重叠峰未测 | 最小 optional `max_native_sockets` 建议 `null`，按 active+retiring+pending+Unknown lease 计。新 socket 一创建即有 persistent owner，配置/注册失败保原 source；Unknown 不返预算。唯一 owner 同锁确认外部 I/O 已退、真实 syscall close receipt 后可返该 exact FD lease，无需把同 peer 的所有业务强制结束 |
| 每 dispatcher / 原生 acquisition 与候选 | 无总 attempt cap；auto 可在首族 bind 失败后换另一族；失败 backoff 250ms→10s、最多记 1024 failure keys，不是 lifetime attempt 限制 | 未测 syscall attempts/并发 open job/DNS candidate storage | 最小 optional `max_pending_opens`、候选存储/每次 resolution 上限建议 `null`；reserve 在 acquire 前，失败记录必须有限/保 source。不要把终身 1 次成功或失败当所有正常 peer 的预算；有限并发和有限单次 retry/backoff 与终身次数区分。候选裁剪不能偷偷改变 resolver 族顺序/既有 fallback 语义 |
| 每原 FD / response ownership | 旧固定 256 个 active+retired tuple；live socket 的 tombstone 不过期；实际 source guard 不可转到另一 FD | 本次没有填满 256；迟到包安全仍靠已有回归 | 继续原 256 与安全语义。满时拒新增 mapping/按既有隔离规则取得另一有预算 socket；不缩短 tombstone 窗口、不清 retired 后复用 tuple。对应 FD 真关闭后才能整体回收，不建议额外配置重复同一安全限制 |
| 每 peer relay / scratch 与取消通知 | Direct relay 分配 64KiB direct buffer；启用 upstream-association-runtime 再分配 64KiB upstream buffer；native receiver 按需再分配 65535-byte scratch；cancel channel 为 unbounded | 本次仅内核 RSS/VmHWM，无逐 owner heap；Tokio tasks/通知峰值 `null` | 检查是否可同调用链消除重复 scratch，先做最大包与取消 fan-in 观察。原 4096 admission × 这些固定 buffer 是分析上界的一部分，不是测得峰值；不能仅凭 RS01 内存承诺所有 peer 的总字节界限。通知应由真实有限 session 数派生界限，不能新增脱管转发任务 |
| 业务 idle | `inbounds[].idle_timeout_secs=null`，TCP 实际默认 300s；`runtime.udp_upstream_idle_timeout_seconds=30`；Direct raw peer 自身 120s | 本次持续活动不触发 idle；旧 baseline 固定默认已捕获 | 复用原字段/协议实际 idle；它们不是容量/FD 数限制。无证据不缩短业务寿命，不拿 upstream30s 关闭 Direct 活跃 peer |
| Packet/stack UDP queue | `runtime.network.udp_receive_queue.capacity=64`；`max_payload_bytes=null`、`max_stack_payload_bytes=null`；只在 RawIpStackOptions→client stack 接线 | 既有 defaults 样本有值，Packet/容量拒绝未覆盖；本测无 Packet | 继续每 socket 项目/字节与每 stack 字节作用域；不称进程/managed Direct 全局 cap。新增建议值 `null`，按实际 Packet profile 验收。满时走现有 final discard observer，不把重试 backpressure 记成最终丢包 |
| 宿主 / 握手、session、等待准入、组件 tasks | 组件显式 finite TaskScope 声明；没有从 baseline 得出的全进程 handshake/session/pending cap | 公开库仅 TCP32 census；CLI/global/UDP census 缺失 | 在实际 admission 前保留 owner/permit，等待有 deadline；计划内任务不叠加每层隐藏 worker。新增全局数值 `null`，不能拿 native threads5 或 TCP tasks35 当 UDP/全局默认 |
| 协议实例 / pool、substream、重组、安全缓存 | 复用协议/transport 自有字段（如 mux/xudp 并发、split HTTP pool 范围）；不存在一个可覆盖所有协议的统一 default | 既有 RS01 与本测未给每协议实例分配/耗尽样本 | 逐协议固定上游版本测量，保留防重放窗口和满时拒新增规则；新增推荐 `null`，不在 proxy 另解释冲突上限、不用统一 LRU 提前释放安全状态 |
| 事件与 Connector / 待确认事实 | `runtime.event_log_capacity=1024`；dispatcher 默认 deliveries4096、delivery bytes16MiB、source bytes8MiB、retry10、max retry delay64000ms、webhook10000ms；源码 outbox reserve 默认 1GiB / 5%（取更严格者） | [旧 defaults](../../baseline/configured-defaults.json) 固定已采样字段，outbox reserve 另由固定旧源码核对；本 Direct 测量无慢 ACK/outbox 磁盘容量实验 | 复用已有 API/outbox 配置与可靠义务；建议保持原缺省，新增数值 `null`。不能为省 bytes 丢未 ACK 事实或把 retry exhausted 当资源关闭；磁盘保护、背压和故障语义独立验收 |
| 准备/配置事务 / 文件、候选与补偿记录 | 旧路径无统一输入/构建并发 cap；新资源 File declaration 有 exact path/max_bytes 与实际 blocking job owner，但不是已有用户配置默认 | 既有事务样本不提供构建并发/文件 heap 峰值；本测仅 runtime apply | 候选有有限一次构建/读取预算，缓存成功与失败，保持取消后真实 job join 与原 source；最小可选输入/并发字段建议 `null`。不得双运行资源实例，失败/Unknown 记录不可提前丢弃 |

最小配置方向是复用已有 `runtime.network`，仅为确实缺失的 managed Direct/dispatch 并发占用和 pending bytes 增加一个小的 optional limits 对象；以上 field 名只是讨论中的作用域说明，未冻结 schema。全局 pending bytes 与 per peer/dispatcher FD/session 不得混为一个数字。旧配置缺字段、显式 null、保存往返及现有 Panel renderer/GUI 必须先用固定样本核验，才可采纳任何新增默认。若目标仍缺真实总上限和新旧配对测量，应保持验收未完成，不能把本报告当作激活范围扩大许可。

## 可保持兼容的最小授权方向

1. 一个真实 peer/dispatcher 保持一个 host resource owner，peer permit 贯穿 active 与 retiring。不要为每个路由结果创建无限新子图，也不要重建一代权限来绕过已耗尽的预算。
2. host 掌握真实 EgressControl、route/policy tag+revision、network generation 和已授权的解析结果。协议不能提交任意 selection、改接口或把一个 literal 的权限解释为任意 domain/IP。新的授权范围须实际覆盖 host 允许的候选和策略切换；不在首个 cap1 API 上静默放宽成 wildcard。
3. logical、native socket、pending acquisition、candidate storage、ingress/response queue bytes 分别计账，作用域明确。预留要在原生创建/任务启动前发生；原生 socket 一创建立即安装持久 owner。保留原 OS/Join source，包括配置、注册、取消和失败阶段。
4. 正常轮转可在同 peer 生命周期中取得新有限 lease。只有唯一 owner 在同一受控状态下确认不再有使用该 FD 的外部 handle/poll，执行原句柄实际关闭并记录 exact close receipt，才可回收该 FD lease；单个 FD 的真实停用不必等待所有同 peer 业务退出。若目前实现只能在 peer TaskExit 后关闭，就仍存在长期轮转累积占用的兼容缺口，不能靠 Drop/driver Ready/逻辑 closed/超时冒称已回收。
5. 原生 acquisition 的 KnownNoNative 与 Unknown 分开。后者保留原 source 和预算占用并拒新增或报告受影响 owner，不能预算回滚、重建 FD、重复 close Unix unknown 或丢一次性 receipt。正常失败 retry 必须有限且保留既有 fallback 语义，不能把一次故障耗尽的最小 grant 冒称完整 Direct 支持。
6. 完整验收还缺：实际 egress-generation 轮转、同 peer route/fake-IP/sniff 投影、多地址候选/失败与恢复、最大包/idle/迟到包、Unknown、每 owner payload reserve 和任务 census、新旧固定二进制相同负载，以及新字段 GUI/Panel 往返。当前建议数字保持 `null`；这不是默认激活许可。

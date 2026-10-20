# current31 Direct UDP 中间实测

本次完成固定源码的 library policy/network 各三轮，以及同一测量窗口中旧、新 CLI 各三轮。全部稳态 payload 收到回包；reload 和 burst 都存在缺包，且 current31 CLI 的 VmHWM 高于旧版三轮。脚本退出成功仅表示 fixture、公共 shutdown 和证据采集完成，不是流量全通过、默认容量校准或 ALL17 最终验收。新增容量建议仍为 `null`，没有改变生产默认或激活新的 endpoint queue 库。

## 固定输入

- current31：源码 `9e3f3ed33024e32f61685b0bff5bb4611f365109`，tree `12fdc3ad23e89a88ccf91ed56a46c0cd04128182`，源码 lock SHA `ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764`。这些数据不适用于后续 `58a08688`、pre-native TCP owner 或新 lease 的运行资格。
- library：`zero-udp-task-observer-current-9e3f3ed3`，实际 SHA `5ddbf5ac16ab2b61b7d64a4abcfe643a5e8e1f1427207e60665a833c602650b2`；独立 observer resolved lock SHA `33ba651da2fd9d53db89ccd84912046723459fd771dc91d61b9702d20024833a`。Tokio `1.54.0`、serde_json `1.0.154`、rustls `0.23.40`，一个 runtime worker。三个 Rust observer 源文件及原有 Python 探针与[历史旧 library](old-library-results.md)逐字一致；旧 Tokio 为 `1.53.3`，两者不是同一 dependency lock。
- CLI：旧源码 `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`，二进制 SHA `026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f`；current31 二进制 SHA `b649836fa7431de38b67f980d30453303b29216e1b3c741073ed8c74e1027dbd`。两者实际 build-info 的 features 顺序、数量及 git_hash 均已核对。
- 两份 CLI 相同的 17 个 feature leaves：`status-api,event-dispatcher,sink-jsonl,connector,grpc-api,host-network-stats,http,hysteria2,mieru,mixed,shadowsocks,socks5,trojan,vless,vmess,wireguard,dns`。current31 使用显式 `--no-default-features --features` 构建。runner 名中的 default 指普通生产执行路径，不指 Cargo 默认 features；library observer 的 features、任务与内存作用域另算。
- 固定镜像 `python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`，实际 `cpu.max=100000 100000`、`memory.max=1073741824`、`memory.swap.max=0`，`network=none`，所有 DNS、echo、客户端目标均为容器 loopback。没有系统路由或网络安全设置变更。

主代理明确 `CARGOFINISHED` 后才顺序启动 library policy3、library network3、CLI old3、CLI current3；测量全部结束后通知主代理恢复 Cargo。没有中途调整负载、超时、阈值或默认。launcher 历史通用字段仍写 parent validating，原字节保留；[build/运行计划证据](current31-build-9e3f3ed3-evidence.json)中的 measurement-window 记录实际无 Cargo 的窗口。CLI summary.scope 的历史 `old Direct Combined` 文案也原样保留，两者实际版本以 SHA/source/build-info 为准。

## 实际 payload

每轮 16 个真实客户端源，固定 domain，A/AAAA 轮换与 16 次实际 policy apply，1024 字节 payload，包含持续发送和 31/32/33 burst。下表每项是三轮累计“完整收到/实际发送（未观察到）”。发送数量受真实 apply 时长和 sender 调度影响，不能假定 reload 是完全相同的 offered load。

| 作用域 | 稳态 | reload 重叠 | burst |
| --- | --- | --- | --- |
| 历史 old library policy | 2448/2448（0） | 4248/4368（120） | 2016/4608（2592） |
| current31 library policy | 2448/2448（0） | 4013/4128（115） | 1267/4608（3341） |
| 历史 old library network | 2448/2448（0） | 4016/4528（512） | 2051/4608（2557） |
| current31 library network | 2448/2448（0） | 3992/4432（440） | 2722/4608（1886） |
| 本轮 old CLI | 2448/2448（0） | 4049/4368（319） | 1762/4608（2846） |
| 本轮 current31 CLI | 2448/2448（0） | 3769/4288（519） | 2035/4608（2573） |

历史旧 library 与 current31 是不同时间的三轮观察；policy burst 的差距没有被隐藏，但不构成可重复退化或原因归属的证明。本轮 CLI 配对中，新版 reload 缺包更多而 burst 缺包更少，不能据此给出一致性能改善或退化结论。原始发送/接收/重复/错误、每次 apply 与 payload 比对全部保留；本轮所有 trial 的 apply_failures、packet_errors、burst_errors、sampler_errors 均为空，shutdown 返回成功且进程退出 0。缺包单独记录，并未因此变成成功。

network 的每轮均保留 16 个成功的实际 EgressControl invalidation 响应，逐步 `0→1` 至 `15→16`；业务记录出现 generation `0..16`。这是共享 API 代次变化，未声称物理路由变化。policy 与 CLI 记录仅观察到 generation0。

## 资源观察

| 作用域 | 三轮 Tokio owned task 峰值 | 进程 VmHWM 范围（字节） | native thread 峰值 |
| --- | --- | --- | --- |
| 历史 old library policy | 20 / 21 / 21 | 57,516,032–58,843,136 | 6 |
| current31 library policy | 21 / 21 / 22 | 66,875,392–70,189,056 | 6 |
| 历史 old library network | 20 / 20 / 21 | 58,855,424–61,644,800 | 6 |
| current31 library network | 22 / 21 / 22 | 67,907,584–71,094,272 | 6 |
| 本轮 old CLI | 无真实 census，`null` | 57,253,888–60,416,000 | 5 |
| 本轮 current31 CLI | 无真实 census，`null` | 68,898,816–69,578,752 | 5 |

本轮 current31 CLI 三次 VmHWM 分别为 `69,578,752 / 68,898,816 / 69,578,752` 字节，old CLI 为 `60,416,000 / 60,211,200 / 57,253,888`。这是明确的观察增加，尚未归因或获得阈值验收，不能把代码段、heap、源码大小等猜测当作结论。library 的进程内还包含有界 Rust sampler 存储和两条实际 join 的探针 native threads，不能拿它与 CLI RSS 作差来推算产品开销。

所有本轮 trial 的观察业务 active flow 峰值为16，历史业务 flow ID 数为272；echo-port 关联业务 FD 采样峰值为16。library 全进程 FD 峰值27、UDP17，CLI 为29、UDP17。policy 与 CLI 历史关联 inode 各272；current31 network 为279/273/282。后者只是关联采样，包含同 PID DNS 等混淆可能，不是精确 native open attempt 数；16 active peer 也不能推导每 peer 并发 FD 或 attempt 默认。历史 single-source 的17次 lifetime opening 仍不能当 slots17。

current31 library 的 scheduler injection queue 峰值分别为 policy `1/0/0`、network `1/0/1`；这不是 UDP packet queue。六次实际首尾 owned-task 采样均为0，sampler 未截断，但 `before_proxy` 标签覆盖启动至首次 phase command，范围曾到4；`after_shutdown_and_owner_drop` 也可能有1到0的尾部。完整 phase 原样保留，不能把该标签或 gauge 当作 TaskScope join/native close receipt；spawn_blocking 不计入 owned task 数。

容器 memory.peak：current31 library policy `236,523,520`、network `317,497,344`，本轮 old CLI `315,596,800`、current31 CLI `331,354,112` 字节；包括 Python 原始记录、DNS、echo 和客户端，不是产品 PID 内存。各容器 OOM 计数均为0。本轮采样中初始 listener inode 均出现；历史 old network trial2 的一个非原子采样仍缺该 inode，原 false 未覆盖，不能据前后同 inode 推断线性一致或原生关闭。

应用 owner pending payload bytes、blocking job census、精确 native acquisition 次数仍没有实际公开计数 API，保持 `null`。内核 UDP RX queue 为内核记账字节，不能推算用户队列或资源授权。公开 shutdown 响应时间包含固定300ms observer tail，不作为产品 native shutdown 延迟。

## 可复核交付

- [完整逐轮与旧版对比 JSON](current31-observations.json)，由 [独立汇总脚本](collect_current31.py)直接读取原 raw/summary 得到。
- current31 library [policy 汇总](library-current-policy-5ddbf5ac-summary.json)、[原始证据索引](library-current-policy-5ddbf5ac-evidence.json)；[network 汇总](library-current-network-5ddbf5ac-summary.json)、[原始证据索引](library-current-network-5ddbf5ac-evidence.json)。
- 同窗口 CLI [old 汇总](matched-cli-old-current31-summary.json)、[current31 汇总](matched-cli-new-current31-summary.json)、[完整配对证据索引](matched-cli-pair-current31-b649836f-evidence.json)。
- [构建、resolved lock、源码 metadata、完整命令、fixture 源码与测量窗口](current31-build-9e3f3ed3-evidence.json)。包含主代理两份实际 build 日志及 exit，未由本代理启动 Cargo。

四个 gzip/tar 均按索引逐文件读取回验 SHA，原始 summary、payload、IPC、DNS/echo、procfs、phase/task、网络代次和退出结果均保留。二进制不入 Git，排除的副本先核对 SHA，并继续保存在 `/workspace/core-baseline-artifacts` 与测量目录。外部目录为 `/workspace/core-next/direct-udp-budget/library-current-{policy,network}-5ddbf5ac`、`matched-cli-pair-current31-b649836f`。证据只能约束已执行 profile；尚不足以选新的总 logical/FD/attempt/pending-byte 默认，不能把 RS01 总124FD 或 outer4096 变成每 peer 预算。

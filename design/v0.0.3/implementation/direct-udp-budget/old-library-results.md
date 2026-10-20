# 实际旧源 UDP Tokio task 与 generation 观察

这是固定旧源的补充测量，未改变产品/配置/默认值，未运行 Cargo。root 统一构建后提供实际产物，本代理核对字节与锁 SHA，再执行已准备的两个真实 profile。它不替代当前源、默认 CLI pair、容量耗尽或所有协议资格验收，不产生新容量默认。

固定旧源 `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`；观察器二进制 `/workspace/core-baseline-artifacts/zero-udp-task-observer-old-2eed`，SHA `25b58d67bd3e479e9cf375ed21e0bc52098084cb7a7a5339672af33f851c7fff`。独立 harness lock SHA `7e78fe70b928136213e300f9c137d6b19dafbd5adf120684579f622e5f3a6693`，Tokio `1.53.3`。root 的实际 verify/metadata/build 命令、退出码、日志、全部 harness 源/清单/锁和内嵌来源在 [构建证据](library-old-observer-build-25b58d67-evidence.json)；没有把待编译入口写成成功运行。

两个 profile 均采用固定 Python 镜像 `python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`，`--network=none`，实际 `cpu.max=100000 100000`、`memory.max=1073741824`、`memory.swap.max=0`。16 个保留至结束的实际 source socket、同一个固定配置 domain、17 阶段（含 16 次轮转）、每阶段每 source 3 个 1024 字节稳态 payload，另含持续发送与 apply 重叠、每 source 31/32/33 包突发。目标均为本地 echo `127.0.0.1/127.0.0.2/::1`；没有网络/安全/系统路由配置变化。父任务同时在验证当前代码，故这里不宣称隔离吞吐或延迟门禁。

[结构化总表](old-library-observations.json) 保留全部计数、初末任务样本、每轮峰值及限制。[policy-only 汇总](library-old-policy-25b58d67-summary.json)、[原始证据](library-old-policy-25b58d67-evidence.json) 与 [network+policy 汇总](library-old-network-25b58d67-summary.json)、[原始证据](library-old-network-25b58d67-evidence.json) 可逐项追溯。原始逐包缺失、控制帧、DNS/echo 到达、真实指标样本和对应执行脚本 SHA 均保留。

| 实际结果（各 3 轮） | policy-only | network + policy |
| --- | --- | --- |
| 稳态完整 payload | **2448/2448** | **2448/2448** |
| reload 重叠唯一响应 / 实际发送 | **4248/4368**，120 未观察到 | **4016/4528**，512 未观察到 |
| 突发唯一响应 / 实际发送 | **2016/4608**，2592 未观察到 | **2051/4608**，2557 未观察到 |
| IPC apply 时间窗内实际发送 / echo 到达 | 197 / 237 | 450 / 314 |
| 每轮 Tokio tasks 采样峰值 | **20 / 21 / 21** | **20 / 20 / 21** |
| Tokio scheduler global queue 采样峰值 | 1 / 1 / 0 | 1 / 1 / 1 |
| 原生线程采样峰值 | 6 / 6 / 6 | 6 / 6 / 6 |
| 每轮活跃业务 flow 峰 / 历史不同 flow ID | 16 / 272 | 16 / 272 |
| 每轮 echo 源 port/family 相关 FD 采样峰值 | 16 / 16 / 16 | 16 / 16 / 16 |
| 历史 echo port/family 相关 socket inode | 272 / 272 / 272 | 273 / 284 / 288 |
| 全观察器进程 FD 峰 / UDP FD 峰 | 27 / 17 | 27 / 17 |
| 观察器进程 VmHWM 范围（字节） | 57,516,032–58,843,136 | 58,855,424–61,644,800 |
| 全容器 memory.peak（字节） | 222,224,384 | 279,191,552 |
| 实际业务记录中的 egress generation | 仅 0 | 每轮均覆盖 **0–16** |
| OOM/oom_kill | 0 / 0 | 0 / 0 |
| public shutdown 返回 / 子进程退出码 | 3/3 成功，均 0 | 3/3 成功，均 0 |
| 每轮最终实际 Tokio task 样本 | 0 | 0 |

六轮指标均未触发 30000 样本截断，稳态比对及每次 apply 结果均成功，逐包 send/receive API 错误列表为空。表中的 missing 是真实未观察到响应，**不因为 harness 完成/退出 0 就算全流量通过**，也没有在缺少 owner observer 时把缺失归因给 ingress mpsc、OS RX、echo/client queue 或某一个恢复阶段。

Tokio task 由实际运行 Proxy 的同一 Handle.metrics 读取，观察器主动创建的 Tokio task 为零。它和原生线程数是两个独立指标；6 个原生线程包含两个实际 join 的观察线程。global queue depth 是 Tokio scheduler 工作，既不是 UDP message 数，也不证明业务 pending bytes 为零。`application_pending_payload_bytes`、精确 native open attempts 仍为 `null`，新容量建议也保持 `null`。16 source 的峰值 21 不是全应用/任意协议的任务上限。

phase 名 `before_proxy` 一直保留到 Python 首次安装 phase，所以该 phase 有启动过程的 0–3 个任务，不能叙述成纯 before 阶段全为 0；六轮最初样本确实为 0。`after_shutdown_and_owner_drop` 个别轮的过渡样本仍有 1 个任务，随后降到 0，原始样本没有删除。关闭响应包含固定 300ms 的退出后观察窗口，因此响应耗时不是内核专属 shutdown 延迟。任务归零、public shutdown 返回和 OS inode 消失均不能替代单个原生 FD 的真实 close receipt。

network profile 每次调用同一真实 `EgressInterfaceControl::invalidate_network()`，保留实际 before/after 0→1…15→16，业务 API 中也确实记录 generation0–16。这是实际 API 代次变更，**不是物理 route/interface 切换**。network 第三轮有一个标为 `rotation-5` 的非原子 procfs 快照未包含初始 listener inode，前后仍观察到同一原 inode；总表保留 false/阶段，既不标成“始终保持”，也不据此猜测已经 close/rebind。

历史 port/family 相关 inode 是采样关联结果，不是 exact native acquisition/close 账本：同进程 DNS 也使用 UDP，采样与逐包到达不线性一致，因此不能直接当每 peer grant 用量或证明任何资源已经关闭。network+policy 中 in-flight 流量可能在两次不同 fence 间获得不同 socket，但本报告不把这种解释当作精确 attempt 计数。观察器进程 RSS 包含实际 Rust 采样记录/控制 facade；容器峰值还包括 Python、echo、DNS 及其完整逐包/JSON 记录，不能拿它们反推应用队列字节或产品默认内存。

复现完整命令在两个压缩包的 `launcher.json`，脚本调用形式见 [观察器运行说明](observer/README.md)。实际输出目录分别为 `/workspace/core-next/direct-udp-budget/library-old-policy-25b58d67`、`/workspace/core-next/direct-udp-budget/library-old-network-25b58d67`。当前源观察器和默认 CLI old/new pair 尚未运行；只能在 root 冻结并编译相应真实二进制后按相同 profile 对照。

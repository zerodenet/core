# Current25 精确准备与 matched run 计划

本记录是 NoCargo 源码/输入核对，不是新观测结果。冻结目标为 `2e7ce766911e48cd1c0216a7358f089c9dcb4e93`，源 lock SHA `ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764`。准备时 tracked tree clean、HEAD/锁前后核对通过，公开 Proxy/ProxyHandle/query/egress API 仍与固定旧源兼容。独立 manifest 已生成：

`/workspace/core-next/udp-task-observer-current-2e7ce766-ffed93de/Cargo.toml`

后续 root26 已开始验证，root27 还将集成 listener shutdown 修复；因此 current25 仅保留为历史准备，尚未构建。最终 observer 要等 root27 的精确 HEAD/锁后使用参数化入口生成全新目录，不把旧准备改标签当作新来源。

同目录 `source.json` 和 `sole-owner-plan.json` 保存完整来源、精确依赖 pin、原始锁种子与构建步骤。此处 Tokio `=1.54.0`，旧库观察器 `=1.53.3`；serde_json 均 `=1.0.154`，rustls 均 `=0.23.40`。三份 observer Rust 文件 SHA 与已运行的旧源完全相同，输入/采样技术不变；transitive package 必须在 owning 锁中，不能重新按最新兼容版本漂移。新 observer 还没有构建/运行，最终 resolved harness lock SHA 和 binary SHA 均待 root 的实际产物，结构化见 [matched plan](matched-current25-plan.json)。

## 精确准备与构建

外部参数化入口 `/workspace/core-next/prepare-current-udp-observer.py` 与 [版本化副本](observer/prepare_current.py) 一致，强制显式完整 HEAD/锁摘要，新的输出目录必须不存在；现有 ea/12 准备不能替代 current25。仅生成清单和操作计划，不调用 Cargo。用法：

```sh
python3 -B /workspace/core-next/prepare-current-udp-observer.py \
  --source /workspace/core \
  --expected-head 2e7ce766911e48cd1c0216a7358f089c9dcb4e93 \
  --expected-lock-sha256 ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764 \
  --output /workspace/core-next/udp-task-observer-current-2e7ce766-ffed93de-REPEAT \
  --target-dir /workspace/core-target
```

root25 构建运行中，本代理不运行任何 Cargo。root 唯一 Cargo owner 在其排他窗口按以下顺序操作；唯一 target 使用 **`/workspace/core-target`**，不创建新的 `/workspace/core/target` 或 standalone target。所有 Cargo 步骤都沿用该 env，避免重复缓存消耗剩余磁盘。

1. `observer/prepare.py --output HARNESS --verify-only` 核对 HEAD/源码/锁与模板。
2. `CARGO_TARGET_DIR=/workspace/core-target cargo metadata --offline --format-version 1 --manifest-path HARNESS/Cargo.toml` 只解析独立锁种子中新增的 observer 包，完整保存输出。
3. 再次 `--verify-only`：检查所有 registry name/version/source/checksum 均在 owning lock。
4. `CARGO_TARGET_DIR=/workspace/core-target cargo build --release --offline --locked --manifest-path HARNESS/Cargo.toml`，保存实际命令、stdout/stderr、退出码、compiler 和最终独立 lock SHA。
5. 最后再次 verify。将 actual completed binary 复制到带 source 标签的不可覆盖产物位置并核对 SHA；共享 target 中同名 old/current 成品会互相覆盖，不能拿旧名字猜新 SHA。

若 strict fix 推进 HEAD 或源锁变化，准备/验证会拒绝原 current25。应先冻结新 source，再用新目录重做准备；不要让 metadata 仍写 current25 而依赖路径却编译了后来源码。已经完成且有完整构建证据的 current25 产物只能报告 current25，不自动代表后续修复。

## 两条独立比较线

| Lane | 固定旧输入 | 当前输入与状态 | 可比较范围 |
| --- | --- | --- | --- |
| public library observer | source2eed，binary SHA `25b58d67bd3e479e9cf375ed21e0bc52098084cb7a7a5339672af33f851c7fff`；两个 profile 各3轮已实跑 | 当前25 manifest 已准备；实际 binary/resolved lock SHA 待 root | 同一 Handle 的 owned async tasks 与同一库 fixture；排除 CLI/IPC/Connector，不把 observer RSS 当产品 CLI RSS |
| 实际产品 CLI 默认执行路径 | source2eed，binary SHA `026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f` | 需 root 另给同一冻结 HEAD 的实际 CLI 成品/Build-info | 相同配置/流量在真正默认执行路径；不借此宣称未被选中的新库 API 已生产激活或有界 |

固定旧 CLI 的实际 feature 列表包含 status-api、event-dispatcher、sink-jsonl、connector、grpc-api、host-network-stats、wireguard 及全部普通协议/DNS。源码 Cargo 默认 `full,status-api` 不包含其中额外 Connector/gRPC/host-stat/WireGuard features。因此 plain release 产物可以测其真实默认执行路径，但 feature 不同就不能标为同 feature 性能配对；采用同 feature 构建时也必须保存实际 build-info，不只根据构建命令猜测。library feature 清单独立固定在 `source.json`，不等于 CLI application feature 清单。

固定 old026741 的 build-info 精确 17 项，按实际输出顺序为：

```text
status-api,event-dispatcher,sink-jsonl,connector,grpc-api,host-network-stats,http,hysteria2,mieru,mixed,shadowsocks,socks5,trojan,vless,vmess,wireguard,dns
```

root 在最终冻结 source 排他构建匹配 CLI 的显式命令为：

```sh
CARGO_TARGET_DIR=/workspace/core-target cargo build -p zero --release --offline --locked \
  --no-default-features \
  --features status-api,event-dispatcher,sink-jsonl,connector,grpc-api,host-network-stats,http,hysteria2,mieru,mixed,shadowsocks,socks5,trojan,vless,vmess,wireguard,dns
```

已检查的 current25 RootCargo 只有上述17项外加 `default/full` 两个别名，`full` 只展开这些已选 leaf，`src/` 与根 `build.rs` 没有额外 full/default 条件行为；所以该版本 `--all-features` 没有额外 concrete leaf。最终 freeze 若增加 feature，必须重新核对，不能沿用这个结论。比较实际 build-info feature 集合时需同时报告 missing/extra，不能将名字相近的 alias 当成遗漏业务 feature。这里“默认”仅指普通生产执行路径、没有实验 activation 覆盖，不表示 Cargo 缺省 feature 构建。

每条 lane 均保留实际成功和失败，不混用其内存、任务数或延迟。若 current CLI 与 library 来源 HEAD 不同，应单独报告两个 source，不能合并为一个“当前相同源码”的结果。

## matched workload 与运行前后校验

固定 Python 镜像、networknone、实际 CPU1/1GiB/swap0、16 个 source socket、一个固定配置 domain、A/AAAA/族策略轮转、1024-byte payload、17 阶段、每阶段每 source3个稳态比对、每 source31/32/33突发，与旧实验完全一致。它不是从一个原始 source payload 携带任意多 wire target。

- library 分别运行 policy-only3轮、另目录 `--invalidate-network`3轮；后者真实调用同一 EgressControl 并保留 generation before/after 和业务路径记录，不能叫物理 route/interface 切换。
- CLI 使用 `run_default_pair.py` 或对应 `run_fanin.py` 顺序执行固定 old/new。library 使用 `run_library.py`，不能互换 binary 入口。旧/new 输入、脚本 SHA、actual build-info 与来源分别保存。
- binary SHA、source HEAD、源 lock SHA、resolved harness lock SHA、三份 Rust SHA 必须明确；library 内嵌 source.json 在实际运行后与传入来源逐字匹配，否则 raw 保留 provenance error，不视为有效比较。
- 检查实际容器 `cpu.max/memory.max/memory.swap.max`，不要用 host4CPU/16GiB 当已约束；记录同时工作的父构建，只做与实际条件相称的结论。
- 逐阶段记录 requested/apply acknowledgement、完整 steady payload、overlap、burst 的 sent/unique received/missing/errors；harness exit0 不等于全流量通过，steady 失败也不能被压力总平均掩盖。
- 任务记录 sample cap/truncated、初末及所有 transition。既有 `before_proxy` 标签含 startup 0–3，`after_shutdown_and_owner_drop` 可能含1→0过渡，不删样本构造稳定0。退出响应含固定300ms观察窗口，不能当产品 shutdown latency。

[源码真实性复核](observer-metrics-source-review.json) 检查了实际 Tokio1.53.3/1.54.0：`num_alive_tasks` 来自 owned task list；blocking pool 创建 unowned task，不在该 async gauge。`global_queue_depth` 只来自 scheduler injection queue；worker-local/阻塞队列和 UDP payload 队列均未被这两个数测得。stdin 的 `blocking_send` 只是原生线程上的 future::block_on，不创建另一个 runtime 或 Tokio task。采样器持有实际相同 Handle、每10ms读取，自己不 spawn Tokio task，但非原子样本仍可能漏短峰值。

因此 application pending bytes、blocking job/queue census、精确 native acquisition attempts、物理网络切换仍为 `null`/未测。Tasks0不证明实际 Scope 已收齐 TaskExit 或 I/O cleanup receipt；echo port/family/inode 关联也不替代真实 FD grant/close 账本。所有新容量建议继续 `null`，不从旧峰值21、outer4096、历史socket272或RS01全进程124FD推导默认。取得当前相同条件的真实数据后再决定是否需要配对控制重跑或更深入 owner 观察，而非凭空制定阈值。

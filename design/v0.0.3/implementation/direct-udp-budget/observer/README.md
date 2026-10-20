# UDP Tokio task 观察器：源代码准备

复用 [既有 proxy-observer](../../../baseline/proxy-observer/src/main.rs) 实际使用的稳定 `Handle.metrics().num_alive_tasks()/global_queue_depth()`，改为真实 Direct Combined fixed-domain、16 source peers、16 次策略轮转与持续流量。独立 harness 未改生产代码、激活路径或默认值。root 后续已统一构建固定旧源，本代理已完成 policy-only/network+policy 各三轮真实测量，见 [old-only 结果](../old-library-results.md)；当前源与默认 CLI pair 仍待真实构建和执行。

## 观察边界

一个真实 Tokio runtime、一个 worker。Rust main future 驱动实际 `Proxy::spawn`、公开 `ProxyHandle::apply_runtime_config_and_wait`、公开 query 及 `RunningProxy::shutdown`。采样器是一个实际持有同 runtime Handle 的原生线程，每 10ms 读取稳定 runtime metrics，最多保留 30000 个样本并明确记录 truncated。输入为另一个原生线程，1MiB frame 上限、mpsc1 控制队列；它不在 Tokio stdin 上创建 blocking task。成功响应前两个原生线程均真实 join；异常退出不构成 join/关闭确认。观察器主动 spawn 的 Tokio task 数为零。

任务观察包含库自身的全部 Tokio 工作，排除 CLI/IPC/Connector 应用任务，与既有 library census 作用域一致。Linux nativeThreads 包含两个观察线程，单独记录，不能替代 Tokio tasks。Tokio global queue depth 是 scheduler 工作，不是 UDP 应用消息队列。两个固定源都没有可读取业务 owner pending payload bytes 的稳定公开接口，因此逐样本和汇总保持 `null`，不从 RSS、内核 UDP queue 或任务数推算。

同一 fixture 仍保留实际端口与 socket 所有权、逐包成功比对/缺失、DNS/echo 记录、IPC 时间窗、实际网络 generation、procfs FD/RSS/nativeThreads 和退出状态。Rust observer 进程 RSS 包含其有界采样记录和控制 facade；Python/echo/DNS 另在同 cgroup。它不等于未经观察器包装的 CLI RSS。因此另跑固定 old/new **默认 CLI 二进制** pair，完整保留所有稳态、切换中和突发失败，不拿 library 结果替代产品默认路径。

可选 `--invalidate-network` 通过实际 `proxy.egress_interface_control().invalidate_network()` 驱动同一宿主共享的真实 EgressControl，每次记录 generation before/after；固定旧源 `2eed` 和当前源均有这项公开 API。没有系统路由/TUN/DNS 配置变更，不宣称物理接口切换。默认不打开该选项，使 policy-only profile 可与之前真实 workload 对照。网络+policy 轮转应另存一次 profile。

## 精确源码与依赖

`prepare.py` 只用 Python 标准库：核对源 HEAD 和 tracked tree clean，读取该源实际 Cargo.lock，exact pin Tokio/serde_json/rustls，并逐项复用该源 `[patch.crates-io]`。生成独立 `[workspace]` manifest 和自己的锁种子，不修改拥有源的 manifest/lock。Rust 源 SHA 与 source SHA/lock SHA 一并编入二进制，执行前再次 `--verify-only`。

已准备目录：

- `/workspace/core-next/udp-task-observer-old` → `/workspace/core-baseline-source`，`2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`；Tokio `=1.53.3`，serde_json `=1.0.154`，rustls `=0.23.40`，源 lock SHA `0275dc0ac51fad8d380056defb4e4fb651edff477d0f8e6cb0166c05ca720cd7`。
- `/workspace/core-next/udp-task-observer-current` → `/workspace/core`，准备时 `ea1fa7b4efb2d668cb9c7456f92a38d7fe606c35`；Tokio `=1.54.0`，其余同上，源 lock SHA `a003359eb327bd299b675c330e9aeca721156df76d117dd3e55c9c47f711377c`。

Tokio 版本差异如实记录，不强制当前源退回旧 runtime 来伪造配对。若当前主树继续集成，旧 current manifest 会在 verify 阶段拒绝；为新冻结 SHA 准备新目录，不能照用过期 source.json。

准备期间主树已从 `ea1fa7b4` 前进到 `12a68916`，`current --verify-only` 已按预期拒绝这个过期来源；因此 current 目录只是尚未构建的历史准备，下一轮必须按实际新冻结 SHA 再生成。old 目录的源码/锁/模板核对通过。

```sh
python3 -B design/v0.0.3/implementation/direct-udp-budget/observer/prepare.py \
  --source /workspace/core --expected-commit ACTUAL_FROZEN_SHA \
  --output /workspace/core-next/udp-task-observer-current-NEXT
```

proxy features 与既有 library observer 保持相同：socks5/http/mixed/vless/hysteria2/shadowsocks/trojan/vmess/mieru/wireguard/dns/host-network-stats，`default-features=false`。没有额外 enable 新组件或改变 runtime defaults。

## 唯一 Cargo owner 的最小构建命令

以下交给 root，在全部 workspace 验证完成且磁盘有余量后串行执行；本代理尚未运行。`TARGET` 由 root 选择并记录，不能让第二个构建进程写其共享 target；old/new 如共享缓存，也必须串行且每次拷贝/核对成品，后一次同名 binary 会覆盖前一次。

```sh
python3 -B design/v0.0.3/implementation/direct-udp-budget/observer/prepare.py \
  --output /workspace/core-next/udp-task-observer-old --verify-only
cargo metadata --offline --format-version 1 \
  --manifest-path /workspace/core-next/udp-task-observer-old/Cargo.toml > OLD_METADATA_LOG
python3 -B design/v0.0.3/implementation/direct-udp-budget/observer/prepare.py \
  --output /workspace/core-next/udp-task-observer-old --verify-only
CARGO_TARGET_DIR=TARGET cargo build --release --offline --locked \
  --manifest-path /workspace/core-next/udp-task-observer-old/Cargo.toml
```

为实际当前冻结源生成的新目录重复上面步骤。保存命令、所有 stdout/stderr、实际 compiler/source/lock/features 和最终 binary SHA。metadata 只为新独立 harness 的锁种子解析额外的 observer package，避免 generate-lockfile 全面选择新的兼容版本。第二次 verify 会拒绝 owning lock 中不存在的 registry name/version/source/checksum。源 workspace 的 lock 仍原样，最终 harness lock SHA 随运行保存。不要在验证结束前由本代理运行 Cargo。

## 构建后运行

`run_library.py` 与既有 `run_fanin.py` 一样核对成品 SHA，采用同一固定 Python 镜像、`--network=none`、实际 1 CPU/1 GiB、swap0，并记录完整 Docker 命令。每个旧/新产物都分别执行下面 profile，各三轮，输出目录必须不存在：

```sh
python3 -B design/v0.0.3/implementation/direct-udp-budget/run_library.py \
  --binary ACTUAL_OBSERVER_BINARY --expected-sha256 ACTUAL_BINARY_SHA \
  --source-commit ACTUAL_SOURCE_SHA --observer-metadata ACTUAL_HARNESS_DIR/source.json \
  --observer-lock ACTUAL_HARNESS_DIR/Cargo.lock \
  --output /workspace/core-next/direct-udp-budget/library-SOURCE-policy \
  --trials 3 --payload-bytes 1024
python3 -B design/v0.0.3/implementation/direct-udp-budget/summarize_library.py \
  /workspace/core-next/direct-udp-budget/library-SOURCE-policy
```

以另一个输出目录加 `--invalidate-network` 再跑真实 network+policy 轮转。`library_worker.py` 通过显式 `kernel_type` 参数复用同一真实 16-source `fanin.trial`，没有换成 mock dispatcher、虚构任务或伪造收发结果。原测量执行时的旧脚本 SHA 仍在既有压缩证据中；新增可选 fixture 参数不改那次历史执行。

默认 CLI pair 另用已有 `run_fanin.py` 的相同命令，分别填固定 old/new 产品二进制 SHA/来源提交，不能把 observer 当 CLI 运行。`summarize_fanin.py` 保留实际 missing/错误；零退出码只代表 harness 完成，不等于压力收发全通过。任务 census 和 pending bytes 建议值须等真实结果后讨论，现阶段仍为 `null`。

默认产品二进制 pair 也可用顺序入口（仍须 root 给定实际产物/完整来源和 SHA）：

```sh
python3 -B design/v0.0.3/implementation/direct-udp-budget/run_default_pair.py \
  --old-binary /workspace/core-baseline-artifacts/zero-release \
  --old-sha256 026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f \
  --old-source 2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a \
  --new-binary ACTUAL_NEW_DEFAULT_BINARY --new-sha256 ACTUAL_NEW_BINARY_SHA \
  --new-source ACTUAL_NEW_SOURCE_SHA \
  --output /workspace/core-next/direct-udp-budget/default-pair-SOURCE
```

两边实际 build-info、完整 Docker 命令、实际失败和派生 summary 都保留；须核对实际 features 才能声称同 feature 配对。入口零退出码只表示两次实验及汇总完成，不把 UDP 压力缺包改成验收通过。

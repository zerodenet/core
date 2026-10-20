# current31 配对 smaps 诊断：只准备，未运行

**状态：PREPARED_ONLY_NOT_EXECUTED。** 本目录是额外诊断 profile，不是原基线重跑或资源资格。必须等 root 明确确认 **CARGOFINISHED** 并授权这个诊断后，才可使用执行命令。命令行 marker 本身不代表授权。本轮没有 Docker、产品二进制、负载或 Cargo 执行。

## 固定输入与保持项

`profile.json` 固定 old `2eedc2b8` / current31 `9e3f3ed3` 的现有 release 二进制路径和完整 SHA，原镜像 `python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`，无拉取、无外网、仅 loopback，1 CPU / 1 GiB / swap0。不重新编译、不复制两份二进制，挂载原固定文件只读；每次 future launch 前核SHA，容器内在发业务包前重核 SHA 和真实 build-info 的同序17features。

六份原 fixture 逐字节放在 `originals/`，SHA与两版既有 launcher 一致。`peers.py`、`worker.py`、`summarize_fanin.py` 原样复用；`fanin.py` 的整个 Clients 类 AST保持完全一致。仍按old→new各三轮，16源、固定domain与本机DNS/echo、1024B、rotation0..16、原三次31/32/33每源burst。缺包、重复、未匹配/错误、IPC frames和实际收包原样保存，不添加通过阈值，不改默认/额外闲置/更多epoch。

原操作超时保持：readiness20秒、IPC10秒、collect1秒、sender启动2秒/join3秒、正常shutdown15秒/TERM3秒/KILL3秒。新增诊断完整快照在阶段边界同步读取，**会延长业务阶段之间的间隔并增加同cgroup的Python/文件读写开销**，old/new采用同一规则，不与原profile的吞吐/延迟/缺包作无条件相等比较。独立container总watchdog为30分钟，仅防诊断失控；触发即记录失败、按本次cidfile的确切containerID清理，不能称产品正常关闭。

## 采样时序

1. `MappingProbe` 的固定binary SHA/ELF geometry在Popen之前完成。记录 `popen_started_ns`、`popen_returned_ns`，返回后立即启动原10ms轻量Sampler，记录启动请求、线程实际进入、每次样本读取首末时间；最早样本是真实观察点，**不是exec瞬间或全部启动分配**。
2. readiness逻辑仍看同control socket与该PID唯一loopbackUDP inode。记录 `readiness_confirmed_ns`；原20秒budget锚定Popen返回，早期观察成本计入。确认后、首次apply前保存一次完整快照。不能拿control socket出现或一次stats读当host内部Ready的新增证明。
3. 完整 `status + smaps_rollup + maps + smaps` 共8个点：readiness后/首次apply前、rotation0后、rotation8后、rotation16后、每个burst后、shutdown开始前。均在实际原阶段完成后请求，保存request/readstart/readend/全部elapsed，不在10ms循环读smaps。
4. Popen后、关闭stdin后、实际进程退出后各尝试轻量status/rollup。原Sampler在真实shutdown wait期间继续运行，保存最后可读status样本。快速退出时末次地址空间可能已消失；明确 `expected_post_exit_unavailable`，不把未读到smaps补为0，不拿关闭前快照冒最终释放状态。
5. 所有文件是连续读取，**不是原子快照**。按PID的 `/proc/PID/stat` start_ticks 与实际 `/proc/PID/exe` dev/inode核同一进程，防PID重用；maps/smaps header不一致也原样标识，不自行合成一致结果。读失败、权限、文件超过明确16MiB诊断单文件上限均保留incomplete；未读到完整文件时不偷偷截断再声称完整分类。

每点 raw文件有长度、SHA、读首末时间；binary有原文件SHA、dev/inode、ELF PT_LOAD/GNU_RELRO/page几何。进程exe身份用实际inode/dev匹配挂载冻结文件，不能仅按路径名 `/kernel` 归类。

## resident 分项及其实际限制

`smaps_parse.py` 按真实VMA汇总 Rss/Pss、Private/Shared Clean/Dirty、Anonymous、Swap/SwapPss/Locked，原始字段和完整每VMA记录均保存：

- 精确主二进制 inode/dev 的 RX、RW、R-other、R-RELRO；load bias由实际映射与PT_LOAD匹配，最终保护取真实permissions。RELRO页范围来自GNU_RELRO与page边界，不能从section的WA猜最终保护。
- 若RELRO与非RELRO同VMA，保 `binary_R_mixed_RELRO_not_apportioned` 和真实overlap bytes，**不按面积比例猜RSS**。load bias不唯一则标RELRO-unresolved，不制造精确分项。
- 其他映射的dev/inode与实际文件header核对，ET_DYN独列 `other_mapped_dynamic_elf`（实际其他共享ELF映射，不宣称每个逻辑库归属）。无法验证则保unverifiedotherfile，不用名字猜RSS。
- `[heap]` 仅标brk heap，`[stack]`单列，其余匿名映射独列；**RssAnon不全叫heap**。主ELF零填充可能是匿名VMA，未另证不能并回主binary；thread stack若内核不标名字也留匿名，不按大小猜stack。
- VmRSS/status、rollup和每VMA的读时点不同，差额不能直接算heap、泄漏、代码映射净贡献或owner队列。没有allocator/smaps因果以外的实际追踪，不校准新容量默认。

## 输出与失败处理

输出是新的独立目录，存在则拒绝覆盖：`old|new/data/trial-N/{raw.json,ipc.json,config.json,kernel.log,shutdown.json,memory/...}`；原manifest、summary和逐包缺失列表继续保留。新增 `memory/index.json`、每点 `capture.json`、原proc文件和mapping-account.json。`pair.json`记录命令、脚本SHA、实际build-info、manifestSHA及退出结果。异常路径仍用原Kernel.finish收/杀并wait实际产品进程；诊断总watchdog触发不是正常产品shutdown/FDACK。No default或threshold更改。

## 可执行命令

**现在允许的仅准备检查/打印，不启动产品或Docker：**

```sh
PYTHONDONTWRITEBYTECODE=1 python3 /workspace/core-next/current31-smaps-diagnostic-prepared/check_prepared.py
PYTHONDONTWRITEBYTECODE=1 python3 /workspace/core-next/current31-smaps-diagnostic-prepared/run_pair.py --output /workspace/core-next/current31-smaps-diagnostic-run-after-authorization
```

`execution-plan.json` 已保存相同只读打印结果。它会实际读两份固定binary核SHA，但不执行binary、Docker或创建结果目录。

**仅在 root 后续明确 CARGOFINISHED + 授权后执行（本轮未执行）：**

```sh
PYTHONDONTWRITEBYTECODE=1 python3 /workspace/core-next/current31-smaps-diagnostic-prepared/run_pair.py --output /workspace/core-next/current31-smaps-diagnostic-run-after-authorization --execute --authorization-note CARGOFINISHED
```

将来需要另一次诊断时必须换新output目录，不能覆盖原基线或本次诊断。两版本顺序与原pair相同；如另加交错顺序/无smaps对照，须另profile另授权，不默改此轮。

## 时间/磁盘估计（未实测）

旧六轮真实trial窗口为17.4–20.4秒；含容器/summary、prehash和8次稀疏完整proc读取，**本诊断配对初估3–5分钟**。读取耗时、容器启动和抖动尚未测，不能承诺上界；单container30分钟watchdog仅诊断防失控。

既有old/new原data共134,929,743字节（约135MB）。新原manifest会再次包含部分原raw数据，稀疏smaps/maps预期再增几十MB，**建议预留400MiB磁盘**，不复制约123MB两binary。每文件16MiB上限只是防异常collector文件膨胀，遇上限判诊断不完整，不能把这个数当产品内存预算。1GiB cgroup中Python DNS/echo/client/collector与被测PID同在，RSS取实际产品PID；容器memory.peak不是产品RSS。

准备时只运行了Python语法、原脚本SHA/Clients AST、固定binary只读SHA和纯smaps文本parser夹具检查。没有在真实产品上采集任何新smaps，没有新负载、Docker或Cargo结果。

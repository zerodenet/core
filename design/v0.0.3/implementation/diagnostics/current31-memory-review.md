# current31 CLI 内存增长：只读证据复核

现有轨迹支持：current31 的 RSS 差异在首次 policy apply/稳态阶段已明显出现；之后旧、新版在16轮流量/策略变化中都继续增长，burst 达到更高峰值，关闭期间又观察到 RSS 回落。因此这次差异不能简单描述为“只在长时间流量后才出现”，也不能据此归因为 heap leak、代码映射或某项队列策略。当前没有 smaps/allocator 分项证据，尚不能分配原因或做默认容量校准。

本复核只读取已保存的 raw、固定二进制和源码，没有执行二进制、Cargo、新负载或产品代码修改。结果仅对应 current31 `9e3f3ed33024e32f61685b0bff5bb4611f365109`，不对应后续 `58a08688` 或 root34 的 listener/lease 修复。

## 固定来源和可复核材料

- old CLI：`/workspace/core-baseline-artifacts/zero-release`，SHA `026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f`，源码 `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`。
- current31 CLI：`/workspace/core-baseline-artifacts/zero-matched-current-9e3f3ed3`，SHA `b649836fa7431de38b67f980d30453303b29216e1b3c741073ed8c74e1027dbd`。两份实际文件重新计算 SHA 后读取。
- 原始轨迹：`/workspace/core-next/direct-udp-budget/matched-cli-pair-current31-b649836f/{old,new}/data/manifest.json`，与每个 `trial-N/raw.json` 一并保留；本次不改原始文件。
- [完整派生数据、每 phase 首末/极值、原命令与输出 SHA](current31-memory-review-data/derived.json)。同目录保存两份 `readelf -W -S`、`readelf -W -l`、`readelf -W -d`、`readelf -p .comment` 和 `size -A -d` 的原始输出。所有下列数值单位为字节；sample 时间仅相对该 trial 首个采样点。

CLI 实际 build-info 具有相同且同序的17项 features，均为 release；`default` 仅指普通生产执行路径，不指 Cargo 默认 features。两份 ELF `.comment` **完全一致**：`rustc 1.99.1 (58cf8f9c3 2026-10-13)`、`LLD 23.1.1`（同 LLVM commit）、`GCC (Debian 14.2.0-19) 14.2.0`。所以不能将本次差异归为已确认的 compiler 版本变化。固定文件内嵌的 Tokio 路径分别为 `tokio-1.53.3`、`tokio-1.54.0`，与已有 lock/pin 证据一致；这只是已确认的依赖差异，不是内存原因证明。两者 DT_NEEDED 名称也同为 `libgcc_s.so.1`、`libm.so.6`、`libc.so.6`、`ld-linux-x86-64.so.2`，不代表各映射实际驻留页相同。

## 采样窗口先于什么、晚于什么

`fanin.trial` 先构造 `Kernel`，其构造器等待 control socket 可用、从该 PID 找到唯一 loopback UDP listener 后返回，然后才启动 `Sampler`。所以 `startup` 是 **readiness/listener 发现后的一个样本**，不是 exec 初始 RSS，也没有覆盖全部启动分配。六轮该标签都只有1个样本，随后 sampler 标签转为 `rotation-0`；不能用它判定“启动时完全没有差异”。

`rotation-0` 包含首次真实 apply、16源稳态 payload、active_flows 查询和等待，phase 不是纯“无流量配置”阶段。后续 rotation 同时包含真实 reload、持续发包和查询，无法仅用现有标签区分 apply、数据路径、DNS、API 序列化各自的内存。procfs 的 status/FD/socket读取也不是同一原子快照。

## 六轮实际轨迹

| 版本/轮次 | 首个 startup VmRSS | rotation0 末 VmRSS | rotation16 末 VmRSS | VmRSS 采样最大 | VmHWM 采样最大 | 关闭阶段末 VmRSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| old 0 | 25,518,080 | 33,017,856 | 48,918,528 | 60,416,000 | 60,416,000 | 55,865,344 |
| old 1 | 25,804,800 | 32,862,208 | 48,750,592 | 60,211,200 | 60,211,200 | 55,853,056 |
| old 2 | 23,011,328 | 30,232,576 | 46,665,728 | 56,098,816 | 57,253,888 | 53,456,896 |
| current31 0 | 28,372,992 | 41,938,944 | 58,163,200 | 69,578,752 | 69,578,752 | 58,662,912 |
| current31 1 | 28,119,040 | 41,771,008 | 57,688,064 | 68,898,816 | 68,898,816 | 58,142,720 |
| current31 2 | 28,389,376 | 42,086,400 | 58,294,272 | 64,753,664 | 69,578,752 | 58,806,272 |

观察的范围与限制：

1. **最早可见样本已有差异。** old startup 为23,011,328–25,804,800，current31 为28,119,040–28,389,376。startup→rotation0 的观察增量 old 为7,057,408–7,499,776，current31 为13,565,952–13,697,024。这一阶段已包含 apply/真实流量，不能把额外增量全叫启动基线。
2. **早期差异随后仍在。** rotation0 的两组范围已经分开约8.8–11.9MB；rotation16 仍分开。若仅将同编号 trial 做描述性对照，rotation0 的差为8,921,088 / 8,908,800 / 11,853,824，rotation16 为9,244,672 / 8,937,472 / 11,628,544。旧三轮全部先于新三轮执行，并非随机交错设计，同编号不是因果或统计配对。
3. **两者后续都增长，增量接近。** rotation0末→rotation16末，old 为15,900,672 / 15,888,384 / 16,433,152，current31 为16,224,256 / 15,917,056 / 16,207,872。现有窗口不足以证明16轮以后会平台化或继续无界增长，更不能据此排除或确认泄漏。历史记录、热身、allocator保留、映射缺页、数据缓存等具体原因均未测定。
4. **所有六轮最大 VmHWM 都在 burst 标签内。** 三种 burst 共31/32/33包每源，各次1024字节；缺包仍按原结果保留。old2 与 current31-2 的 VmHWM 最大值高于任何被10ms目标间隔采到的 VmRSS，因此短峰可能没有对应的 RSS 样本；不能拿两者差值当作某类缓存大小。
5. **关闭阶段看到部分 VmRSS 回落。** 相对每轮最大被采到的 VmRSS，old 回落4,550,656 / 4,358,144 / 2,641,920，current31 回落10,915,840 / 10,756,096 / 5,947,392。最后样本的线程数已为2、FD为9、TCP/UDP列表为空，仍是进程存活期间的关闭过程，不是退出后的稳态。六轮最终 wait/exit0，不存在可继续采样的同一进程退出后 RSS；不把缺失样本填为0，也不把 socket列表空当原生 close receipt。
6. **不能用最后一个 VmHWM代替峰值，也不能用其下降解释释放。** 原始 status 的 HWM 在六轮都出现过下降（次数1/8/1与1/4/2）；例如 old0 在 burst 记录60,416,000，末关闭样本为59,969,536。未进行专门内核计数/重置验证，不推测原因、不修平该原始现象。上表保留 `max(observed VmHWM)`，内存回落描述只用实际 VmRSS，精确轨迹在派生JSON。

## 固定可执行文件的静态尺寸

以下是 ELF 文件/section/program-header 几何信息，**不是**实际 resident mapping 测量。本轮没有保存该进程的 maps/smaps；文件变大或 PT_LOAD MemSiz 变大，不表示这些页都驻留，更不能直接与进程 RSS 差额相减来算 heap。

| 项目 | old | current31 | 增量 |
| --- | ---: | ---: | ---: |
| 文件实际字节 | 58,579,864 | 64,142,136 | 5,562,272 |
| `.text` | 42,469,564 | 46,458,748 | 3,989,184 |
| `.rodata` | 3,700,512 | 3,934,160 | 233,648 |
| `.gcc_except_table` | 2,778,280 | 3,208,128 | 429,848 |
| `.eh_frame` | 4,803,504 | 5,295,212 | 491,708 |
| `.eh_frame_hdr` | 675,396 | 745,684 | 70,288 |
| `.rela.dyn` | 2,334,024 | 2,529,240 | 195,216 |
| `.data.rel.ro` | 1,671,712 | 1,818,984 | 147,272 |
| `.data` | 59,752 | 60,104 | 352 |
| `.bss` | 33,252 | 33,252 | 0 |
| TLS `.tdata` / `.tbss` | 304 / 408 | 368 / 408 | 64 / 0 |
| PT_LOAD R MemSiz | 14,308,800 | 15,729,940 | 1,421,140 |
| PT_LOAD R E MemSiz | 42,471,920 | 46,461,104 | 3,989,184 |
| PT_LOAD RW 第1段 MemSiz | 1,738,832 | 1,890,832 | 152,000 |
| PT_LOAD RW 第2段 MemSiz | 93,036 | 93,484 | 448 |

这些信息支持“当前可执行文件代码与只读/重定位等静态内容增加，`.bss` 未增加”；不能支持“RSS增加全由代码映射”或“扣掉5,562,272之后就是heap”。RELRO的最终页保护与脏/共享页记账也不能仅从 WA section flags推断。相同 DT_NEEDED/线程数量同样不能证明共享库/stack驻留量不变。

## 尚未执行的最小下一诊断

保持本次固定old/current SHA、17 features、镜像、1CPU/1GiB、1024字节和原始负载/超时不变，另设诊断试次并保留原测量对照，不修改默认或阈值：

1. **先补映射分项，再谈heap。** 将外部 sampler 提前到 Popen后立即开始，记录最早实际样本时间，仍不冒称 exec瞬间。至少在 readiness后但首次apply前、rotation0后、rotation8后、rotation16后、各burst后、关闭开始/末尾，保存 status、smaps_rollup，并在少量关键点保存 maps+smaps。获取 RssAnon/RssFile/RssShmem、Rss/Pss、Private/Shared Clean/Dirty、Anonymous、Swap，以及各文件路径/地址区间；读smaps本身有开销，给两版本相同采样负担并记录时间，避免10ms全量smaps干扰负载。
2. **以实际resident页核对ELF假设。** 将 smaps 中该精确二进制 inode/path 的R/RX/RELRO/RW映射驻留页，与共享库、heap、stack及其他匿名映射分开；不能用段的虚拟上限代替RSS。现有仅status数据无法完成此步。
3. **仅在匿名/私有分项确实增加时做allocator/owner诊断。** 先确认实际生产allocator，再选择能覆盖实际Rust分配的工具或受控instrumentation；任何需要重新编译的诊断二进制都由root单独构建，并与未仪器化产物分开标记开销。不默认 LD_PRELOAD 能拦截所有分配，不把全部RssAnon都叫heap。若增加来自实际队列/缓存，必须从真实owner取得计数及生命周期，不能由RSS逆算pending bytes。
4. **区分热身、保留与持续增长。** 在明确许可的诊断扩展中，给同样负载加入相同的空闲/停止业务后观察窗口，记录真实活跃/retired owner及缓存计数；若仍需测试超过16次的epoch，应先固定新profile并成对运行。RSS不降本身不能区分仍被引用的对象、allocator保留和匿名页驻留；单次回落也不能排除后续持续增长。不得将测试阶段的queue库冒充已生产激活的原因。

以上步骤均尚未运行。本次仅得到“早期已有差异、业务期继续增长、burst峰值、关闭期部分回落、静态ELF变大”的事实组合；仍缺resident分项和实际allocation/owner证据，原因与新容量默认均待定。

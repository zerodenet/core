# 独立 smaps / root40–41 证据归档

本归档独立于已安装 through39 的 **470 条记录**，不修改其 manifest/report/gzip/raw，不赋予当前 HEAD 或后续 root42 新资格。所有记录逐项保留 raw / gzip 长度与 SHA；gzip 固定 mtime=0。原路径未覆盖，ELF 不入库、不执行。`archive-manifest.json` 区分原始 raw、派生诊断、历史 prepared 输入、冻结命令证据与单项 syscall 诊断。

## current31 smaps 诊断

[完整派生报告](derived/report.md)、[输入核验](derived/input-verification.json)、[原派生输出 digest](derived/output-digests.json) 和 [ELF zero-fill 证据](derived/elf-zerofill.derived.json) 保持原字节。381 个原输出（144,861,227 bytes）全部作为 gzip 归档，包括原 pair、两个 container logs/cid、两次 **FileNotFoundError / summary exit1**、6份 manifest内trial与raw、全部maps/smaps/status/rollup/capture/index及IPC/缺包/退出证据；原 `launcher.json` 未生成，不在原路径伪造。派生目录 **11个输出+原output-digests.json索引，共12文件** 全部归档，两份 launcher metadata 明示 derived，不更改原 pair exit1。

prepared 的 **22 文件**（全部脚本、6份 originals、profile、原准备检查/文件digest与README）原字节归档，排除缓存 pyc。其 PREPARED_ONLY 文字是原历史准备状态；实际授权执行结果看原 pair，不能改原准备文案伪造当时已运行。原脚本 SHA、profile SHA 与实际 pair 交叉匹配；二进制身份见 [metadata](metadata/binary-identities.json)，仅保绝对 artifact 路径及 SHA，不复制 ELF。

范围为 old `2eedc2b8` / current31 `9e3f3ed3`，不是 root40/41/当前 HEAD。所有 steady 各48/48；overlap/burst缺包保持原记录。48个完整VMA点中47个maps/smaps headers一致，new trial1 readiness一处不一致原样保留；6次真实退出后地址空间不可用不补零RSS。rotation0后三轮new多约7.8MiB RSS，21次5888KiB全resident匿名VMA经实际PT_LOAD/loadbias证明不属于mainELF zero-fill（全部LOAD尾部增量仅544B），其用途仍unknown，不能猜queue/heap/default或全部性能因果。

## 冻结 root40 / root41 与原失败

| 冻结 | 实际证据 | 未取得的资格 |
| --- | --- | --- |
| root40 `670be674ac64d758ca14e8c04703448cc3173ce2`，tree `fdd033faa34c45b9597b62b039d4b7d75e929684` | fmt exit0；workspace all-features/all-targets check exit101。三处 `notify_hosted` E0624 private（join.rs179/188、scope.rs263），lib与lib-test编译失败。 | 该冻结没有运行后续runtime tests；原错误保留。 |
| root41 `7e1b9986bec750905350472f3392e49efdce894b`，tree `d234bda1a850c56d94413d52f50e21ba7e08beb0` | fmt/alltargets/layout通过；platform **124/0/1**，contracts **82/0/0**，transport **195/0/1**，Proxy **652/0/2**，runtime/architecture guards **189/0/0 + 26/0/0**；Windows GNU `cargo check --tests` exit0（仅编译）。 | strict exit101：hosted/tests/wake.rs86 manual_noop_waker、tests/file/owner.rs105两处unnecessary_cast，共3个原test lint；后续范围不称已通过。没有新WG recovery通过，也没有whole workspace/release/下游或Windows运行资格。 |

两个冻结 lock SHA 均 `ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764`。固定 Git tree 与原日志 counts / selected条目均独立核对，原命令 source_clean / head_unchanged / lock_unchanged flags保留。root41后继成功不改写root40编译失败，root39的WG恢复失败也不被未运行的WG命令覆盖。

## root41 单项 Packet no-probe syscall

[原命令与二进制身份](packet-single/pr05-parallel-41-packet-noprobe.json)、[原测试日志](packet-single/pr05-parallel-41-packet-noprobe.txt)、[原strace](packet-single/pr05-parallel-41-packet-noprobe.strace) 均保留。源身份为上述 **root41 `7e1b9986`**，固定binary SHA `4b12815f90c5106fea998da3b0e48699ce2dc1f12f9712eca9bdaa6af8a30dd5`。仅运行 `required_native_probe_is_unsupported_without_new_native_attempt`，真实1/0、124 filtered out；原trace内一个声明的 AF_INET UDP socket、一次bind、同fd9 close，未见其他socket或connect。测试拒绝需要native probe的发送，证据只支持该单项的原ledger/系统调用范围，不是native packet全面资格、benchmark或当前HEAD全量运行。

## 只读复验与复制

先运行 `python3 -B preparation/verify-smaps-through41-archive.py --base <本目录>`，逐项核所有gzip/raw。加 `--originals` 可核保留的原绝对artifact和3个固定binarySHA，加 `--repo /workspace/core` 可核两冻结Git tree与through39原470条未变；没有运行Cargo、Docker、产品/测试binary。

父代理在自己的冻结完成后可用 `preparation/copy-smaps-through41-archive.py --stage <本目录> --destination <NEW独立diagnostics目录>` 安装。目标必须不存在，脚本验证后只复制本独立文件夹，不修改任何已有through39文件。复制前后的只读核验与digest见stage外说明；制备脚本故意拒绝覆盖已有stage。此归档不是ALL17、RuntimeHost M4、完整资源替换/平台/最终HEAD/下游/性能资格。

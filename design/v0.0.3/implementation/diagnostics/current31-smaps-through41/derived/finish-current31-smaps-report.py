"""Offline ELF geometry proof and diagnostic report; preserves all original raw files."""
from pathlib import Path
import hashlib
import json
import runpy
import statistics
import shutil

out = Path('/workspace/core-next/current31-smaps-offline-derived')
raw = Path('/workspace/core-next/current31-smaps-diagnostic-run-after-authorization')
prepared = Path('/workspace/core-next/current31-smaps-diagnostic-prepared')
sha = lambda b: hashlib.sha256(b).hexdigest()
d = json.loads((out / 'memory-phases.derived.json').read_text())
verification = json.loads((out / 'input-verification.json').read_text())
profile = json.loads((prepared / 'profile.json').read_text())
identity = runpy.run_path(str(prepared / 'elf_metadata.py'))['identity']
geometry, candidates = {}, []
for role in ('old', 'new'):
    actual = identity(profile['binary'][role]['path'])
    saved = d['roles'][role][0]['binary_identity']
    assert actual['sha256'] == saved['sha256']
    assert actual['segments'] == saved['segments']
    assert all(actual[k] == saved[k] for k in ('inode', 'device_major', 'device_minor', 'page_bytes'))
    loads = [s for s in actual['segments'] if s['kind'] == 'LOAD']
    geometry[role] = {'actual_binary_sha256': actual['sha256'], 'source': profile['binary'][role]['source'],
                      'load_segments': loads, 'total_load_memsz_minus_filesz_bytes': sum(s['memsz']-s['filesz'] for s in loads)}
    for trial in d['roles'][role]:
        accounts = []
        for point in trial['points']:
            if not point['full']:
                continue
            a = json.loads(Path(point['account_path']).read_text())
            biases = a['candidate_load_biases']
            assert len(biases) == 1
            bias, page = biases[0], saved['page_bytes']
            intervals = [{'start': bias+s['vaddr']+s['filesz'], 'end': bias+s['vaddr']+s['memsz'],
                          'page_cover_start': (bias+s['vaddr']+s['filesz'])//page*page,
                          'whole_anon_pages_start': (bias+s['vaddr']+s['filesz']+page-1)//page*page,
                          'page_cover_end': (bias+s['vaddr']+s['memsz']+page-1)//page*page,
                          'exact_bytes': s['memsz']-s['filesz']} for s in loads if s['memsz']>s['filesz']]
            for mapping in a['mappings']:
                if mapping['category'] == 'other_anonymous_not_assumed_heap' and mapping['metrics_bytes']['Size'] == 5888*1024:
                    exact = sum(max(0,min(mapping['end'],r['end'])-max(mapping['start'],r['start'])) for r in intervals)
                    pages = sum(max(0,min(mapping['end'],r['page_cover_end'])-max(mapping['start'],r['page_cover_start'])) for r in intervals)
                    any_load = sum(max(0,min(mapping['end'],bias+s['vaddr']+s['memsz'])-max(mapping['start'],bias+s['vaddr'])) for s in loads)
                    candidates.append({'role': role, 'trial': trial['trial'], 'label': point['label'],
                                       'account_path': point['account_path'], 'load_bias': bias,
                                       'start': mapping['start'], 'end': mapping['end'], 'permissions': mapping['permissions'],
                                       'metrics_bytes': mapping['metrics_bytes'], 'main_elf_exact_zerofill_overlap_bytes': exact,
                                       'main_elf_zerofill_page_cover_overlap_bytes': pages, 'main_elf_any_load_overlap_bytes': any_load,
                                       'classification': 'anonymous; proven outside all main ELF PT_LOAD and zero-fill/page-cover intervals; allocation purpose unknown'})
                    assert exact == pages == any_load == 0
            accounts.append({'label': point['label'], 'candidate_load_bias': bias, 'main_elf_zerofill_intervals': intervals})
        geometry[role].setdefault('trials', []).append({'trial': trial['trial'], 'accounts': accounts})
assert geometry['new']['total_load_memsz_minus_filesz_bytes'] - geometry['old']['total_load_memsz_minus_filesz_bytes'] == 544
assert len(candidates) == 21 and {x['role'] for x in candidates} == {'new'}
assert all(x['metrics_bytes']['Rss'] == 5888*1024 for x in candidates)
proof = {'scope': 'saved absolute VMA ranges + actual frozen ELF program headers, no allocation inference',
         'geometry': geometry, 'candidate_5888kib_mapping_observations': candidates,
         'conclusion': '5888KiB anonymous VMA is outside all actual main ELF LOAD/zero-fill ranges in all21 observations; total ELF zero-fill delta is only544bytes, so it is not main ELF BSS zero-fill backing',
         'no_proportional_rss_split': True, 'no_product_or_cargo_execution': True}
(out / 'elf-zerofill.derived.json').write_text(json.dumps(proof, ensure_ascii=False, indent=2)+'\n')

summaries = {r: json.loads((out/r/'fanin-summary.derived.json').read_text()) for r in ('old','new')}
comparisons = {x['label']: x for x in d['comparison']}
ki = lambda v: f'{v/1024:,.0f}'
ms = lambda v: f'{v/1e6:.2f}'
report = '''# current31 配对 smaps：离线派生诊断

此报告只分析父代理已授权并实际完成的 old→current31 额外采样，不是新负载。**原 pair 进程 exit 1 保留**：old/new Docker 负载各 exit 0，但两次原 summary 均因缺少 `launcher.json` 得到 FileNotFoundError / exit 1。未在原路径伪造 launcher、未改 `pair.json`、`summary.log`、raw、manifest、maps/smaps 或原脚本。`old|new/launcher-metadata.derived.json` 明确是由实际 pair 命令、原结果、manifest、build-info、容器 ID 和已重核二进制 SHA 派生的元数据；`fanin-summary.derived.json` 复用原纯 `summarize(trial)`，没有运行原 CLI 的写入路径。

输入为 old `2eedc2b8` 与 current31 `9e3f3ed3`，不是 root39 或当前 HEAD。二进制 SHA 分别为 `026741152e55cceb6e5f96fc908f6d4585b3d6f6eef378d832c5fe573420be2f` / `b649836fa7431de38b67f980d30453303b29216e1b3c741073ed8c74e1027dbd`。两边记录的同序 17 features 一致。原命令与实际 cgroup 记录为相同 pinned Python image、network=none（仅容器 loopback）、1 CPU / 1 GiB / swap0；old 后 new，各三轮，16 源、1024B、同 domain/DNS/echo、rotation0..16 与每源31/32/33 burst。

## 原证据完整性及采样边界

[输入核验](input-verification.json) 登记全部 **381 个原输出文件 / 144,861,227 bytes** 和原 profile/脚本 digest，读取前后逐项保持。六份 trial raw 与原 manifest 完全相等；原 shutdown JSON 也与各 trial 相等；两个固定二进制仅重新读 SHA 与 ELF headers，不复制/运行/删除。

六轮各有 11 个采样事件：**48 个完整 maps/smaps 点 + 12 个存活轻量 status/rollup 点 + 6 个退出后不可用点**。48 点的 raw 长度/SHA、原 mapping-account SHA、每 VMA 原 metrics、主 ELF dev/inode、原分类汇总均复核。**其中 47 点 maps/smaps headers 一致，1 点不一致**：new trial1 `ready_before_first_apply`。原 maps 中 `0x7f94d0000000–0x7f94d8000000 ---p` 在随后 smaps 已改变边界/权限，并出现额外 guard/RW VMA。此不一致保持原样；原 `snapshot_complete` 只表示读取没有失败，不等于原子时刻一致。status / rollup / smaps 各自读取也会有 RSS 差额，不拿差额当 heap。

Popen 返回后最早点只读 status/rollup，没有 maps/smaps，不是 exec 瞬间或完整启动分配；old/new 当时处于不同 loader/启动阶段，不能从该差额定位代码原因。完整快照在已完成阶段之间同步读，增加数毫秒至更多延时和同 cgroup Python 成本；逐点耗时在 [phase 数据](memory-phases.derived.json) 中，不与原未加 smaps profile 的吞吐/丢包作无条件比较。六次真实进程退出后 `/proc` 地址空间消失，原 FileNotFoundError 与 expected-unavailable 保留，**没有补零 RSS，也没有用退出前快照冒最终释放**。

## 首个稳定业务后差异与各阶段

下表单位 KiB，分别取同阶段三轮的 **rollup RSS 中位数**，差额为两组中位数之差（不同组分的中位数不能保证可加）。原每轮值、paired delta、PSS/Anonymous/PrivateDirty、各 VMA 分类与采样时距都在 phase JSON 中。

| 实际采样点 | old RSS | current31 RSS | new − old |
| --- | ---: | ---: | ---: |
'''
for label,x in comparisons.items():
    a,b=x['medians']['old']['rollup_Rss'],x['medians']['new']['rollup_Rss']
    report += f'| {label} | {ki(a)} | {ki(b)} | {ki(b-a)} |\n'
report += '''
readiness 完整点的差额跨三轮为 −924 / +636 / +2,092 KiB，并有上述一次 headers 变动，尚不能说启动即已有同一块固定增量。**rotation0 后**三轮 rollup 差额为 +7,936 / +7,944 / +8,044 KiB，且六个点各自 maps/smaps headers 一致：这是本轮首次跨三轮都观察到的稳定业务后正差距。它处于首次 apply + 16 源 steady 完成之后，只定位到这个观测区间，不是首次实际分配的调用栈证据。

以 trial0 的 rotation0 完整 smaps 为精确可加示例：总 RSS 差 **7,936 KiB**，未标 heap 的匿名 VMA差 **6,752 KiB**，主 ELF RX **+696 KiB**、R-other **+328 KiB**、RELRO **+148 KiB**、主 ELF RW **0**、brk heap 标签 **+60 KiB**、其他验证动态 ELF **−68 KiB**、stack 标签 **+20 KiB**，其余 **0**。这描述驻留页归类，不证明某个对象/队列/owner 的分配。

三轮 current31 均在 ready→rotation0 区间出现同大小 **5888 KiB** 的匿名 RW VMA，Rss / Anonymous / PrivateDirty 均 **5888 KiB**，并持续出现在 rotation0 至 shutdown_begin 的七个完整点，共 **21** 次真实观测；old 无同大小 VMA。其余匿名增量仍存在，例如原有 2 MiB RW VMA 的 resident 变化。VMA range 新增或扩张不是净分配量证明，匿名映射不全是 heap，也不能凭大小叫 thread stack 或用户队列。

### 已核 main ELF zero-fill，不能把 5888 KiB 归 BSS

[ELF 几何核验](elf-zerofill.derived.json) 重读同 SHA 的实际二进制 PT_LOAD headers，逐项匹配容器内保存的 headers。对每个完整点，由实际 dev/inode 对应 file mappings 得到唯一 load bias，计算 `bias + p_vaddr + p_filesz .. bias + p_vaddr + p_memsz` 与页覆盖区间。5888 KiB VMA 的 **21 次观测均与 main ELF 的任何 PT_LOAD、精确 zero-fill、zero-fill 页覆盖范围交集为 0**，不是只按地址邻近或名字分类。

old 全部 PT_LOAD 的 `memsz-filesz` 合计 **35,332 bytes**（2,056 + 33,276），current31 **35,876 bytes**（2,504 + 33,372），仅多 **544 bytes**，不是 5888 KiB。故该大 VMA 可排除为 main ELF BSS zero-fill backing，仍标 **来源用途未知的匿名 VMA**。部分 ELF 尾页没有按面积猜 RSS，未将匿名页强行并回 binary；这不排除 allocator/其他实际匿名来源，现证据不能选定因果。

## 原收包、资源采样及退出

每轮17次 steady 各 **48/48**，总 **816/816**，old/new 六轮一致；实际 source peer 均16，无 apply 失败。overlap 与 burst 均有真实未观察到的 payload，以下保留原函数汇总，绝不因稳态通过吞掉缺包。单位 received/sent，括号为 not-observed。

| 版本/轮次 | overlap | burst31 | burst32 | burst33 |
| --- | ---: | ---: | ---: | ---: |
'''
for role in ('old','new'):
    for t in summaries[role]['trials']:
        o=t['overlapping_reload']
        vals=[f"{o['unique_received']}/{o['sent']} ({o['not_observed']})"]
        for name in ('31','32','33'):
            b=t['bursts'][name];vals.append(f"{b['unique_received']}/{b['sent']} ({b['not_observed']})")
        report += f"| {role}/{t['trial']} | " + ' | '.join(vals) + ' |\n'
report += '\n'
for role in ('old','new'):
    ts=summaries[role]['trials']
    report += f"{role} sampled VmHWM 为 " + ' / '.join(ki(t['kernel_vmhwm_max_observed_bytes']) for t in ts) + ' KiB；'
    report += '进程 FD peak 均29、native thread peak 均5（采样下界，不是 exact opens/任务数或 close receipt）；'
    report += '原 finish() 从 stdin-close 至采样收尾的 elapsed（含 wait、轻量采样及 sampler 收尾）为 ' + ' / '.join(ms(t['shutdown']['elapsed_ns']) for t in ts) + ' ms，三轮 graceful / exit0。\n\n'
report += '''进程 exit0 是真实进程等待结果，不能外推全部协议任务或每个底层 FD 的独立 join/native-close 证明。cgroup memory.peak 包含 Python peers/collector/JSON 等，不当作产品 PID RSS；采样没有 TaskScope task 数、user queue bytes、exact open attempts，不补造数值。old/new 同 cgroup CPU throttled periods 均7，但实际 throttled_usec 分别38,613 / 149,485；没有环境抖动/allocator trace 控制，不能据此解释全部差距或调整默认容量。

本轮可执行后续仅为基于既有 source 与这段首次业务后范围的只读调用/分配审查；新 allocator tracing、交错顺序、无 smaps 控制或新负载均需单独授权。此诊断不改变原资源验收阈值，不是性能通过/修复或 ALL17、M4、最终 HEAD/平台/下游资格。

## 再核验（只读取原文件，不运行负载）

`input-verification.json` 登记源文件绝对 artifact 路径/长度/SHA；`output-digests.json` 登记本派生目录所有其他文件。可用 Python `hashlib` 重核，或者运行本目录 `verify-derived.py`。原目录没有 launcher.json，原 summary 两次 exit1 的证据仍在；本报告的 derived 标签不替换该状态。
'''
(out / 'report.md').write_text(report)
shutil.copyfile(Path(__file__), out / 'finish-current31-smaps-report.py')
for row in verification['all_input_files']:
    content = Path(row['path']).read_bytes()
    assert len(content) == row['bytes'] and sha(content) == row['sha256']
print(json.dumps({'report': str(out/'report.md'), 'elf_zero_fill_delta_bytes': 544,
                  'candidate_vmas_verified_outside_main_elf': len(candidates), 'originals_unchanged': True},indent=2))

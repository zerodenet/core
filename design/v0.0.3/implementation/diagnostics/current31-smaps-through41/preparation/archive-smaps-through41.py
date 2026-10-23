"""Create a new independent, versionable diagnostic archive; no root writes/Cargo/product execution."""
from pathlib import Path
from collections import Counter
import gzip
import hashlib
import json
import re
import subprocess

repo=Path('/workspace/core')
raw=Path('/workspace/core-next/current31-smaps-diagnostic-run-after-authorization')
derived=Path('/workspace/core-next/current31-smaps-offline-derived')
prepared=Path('/workspace/core-next/current31-smaps-diagnostic-prepared')
artifacts=Path('/workspace/core-baseline-artifacts')
stage=Path('/workspace/core-next/smaps-through41-archive-stage')
assert not stage.exists(), 'never overwrite an existing stage'
sha=lambda b:hashlib.sha256(b).hexdigest()
inputs={}

def read(path):
    data=path.read_bytes()
    row={'bytes':len(data),'sha256':sha(data)}
    assert inputs.setdefault(str(path),row)==row,path
    return data

def load(path):
    return json.loads(read(path))

implementation=repo/'design/v0.0.3/implementation'
sentinels={name:sha(read(implementation/name)) for name in ('pr05-parallel-progress-validation.json','pr05-parallel-progress.md','stage-verification.json')}
assert sentinels['pr05-parallel-progress-validation.json']=='5b24d9d706de073bddb1006fc821b4d9fccb548da3f2196ee96855c2b030506e'
assert sentinels['pr05-parallel-progress.md']=='9496a5f73fa85dac52e12e222150b7d8ef7779c4eb42ea8a5f6ade742c1a5b43'
old=load(implementation/'pr05-parallel-progress-validation.json')
assert len(old['records'])==470
for r in old['records']:
    b=read(implementation/r['path'])
    assert sha(b)==r['gzip_sha256'] and sha(gzip.decompress(b))==r['raw_sha256']
    if r.get('diagnostic_raw_path'):
        assert read(implementation/r['diagnostic_raw_path'])==gzip.decompress(b)
originals=sorted(p for p in raw.rglob('*') if p.is_file())
derived_files=sorted(p for p in derived.rglob('*') if p.is_file())
prepared_files=sorted(p for p in prepared.rglob('*') if p.is_file() and '__pycache__' not in p.parts)
assert len(originals)==381 and sum(len(read(p)) for p in originals)==144861227
assert len(derived_files)==12 and len(prepared_files)==22
for item in load(derived/'output-digests.json')['files']:
    b=read(derived/item['path'])
    assert (len(b),sha(b))==(item['bytes'],item['sha256'])
for item in load(derived/'input-verification.json')['all_input_files']:
    b=read(Path(item['path']))
    assert (len(b),sha(b))==(item['bytes'],item['sha256'])
for item in load(prepared/'preparation-files.json')['files']:
    b=read(prepared/item['path'])
    assert (len(b),sha(b))==(item['bytes'],item['sha256'])
pair=load(raw/'pair.json');profile=load(prepared/'profile.json')
assert sha(read(prepared/'profile.json'))==pair['profile_sha256']
for name,expected in pair['scripts_sha256'].items():
    assert sha(read(prepared/name))==expected
for role in ('old','new'):
    assert pair['results'][role]['exit_code']==0 and pair['results'][role]['summary_exit_code']==1
    assert not (raw/role/'launcher.json').exists()
    error=read(raw/role/'summary.log').decode()
    assert 'FileNotFoundError' in error and str(raw/role/'launcher.json') in error
rounds={str(n):load(artifacts/f'pr05-parallel-focused-{n}.json') for n in (40,41)}
expected_heads={'40':'670be674ac64d758ca14e8c04703448cc3173ce2','41':'7e1b9986bec750905350472f3392e49efdce894b'}
parsed=[]
for n,item in rounds.items():
    assert item['head']==expected_heads[n]
    tree=subprocess.check_output(['git','-C',str(repo),'rev-parse',item['head']+'^{tree}']).decode().strip()
    assert tree==item['tree']
    assert item['lock_sha256']=='ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764'
    for c in item['commands']:
        assert all(c[k] for k in ('head_unchanged','source_clean','coverage_ok','lock_unchanged'))
        log=read(Path(c['log'])).decode()
        counts=re.findall(r'^test result: .*? (\d+) passed; (\d+) failed; (\d+) ignored;',log,re.M)
        if c['counts'] is not None:
            assert len(counts)==1 and list(map(int,counts[0]))==c['counts']
            rows=re.findall(r'^test (\S+)(?: - should panic)? \.\.\. (ok|FAILED|ignored(?:,.*)?)$',log,re.M)
            assert len(rows)==c['selected_test_count']==sum(c['counts'])
            assert [sum(r=='ok' for _,r in rows),sum(r=='FAILED' for _,r in rows),sum(r.startswith('ignored') for _,r in rows)]==c['counts']
        else:
            assert not counts
        parsed.append({'round':n,'name':c['name'],'exit':c['exit'],'counts':c['counts'],'parsed_matches_manifest':True})
assert len(rounds['40']['commands'])==2
assert rounds['40']['commands'][1]['exit']==101
compile40=read(artifacts/'pr05-parallel-focused-40-workspace-alltargets-check.txt').decode()
assert compile40.count('error[E0624]: method `notify_hosted` is private')==3
assert 'join.rs:179:20' in compile40 and 'join.rs:188:16' in compile40 and 'scope.rs:263:16' in compile40
cmd41={c['name']:c for c in rounds['41']['commands']}
assert len(cmd41)==11 and cmd41['strict']['exit']==101
expected_counts={'platform-unit':[124,0,1],'platform-contracts-all':[82,0,0],'transport-library':[195,0,1],
                 'proxy-library':[652,0,2],'source-runtime-guard':[189,0,0],'source-architecture-guard':[26,0,0]}
for name,counts in expected_counts.items():
    assert cmd41[name]['exit']==0 and cmd41[name]['counts']==counts
strict=read(artifacts/'pr05-parallel-focused-41-strict.txt').decode()
assert 'manual implementation of a no-op waker' in strict and 'hosted/tests/wake.rs:86:6' in strict
assert strict.count('error: casting to the same type is unnecessary')==2 and 'tests/file/owner.rs:105:' in strict
assert 'zero-platform-tokio` (lib test) due to 3 previous errors' in strict
packet=load(artifacts/'pr05-parallel-41-packet-noprobe.json')
assert packet['head']==rounds['41']['head'] and packet['tree']==rounds['41']['tree'] and packet['exit']==0
trace=read(artifacts/'pr05-parallel-41-packet-noprobe.strace').decode()
assert trace.count('socket(')==1 and trace.count('bind(')==1 and 'connect(' not in trace
assert 'AF_INET, SOCK_DGRAM|SOCK_CLOEXEC, IPPROTO_UDP) = 9' in trace
assert 'close(9)' in trace
packetlog=read(artifacts/'pr05-parallel-41-packet-noprobe.txt').decode()
assert '1 passed; 0 failed; 0 ignored; 0 measured; 124 filtered out' in packetlog
binaries=[]
for name,path,expected in [(r,Path(profile['binary'][r]['path']),profile['binary'][r]['sha256']) for r in ('old','new')]+[('root41-single-packet',Path(packet['binary']),packet['binary_sha256'])]:
    before=path.stat()
    with path.open('rb') as stream: digest=hashlib.file_digest(stream,'sha256').hexdigest()
    after=path.stat()
    assert (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns)
    assert digest==expected
    binaries.append({'role':name,'path':str(path),'bytes':after.st_size,'sha256':digest,'archived':False,'executed_by_this_preparation':False})

stage.mkdir()
records=[]

def archive_bytes(relative,data,category,source_location=None,raw_copy=False):
    assert data[:4]!=b'\x7fELF',relative
    target=Path('evidence')/(str(relative)+'.gz')
    assert not (stage/target).exists()
    compressed=gzip.compress(data,mtime=0)
    (stage/target).parent.mkdir(parents=True,exist_ok=True)
    (stage/target).write_bytes(compressed)
    row={'path':str(target),'category':category,'raw_bytes':len(data),'raw_sha256':sha(data),
         'gzip_bytes':len(compressed),'gzip_sha256':sha(compressed),'diagnostic_only':True}
    if source_location:row['source_location']=source_location
    if raw_copy:
        (stage/relative).parent.mkdir(parents=True,exist_ok=True)
        (stage/relative).write_bytes(data)
        row['raw_copy']=str(relative)
    records.append(row)

for p in originals:
    archive_bytes(Path('smaps-raw')/p.relative_to(raw),read(p),'smaps-original-raw-diagnostic-only',str(p))
for p in derived_files:
    archive_bytes(Path('derived')/p.relative_to(derived),read(p),'smaps-derived-diagnostic-only',str(p),True)
for p in prepared_files:
    archive_bytes(Path('prepared')/p.relative_to(prepared),read(p),'prepared-historical-input-not-execution',str(p),True)
for n in (40,41):
    paths=sorted(artifacts.glob(f'pr05-parallel-focused-{n}*'))
    assert len(paths)==(3 if n==40 else 12)
    for p in paths:
        archive_bytes(Path('frozen')/f'root{n}'/p.name,read(p),'frozen-command-evidence-not-whole-head',str(p),True)
for p in sorted(artifacts.glob('pr05-parallel-41-packet-noprobe.*')):
    archive_bytes(Path('packet-single')/p.name,read(p),'single-packet-syscall-diagnostic-only',str(p),True)
metadata={'scope':'identity only; original binaries retained; no ELF copied/executed', 'binaries':binaries,
          'root41_packet_scope':'one explicit unsupported-probe case, one declared UDP listener socket/bind/close; no additional socket/connect in trace; not a benchmark or whole-head result'}
archive_bytes(Path('metadata/binary-identities.json'),(json.dumps(metadata,ensure_ascii=False,indent=2)+'\n').encode(),'derived-binary-identity-not-execution',raw_copy=True)
for name in ('archive-smaps-through41.py','verify-smaps-through41-archive.py','copy-smaps-through41-archive.py'):
    p=Path('/workspace/core-next')/name
    archive_bytes(Path('preparation')/name,read(p),'archive-tooling-no-product-execution',str(p),True)
readme='''# 独立 smaps / root40–41 证据归档

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
'''
archive_bytes(Path('README.md'),readme.encode(),'archive-derived-scope-not-qualification',raw_copy=True)
assert len(records)==438,len(records)
categories=dict(sorted(Counter(r['category'] for r in records).items()))
manifest={'scope':'independent diagnostic archive, not new runtime qualification','format_version':1,
          'overall_qualification':False,'independent_of_through39_records':True,
          'through39_unchanged_sentinels':sentinels,'rounds':rounds,'parsed_frozen_log_checks':parsed,
          'original_smaps_files':381,'original_smaps_bytes':144861227,'derived_files_including_output_index':12,
          'prepared_files_without_pyc':22,'binary_identities':binaries,'packet_single':packet,
          'category_counts':categories,'records':sorted(records,key=lambda r:r['path'])}
(stage/'archive-manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
for row in records:
    zipped=(stage/row['path']).read_bytes();data=gzip.decompress(zipped)
    assert (len(zipped),sha(zipped),len(data),sha(data))==(row['gzip_bytes'],row['gzip_sha256'],row['raw_bytes'],row['raw_sha256'])
    if row.get('raw_copy'):assert (stage/row['raw_copy']).read_bytes()==data
for path,expected in inputs.items():
    b=Path(path).read_bytes();assert len(b)==expected['bytes'] and sha(b)==expected['sha256'],path
for binary in binaries:
    with Path(binary['path']).open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==binary['sha256']
verify={'scope':'archive preparation only; no new Cargo, product, Docker, workload or root writes',
        'records':438,'all_gzip_raw_verified':True,'all_original_inputs_and_through39_unchanged':True,
        'manifest_sha256':sha((stage/'archive-manifest.json').read_bytes()),
        'gzip_bytes':sum(r['gzip_bytes'] for r in records),'raw_bytes':sum(r['raw_bytes'] for r in records),
        'nonrecord_files':{},'overall_qualification':False}
(stage/'archive-verification.json').write_text(json.dumps(verify,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'stage':str(stage),'category_counts':categories,**verify},indent=2))

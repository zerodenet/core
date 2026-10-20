"""Copy fixed evidence through root38. Never run Cargo/product/test binaries."""
from pathlib import Path
import gzip
import hashlib
import json
import re
import subprocess

repo = Path('/workspace/core')
raw = Path('/workspace/core-baseline-artifacts')
base = repo / 'design/v0.0.3/implementation'
stage = Path('/workspace/core-next/progress-through38')
assert not stage.exists(), 'never overwrite an existing evidence stage'
sha = lambda content: hashlib.sha256(content).hexdigest()
inputs = {}


def read(path):
    content = path.read_bytes()
    previous = inputs.setdefault(str(path), sha(content))
    assert previous == sha(content), f'input changed: {path}'
    return content


def git_bytes(head, path):
    return subprocess.check_output(['git', '-C', str(repo), 'show', f'{head}:{path}'])


validation_path = base / 'pr05-parallel-progress-validation.json'
progress_path = base / 'pr05-parallel-progress.md'
original_validation = read(validation_path)
original_progress = read(progress_path)
d = json.loads(original_validation)
old_records = list(d['records'])
assert len(old_records) == 390
assert d['latest_runtime_code_commit'] == 'f96b7600162c97eda4e97503bfc33ff8925cfd42'
rounds = {str(n): json.loads(read(raw / f'pr05-parallel-focused-{n}.json')) for n in range(35, 39)}
expected_heads = {
    '35': '68c166321194a20921ced96a8bdd1d22be97099b',
    '36': 'eb7371f50ac944b0acb82e94085c3407f1c8b3d6',
    '37': '0ee7c39d971b6656c837c68222cf2213ac210a50',
    '38': 'dafcf04f83a1007b075832f27ae56ddc7559c61e',
}
for number, manifest in rounds.items():
    assert manifest['head'] == expected_heads[number]
    assert manifest['lock_sha256'] == 'ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764'
    for command in manifest['commands']:
        assert command['head_unchanged'] and command['source_clean'] and command['lock_unchanged']
        log = read(Path(command['log'])).decode()
        if command['counts'] is not None:
            rows = re.findall(r'^test result: .*? (\d+) passed; (\d+) failed; (\d+) ignored;', log, re.M)
            assert tuple(map(int, rows[-1])) == tuple(command['counts'])
        if number == '37' and command['name'] == 'transport-library':
            assert command['counts'] is None and not command['coverage_ok'] and command['exit'] == 101
            assert 'signal: 15, SIGTERM' in log and not re.search(r'^test result:', log, re.M)
        else:
            assert command['coverage_ok']

commands = {n: {c['name']: c for c in m['commands']} for n, m in rounds.items()}
assert commands['35']['component-unit']['counts'] == [128, 0, 0]
assert commands['35']['source-runtime-guard']['counts'] == [188, 1, 0]
assert len(rounds['36']['commands']) == 2 and commands['36']['workspace-alltargets-check']['exit'] == 101
assert commands['37']['workspace-alltargets-check']['exit'] == 0
for name, counts in {'component-unit': [128, 0, 0], 'vmess-library': [25, 0, 0],
                     'source-runtime-guard': [189, 0, 0], 'source-architecture-guard': [26, 0, 0]}.items():
    assert commands['37'][name]['counts'] == counts
assert commands['38']['transport-library']['counts'] == [194, 1, 1]
assert len(rounds['38']['commands']) == 3 and commands['38']['workspace-alltargets-check']['exit'] == 101
compile38 = read(Path(commands['38']['workspace-alltargets-check']['log'])).decode()
assert 'could not compile `zero-proxy` (lib) due to 4 previous errors' in compile38
assert 'could not compile `zero-proxy` (lib test) due to 10 previous errors' in compile38

transport_logs = {n: read(Path(commands[n]['transport-library']['log'])).decode() for n in ('37', '38')}
subsets = {}
for n, text in transport_logs.items():
    rows = re.findall(r'^test (\S+) \.\.\. (ok|FAILED|ignored)$', text, re.M)
    h2 = [name for name, result in rows if name.startswith('split_http::managed_h2::') and result == 'ok']
    single = [name for name, result in rows if name.startswith('grpc::tests::single::') and result == 'ok']
    assert len(h2) == (6 if n == '37' else 12) and len(single) == 2
    subsets[n] = {'managed_h2_individual_ok': h2, 'grpc_single_individual_ok': single,
                  'not_a_whole_suite_pass': True}
hang_case = 'grpc::owned::tests::logical::actual_remote_h2_stream_limit_bounds_pending_demands_and_cancellation_preserves_shared_carrier'
cleanup_case = 'grpc::response::tests::dropping_head_after_real_pending_response_registration_reclaims_it_before_connection_cleanup'
assert f'test {hang_case} ... ok' in transport_logs['38']
assert f'test {cleanup_case} ... FAILED' in transport_logs['38'] and 'BrokenPipe' in transport_logs['38']

diagnostic_sources = sorted(raw.glob('pr05-parallel-37-*'))
assert len(diagnostic_sources) == 30 and all(p.is_file() for p in diagnostic_sources)
diagnostic_json = {p.name: json.loads(read(p)) for p in diagnostic_sources if p.suffix == '.json'}
expected_binaries = {}
for name, item in diagnostic_json.items():
    if 'binary_sha256' in item:
        path = item.get('binary') or item['argv'][0]
        entry = expected_binaries.setdefault(path, {'expected_sha256': item['binary_sha256'], 'referenced_by': []})
        assert entry['expected_sha256'] == item['binary_sha256']
        entry['referenced_by'].append(name)
assert len(expected_binaries) == 5
assert set(expected_binaries) == {str(p) for p in raw.glob('zero-transport-root37-*')}


def binary_identity(path, expected):
    before = path.stat()
    with path.open('rb') as stream:
        actual = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    assert actual == expected
    return {'path': str(path), 'sha256': actual, 'bytes': after.st_size,
            'mtime_ns': after.st_mtime_ns, 'archived': False, 'executed_by_this_preparation': False}


binary_checks = []
for path, item in sorted(expected_binaries.items()):
    row = binary_identity(Path(path), item['expected_sha256'])
    row['referenced_by'] = sorted(item['referenced_by'])
    binary_checks.append(row)
prior_binary_checks = [binary_identity(Path(item['path']), item['sha256'])
                       for item in d['current31_memory_diagnostic']['fixed_binary_identity_checks']]

restored = diagnostic_json['pr05-parallel-37-diagnostic-restore.json']
assert restored['all_originals_match_current_git_head'] and len(restored['rows']) == 3
restore_artifacts = {
    'crates/transport/src/grpc/owned/lifecycle.rs': ('grpc-lifecycle-original', 'grpc-lifecycle-instrumented'),
    'crates/transport/tests/grpc/owned/logical.rs': ('grpc-logical-original', 'grpc-shutdown-branches-instrumented'),
    'crates/platform/tokio/src/contract/scope.rs': ('scope-original', 'scope-instrumented'),
}
restore_checks = []
for row in restored['rows']:
    original, instrumented = restore_artifacts[row['path']]
    original_path = raw / f'pr05-parallel-37-{original}.rs'
    instrumented_path = raw / f'pr05-parallel-37-{instrumented}.rs'
    assert sha(read(original_path)) == row['restored_sha256']
    assert sha(read(instrumented_path)) == row['diagnostic_sha256']
    commits = {}
    for head in ('0ee7c39d971b6656c837c68222cf2213ac210a50', '9d08459f5879740890c04742b7b2e92e425829d3'):
        commits[head] = sha(git_bytes(head, row['path']))
        assert commits[head] == row['restored_sha256']
    restore_checks.append(dict(row, original_artifact=original_path.name,
                               last_instrumented_artifact=instrumented_path.name,
                               fixed_git_sources_verified=commits))
for metadata, original, instrumented in (
    ('grpc-logical-instrumentation', 'grpc-logical-original', 'grpc-logical-instrumented'),
    ('grpc-lifecycle-instrumentation', 'grpc-lifecycle-original', 'grpc-lifecycle-instrumented'),
    ('scope-instrumentation', 'scope-original', 'scope-instrumented'),
):
    item = diagnostic_json[f'pr05-parallel-37-{metadata}.json']
    assert sha(read(raw / f'pr05-parallel-37-{original}.rs')) == item['original_sha256']
    assert sha(read(raw / f'pr05-parallel-37-{instrumented}.rs')) == item['instrumented_sha256']
logical_path = 'crates/transport/tests/grpc/owned/logical.rs'
logical = read(raw / 'pr05-parallel-37-grpc-logical-original.rs')
assert b'Duration::from_secs(7)' in logical and logical == git_bytes(expected_heads['38'], logical_path)

# Everything above is read-only preflight. Writes begin only in a new stage.
(stage / 'evidence/parallel-progress').mkdir(parents=True)
known = {r['source'] for r in old_records}
for record in old_records:
    compressed = read(base / record['path'])
    content = gzip.decompress(compressed)
    assert (len(compressed), sha(compressed), len(content), sha(content)) == (
        record['gzip_bytes'], record['gzip_sha256'], record['raw_bytes'], record['raw_sha256'])
    dest = stage / record['path']
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(compressed)
    if 'diagnostic_raw_path' in record:
        original = read(base / record['diagnostic_raw_path'])
        assert original == content
        target = stage / record['diagnostic_raw_path']
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(original)


def archive_bytes(name, content, kind, raw_path=None, source_location=None):
    assert name not in known, name
    compressed = gzip.compress(content, mtime=0)
    relative = Path('evidence/parallel-progress') / (name + '.gz')
    (stage / relative).write_bytes(compressed)
    record = dict(source=name, path=str(relative), kind=kind,
                  raw_bytes=len(content), raw_sha256=sha(content),
                  gzip_bytes=len(compressed), gzip_sha256=sha(compressed))
    if source_location:
        record['source_location'] = source_location
    if raw_path:
        target = stage / raw_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        record['diagnostic_raw_path'] = str(raw_path)
    d['records'].append(record)
    known.add(name)
    return record


runtime_records = []
for number in range(35, 39):
    for source in sorted(raw.glob(f'pr05-parallel-focused-{number}*')):
        if source.is_file():
            runtime_records.append(archive_bytes(source.name, read(source), 'frozen-command-evidence', source_location=str(source)))
assert len(runtime_records) == 23
diagnostic_records = []
for source in diagnostic_sources:
    diagnostic_records.append(archive_bytes(source.name, read(source), 'diagnostic-only-not-qualification',
        Path('diagnostics/root37') / source.name, str(source)))
assert len([r for r in diagnostic_records if r['source'].endswith('.rs')]) == 7
traces = [r for r in diagnostic_records if re.search(r'-strace\.\d+$', r['source'])]
assert len(traces) == 2

metadata = {'scope': 'identity checks only; no binaries copied/executed; originals retained',
            'root37_fixed_binaries': binary_checks, 'prior_current31_binaries_rechecked': prior_binary_checks}
archive_bytes('root37-binary-identity-verification.json', (json.dumps(metadata, ensure_ascii=False, indent=2)+'\n').encode(),
              'derived-identity-verification', Path('diagnostics/root37/binary-identities.json'))
restore_metadata = {'scope': 'historical exact restore verified against saved original/instrumented bytes and fixed git objects, not a claim about later mutable HEAD',
                    'restored_three_files': restore_checks, 'root38_original_logical_test_byte_identical': True,
                    'original_inner_timeout_seconds': 7}
archive_bytes('root37-restore-source-verification.json', (json.dumps(restore_metadata, ensure_ascii=False, indent=2)+'\n').encode(),
              'derived-source-verification', Path('diagnostics/root37/restore-verification.json'))

# Versioned snapshots support the source-lock diagnosis and its exact successor.
snapshots = [
    ('root37-head-original.rs', expected_heads['37'], 'crates/transport/src/grpc/response.rs'),
    ('root38-head-wake-relay.rs', expected_heads['38'], 'crates/transport/src/grpc/response.rs'),
    ('root38-head-regression-fixture.rs', expected_heads['38'], 'crates/transport/tests/grpc/response.rs'),
]
for name, head, path in snapshots:
    archive_bytes(name, git_bytes(head, path), 'fixed-source-snapshot-not-runtime',
                  Path('diagnostics/root37') / name, f'git:{head}:{path}')

wg_source = Path('/workspace/core-next/wireguard-stopold-recovery-review.md')
wg_raw = read(wg_source)
wg_original = archive_bytes(wg_source.name, wg_raw, 'read-only-audit-original-not-implementation', source_location=str(wg_source))
portable_links = []


def portable_link(match):
    label, relative, line = match.group(1), match.group(2), match.group(3)
    assert (repo / relative).is_file(), relative
    target = '../../../../' + relative
    if line:
        target += '#L' + line
    portable_links.append({'source_path': relative, 'original_audit_line': int(line) if line else None, 'portable_target': target})
    return f'[{label}]({target})' + (f'（审计时第 {line} 行）' if line else '')


portable = re.sub(r'\[([^\]]+)\]\(/workspace/core/([^\s)]+?)(?::(\d+))?\)', portable_link, wg_raw.decode())
assert portable_links and not re.search(r'\]\(/workspace/', portable)
portable = ('> 可移植诊断副本；源码链接改为仓库相对路径并标注原审计行号，内容结论未改。'
            '本文件与[原字节 gzip](../evidence/parallel-progress/wireguard-stopold-recovery-review.md.gz)不同；'
            '原文 raw/gzip SHA 见证据清单。本文仅只读方案，不代表已实现或通过运行验证。\n\n' + portable)
wg_portable = archive_bytes('wireguard-stopold-recovery-review.portable.md', portable.encode(),
                           'portable-derived-audit-not-implementation', Path('diagnostics/wireguard-stopold-recovery-review.md'))
wg_portable['derived_from_raw_sha256'] = sha(wg_raw)
wg_portable['portable_source_links_verified'] = portable_links
archive_bytes('prepare-progress-through38.py', read(Path(__file__)), 'evidence-preparation-script-no-product-execution',
              Path('diagnostics/preparation/prepare-progress-through38.py'), str(Path(__file__)))

latest = rounds['38']
d['latest_runtime_code_commit'] = latest['head']
d['latest_runtime_tree'] = latest['tree']
d['latest_runtime_lock_sha256'] = latest['lock_sha256']
d['latest_alltargets_pass_commit'] = rounds['37']['head']
d['rounds35_38'] = rounds
d['root35'] = '68c16632: fmt/alltargets/layout PASS; component128/0, runtime guard188/1, architecture26/0. cold test fixture concrete-gate violation fixed in c1a2ced0; root37 runtime189/0 is successor evidence, not a rewrite.'
d['root36'] = 'eb7371f5: fmt PASS, alltargets compile blocked by four ambiguous fixture methods; no runtime command executed.'
d['root37'] = '0ee7c39d: fmt/alltargets/layout PASS; whole transport hung and SIGTERM, counts=null/coverage_ok=false; individual managedH2 six and grpc single two ok cannot qualify the whole library. VMess25/0, component128/0, runtime189/0, architecture26/0.'
d['root38'] = 'dafcf04f: fmt PASS; transport194/1/1 FAIL (original logical hang regression now ok, new Head cleanup fixture BrokenPipe failure retained); managedH2 twelve individual cases ok; alltargets FAIL zero-proxy4lib+10libtest. Other intended scopes not run.'
d['post38_status'] = 'Stops at root38 partial failures. Root4229afb7 compile repair and subsequent response-fixture repair/root39 are not qualified by this stage. No full ALL17/RuntimeHost M4/shared-resource/platform/final-head or downstream qualification.'
d['root37_hang_diagnostic'] = {
    'scope': 'historical diagnosis only; temporary instrumentation not production qualification',
    'capture': 'diagnostics/root37/pr05-parallel-37-transport-hang.json',
    'binary_metadata': 'diagnostics/root37/binary-identities.json',
    'source_restore': 'diagnostics/root37/pr05-parallel-37-diagnostic-restore.json',
    'source_restore_verification': 'diagnostics/root37/restore-verification.json',
    'original_case': hang_case, 'original_inner_timeout_seconds': 7,
    'original_binary_repro_exit': -15, 'instrumented_runs_not_qualification': True,
    'strace': {'per_pid_traces': [r['diagnostic_raw_path'] for r in traces],
               'single_case_observed_pass_under_strace': True, 'does_not_negate_original_hang': True},
    'source_diagnosis': 'Head Shared strongly owned its registered waker and Pending ResponseFuture. H2 can drop that waker while holding streams.inner; last Shared drop re-enters drop_stream_ref on the same mutex. WakeRelay is separated from Shared to break this ownership/re-entry path.',
    'integrated_fix_commit': latest['head'],
    'equivalent_worker_fix_commit': '687da883ace5a3307a40712a43daf11146274f87',
    'root38_original_case_pass': True, 'root38_new_cleanup_fixture_pass': False,
    'no_proc_stack_or_gdb_claim': True,
}
d['individual_transport_observations37_38'] = subsets
d['wireguard_stopold_recovery_review'] = {
    'scope': 'read-only design audit, not implementation, activation or qualification',
    'implemented': False, 'runtime_validated': False,
    'original_raw_gzip': wg_original['path'], 'original_raw_sha256': wg_original['raw_sha256'],
    'portable_copy': wg_portable['diagnostic_raw_path'], 'portable_raw_sha256': wg_portable['raw_sha256'],
    'portable_source_link_count': len(portable_links),
    'gap': 'same actual WG security/history object may be retained across new FDs; opaque recovery capsule, actual native/control obligations, join/close, candidate cleanup, last-good recovery, ancillary and cold/staged device wiring remain unimplemented.'}
d['records'].sort(key=lambda record: record['source'])
(stage / 'pr05-parallel-progress-validation.json').write_text(json.dumps(d, ensure_ascii=False, indent=2)+'\n')

appendix = '''
## root35–38 后继记录与真实挂起诊断（保留此前截止/失败）

本节把归档推进到 **root38 `dafcf04f83a1007b075832f27ae56ddc7559c61e`**，tree `821c5a8b4ec96f918126a5935c4506efd48b104e`，lock SHA `ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764`。此前“latest/待下一冻结”等属于原历史段落，旧 fields/失败不删除。root38 自身仍失败；后继 `4229afb7` 类型修复与准备中的 root39 不在此运行资格内，**ALL17、RuntimeHost M4、完整资源替换/平台/最终 head/下游资格仍未完成**。

| 冻结 | 实际范围与结果 | 限制 |
| --- | --- | --- |
| root35 `68c16632` | fmt、all-features/all-targets、layout 通过；component **128/0**；runtime guard **188/1**；architecture **26/0**。 | 首次失败是 `component/catalog/cold.rs` 测试的具体协议 gate 越过通用边界。后继 `c1a2ced0` 将真实 fixture 移入测试边界，root37 的 **189/0** 是后继证据，不覆盖 root35 原失败。 |
| root36 `eb7371f5` | fmt 通过；all-targets 在四处 gRPC fixture 方法歧义处编译阻塞。 | 没有运行 runtime 测试；不能把仅 managedH2 类型无错误称为整体通过。 |
| root37 `0ee7c39d` | fmt、workspace all-targets、layout 通过；VMess **25/0**，component **128/0**，runtime/architecture guards **189/0 + 26/0**。whole transport 原 logical remote-H2 case 挂起，实际 SIGTERM，manifest **counts=null / coverage_ok=false**。 | 日志中 managedH2 **6 个**与原 single **2 个**单项 ok，仅说明这些个体已执行，不构成 whole transport 或可相加的 aggregate counts。 |
| root38 `dafcf04f` | fmt 通过；transport **194/1/1**（passed/failed/ignored），原挂起 logical case 已实际 ok，managedH2/source **12 个**个体 ok。新 Head cleanup fixture 在 `response.rs:132` 的原 unwrap 上得到 **BrokenPipe**，该失败保留。workspace all-targets **4 lib + 10 lib-test 编译失败**。 | transport 与 all-targets 均未通过；其余计划范围未运行，没有新 strict/workspace 全量/平台结论。忽略项不计运行通过，子范围不相加成全库资格。 |

### root37 原挂起、源码自锁与修复证据

原 whole-suite 进程在七秒内部 timeout 之外挂住。[原进程现场](diagnostics/root37/pr05-parallel-37-transport-hang.json) 保留 PID/thread/FD/proc 可读取内容，也保留 stack/syscall 权限拒绝；没有伪造 stack、gdb 或 FD 读数。原 immutable binary 的独立重现再次外部 SIGTERM；五个原/诊断二进制均按各运行 JSON 的 SHA 逐项读验，保存在[身份清单](diagnostics/root37/binary-identities.json)，**只保元数据与原绝对 artifact 路径，不把二进制入库、不执行或删除原文件**。

全部 instrumentation JSON、构建日志、各原/阶段 `.rs`、原运行/超时 JSON/log、每 PID strace 原 trace 均归档在 `diagnostics/root37/` 并有对应确定性 gzip。strace 观察的一次原 binary 单项实际 **1/0、0.10s**，不是超时；该反例原样保留，不能改写成“始终必现”，也不能据此否定既有 whole-suite hang 或宣布修复。临时 instrumentation 只用于诊断，不作为生产资格。

源码调查定位 `Head → Shared → 强 registered waker/Pending ResponseFuture` 保活与重入：h2 持 `streams.inner` 时释放该 waker，最后 Shared 销毁 PendingResponseFuture，再进 `drop_stream_ref` 请求同锁。根 **`dafcf04f`**（对应独立 worker `687da883`）把 WakeRelay 与 Shared 所有权分开。[原 Head](diagnostics/root37/root37-head-original.rs)、[固定 root38 Head](diagnostics/root37/root38-head-wake-relay.rs) 与[新回归源码](diagnostics/root37/root38-head-regression-fixture.rs) 是固定 Git 对象的副本；不是据 proc 权限不足的现场捏造的调用栈。

[三文件原 restore JSON](diagnostics/root37/pr05-parallel-37-diagnostic-restore.json) 原字节保留；[独立核验](diagnostics/root37/restore-verification.json) 对 lifecycle/logical/TaskScope 的原与最后诊断版本 SHA、固定 `0ee7c39d`/`9d08459f` Git 对象逐项匹配。原 logical 测试在 root38 仍 byte-identical，**七秒 timeout、wire 和断言未放宽**；修复后该原 case 实际通过。新 cleanup fixture 的 BrokenPipe 失败仍是 root38 未通过事项，后继修复待新冻结验证。

### WireGuard StopOld 恢复审计尚未实施

[可移植 WG 审计副本](diagnostics/wireguard-stopold-recovery-review.md) 只分析 same actual security/history state 跨新 FD 恢复、StopOld/候选清理/last-good/capsule/native-control/ancillary 的真实缺口。源码链接已改仓库相对路径并标原审计行号；[原文 gzip](evidence/parallel-progress/wireguard-stopold-recovery-review.md.gz) 保持原字节，raw/gzip SHA 与 portable 派生 SHA 分别登记，两者明确不同。该报告不是实现、生产启用或回归通过；正常 legacy WG 空 resource 迭代可能误确认 physical complete 等缺口仍保留，不能以“同 crypto 对象可留存”推导 FD 已关闭。

本更新只复制原证据、读取固定 Git 源码和核验 digest。全部旧 gzip 逐字节保留，新 gzip 确定性生成；逐项解压核 raw/gzip 长度与 SHA，所有原日志/失败/取消/未运行状态保留。没有 Cargo、产品/测试 binary、诊断负载、新源码实现、commit、push 或 Mac 操作。
'''
(stage / 'pr05-parallel-progress.md').write_text(original_progress.decode()+appendix)

verified = []
for record in d['records']:
    compressed = (stage / record['path']).read_bytes()
    content = gzip.decompress(compressed)
    assert (len(compressed), sha(compressed), len(content), sha(content)) == (
        record['gzip_bytes'], record['gzip_sha256'], record['raw_bytes'], record['raw_sha256'])
    if 'diagnostic_raw_path' in record:
        assert (stage / record['diagnostic_raw_path']).read_bytes() == content
    original = raw / record['source']
    if original.is_file():
        assert read(original) == content
    verified.append({'source': record['source'], 'gzip_sha256': record['gzip_sha256'],
                     'raw_sha256': record['raw_sha256'], 'gzip_and_raw_verified': True})
for record in old_records:
    assert (stage / record['path']).read_bytes() == read(base / record['path'])
for path, expected in inputs.items():
    assert sha(Path(path).read_bytes()) == expected, f'original input changed: {path}'
for row in binary_checks + prior_binary_checks:
    binary_identity(Path(row['path']), row['sha256'])
assert validation_path.read_bytes() == original_validation and progress_path.read_bytes() == original_progress
for path in stage.rglob('*'):
    if path.is_file():
        with path.open('rb') as stream:
            assert stream.read(4) != b'\x7fELF', f'no ELF archive allowed: {path}'
summary = {'stage': str(stage), 'scope': 'through38 partial evidence only; ALL17 and overall qualification incomplete',
           'old_records': len(old_records), 'new_runtime_records': len(runtime_records),
           'root37_original_diagnostic_records': len(diagnostic_records), 'root37_source_rs_records': 7,
           'root37_per_pid_trace_records': len(traces), 'root37_fixed_binary_identity_checks': len(binary_checks),
           'prior_current31_binary_identity_rechecks': len(prior_binary_checks), 'total_records': len(d['records']),
           'all_old_and_new_gzip_and_raw_verified': True, 'all_source_inputs_and_binaries_unchanged': True,
           'root_report_files_unchanged': True, 'product_binary_archived_or_executed': False,
           'cargo_or_workload_run': False, 'commit_or_push': False,
           'latest_runtime_code_commit': latest['head'], 'overall_passed': False,
           'manifest_sha256': sha((stage/'pr05-parallel-progress-validation.json').read_bytes()),
           'progress_sha256': sha((stage/'pr05-parallel-progress.md').read_bytes())}
(stage / 'stage-verification.json').write_text(json.dumps({'summary': summary, 'verified': verified}, ensure_ascii=False, indent=2)+'\n')
print(json.dumps(summary, ensure_ascii=False, indent=2))

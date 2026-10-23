"""Stage immutable root39 evidence on the unchanged 451-record through38 stage. No Cargo/product execution."""
from pathlib import Path
import gzip
import hashlib
import json
import re
import subprocess

repo = Path('/workspace/core')
raw = Path('/workspace/core-baseline-artifacts')
base = Path('/workspace/core-next/progress-through38')
stage = Path('/workspace/core-next/progress-through39')
assert not stage.exists(), 'never overwrite evidence stages'
sha = lambda content: hashlib.sha256(content).hexdigest()
inputs = {}


def read(path):
    content = path.read_bytes()
    assert inputs.setdefault(str(path), sha(content)) == sha(content), path
    return content


old_manifest = read(base / 'pr05-parallel-progress-validation.json')
old_progress = read(base / 'pr05-parallel-progress.md')
old_verification = read(base / 'stage-verification.json')
assert sha(old_manifest) == 'c9e85021a9f7269d80ad724c55a06b489cf17fbc6ee1801c6c04c56d151d0570'
assert sha(old_progress) == '6d421cf13947e3430dd9c83acbd74712c61d63058effb24a45f5f02660e13b64'
assert sha(old_verification) == 'ef8caeb5f3dcc3936ecb8b0e2ab1aa72ca93e1f304b729bdd5fb25906bd3d870'
d = json.loads(old_manifest)
old_records = list(d['records'])
assert len(old_records) == 451
# Read-only root report sentinels: root has not installed the through38 stage.
root_reports = {name: read(repo / 'design/v0.0.3/implementation' / name) for name in (
    'pr05-parallel-progress-validation.json', 'pr05-parallel-progress.md')}
manifest = json.loads(read(raw / 'pr05-parallel-focused-39.json'))
head = '7a2670a4ac50e92e37758806df6d40552fc7509d'
tree = 'de87a8e6cf751b31e6cbd4b94cfee2c51e9aa1d1'
lock = 'ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764'
assert (manifest['head'], manifest['tree'], manifest['lock_sha256']) == (head, tree, lock)
assert subprocess.check_output(['git', '-C', str(repo), 'rev-parse', head+'^{tree}']).decode().strip() == tree
assert sha(read(repo / 'Cargo.lock')) == lock
commands = {c['name']: c for c in manifest['commands']}
assert len(commands) == 16
expected = {
    'fmt': (0, None), 'workspace-alltargets-check': (0, None), 'test-layout': (0, None),
    'transport-library': (0, [195, 0, 1]), 'platform-unit': (0, [92, 0, 1]),
    'platform-contracts-all': (0, [82, 0, 0]), 'windows-platform': (0, None),
    'vmess-library': (0, [25, 0, 0]), 'vmess-minimal': (0, None),
    'proxy-library': (0, [648, 0, 2]), 'proxy-dispatch-all': (0, [99, 0, 0]),
    'vless-proxy-all': (0, [52, 0, 1]), 'wireguard-inbound-all': (101, [5, 2, 1]),
    'source-runtime-guard': (101, [186, 3, 0]), 'source-architecture-guard': (0, [26, 0, 0]),
    'strict': (101, None),
}
logs = {}
log_checks = []
for name, c in commands.items():
    assert (c['exit'], c['counts']) == expected[name]
    assert all(c[k] for k in ('head_unchanged', 'source_clean', 'coverage_ok', 'lock_unchanged'))
    log = read(Path(c['log'])).decode()
    logs[name] = log
    rows = re.findall(r'^test result: .*? (\d+) passed; (\d+) failed; (\d+) ignored;', log, re.M)
    if c['counts'] is not None:
        assert len(rows) == 1 and list(map(int, rows[0])) == c['counts'], name
        assert sum(c['counts']) == c['selected_test_count'], name
        selected = re.findall(r'^test (\S+)(?: - should panic)? \.\.\. (ok|FAILED|ignored(?:,.*)?)$', log, re.M)
        assert len(selected) == c['selected_test_count'], (name, len(selected))
        assert sum(result == 'ok' for _, result in selected) == c['counts'][0]
        assert sum(result == 'FAILED' for _, result in selected) == c['counts'][1]
        assert sum(result.startswith('ignored') for _, result in selected) == c['counts'][2]
    else:
        assert not rows
    log_checks.append({'name': name, 'exit': c['exit'], 'counts': c['counts'],
                       'selected_test_count': c['selected_test_count'], 'raw_log_sha256': sha(log.encode()),
                       'parsed_count_matches_manifest': True, 'frozen_source_flags_all_true': True})
assert logs['test-layout'].strip() == 'Integration layout: 313 source files, 95 targets, 0 errors'
assert 'Finished `dev` profile' in logs['workspace-alltargets-check']
assert 'Finished `dev` profile' in logs['windows-platform']
assert 'x86_64-pc-windows-gnu' in ' '.join(commands['windows-platform']['argv'])
assert commands['windows-platform']['argv'][1] == 'check'
assert 'Finished `dev` profile' in logs['vmess-minimal']
assert 'actual_remote_h2_stream_limit_bounds_pending_demands_and_cancellation_preserves_shared_carrier ... ok' in logs['transport-library']
assert 'dropping_head_after_real_pending_response_registration_reclaims_it_before_connection_cleanup ... ok' in logs['transport-library']
wg = logs['wireguard-inbound-all']
assert 'WireGuard inbound did not recover its old listener after failed reload: Elapsed(())' in wg
assert 'linked endpoint lost its session after failed bind: Elapsed(())' in wg
assert 'wireguard_inbound.rs:452:6' in wg and 'wireguard_inbound.rs:214:6' in wg
runtime = logs['source-runtime-guard']
assert 'listeners/inbound.rs must not contain boundary token `feature = "socks5"`' in runtime
assert 'listeners/inbound.rs must gate generic code by capabilities instead of concrete protocols' in runtime
assert '!listener_inbound.contains("proxy: &Proxy")' in runtime
strict = logs['strict']
assert strict.count('error: this async expression only awaits a single future') == 2
assert 'response.rs:36:33' in strict and 'response.rs:116:35' in strict
assert 'error: this loop never actually loops' in strict and 'managed_h2/wire.rs:56:17' in strict
assert 'could not compile `zero-transport` (lib test) due to 3 previous errors' in strict

# Verify all inherited payloads before writing the new stage.
for r in old_records:
    zipped = read(base / r['path'])
    content = gzip.decompress(zipped)
    assert (len(zipped), sha(zipped), len(content), sha(content)) == (r['gzip_bytes'], r['gzip_sha256'], r['raw_bytes'], r['raw_sha256'])
    if 'diagnostic_raw_path' in r:
        assert read(base / r['diagnostic_raw_path']) == content
metadata = json.loads(read(base / 'diagnostics/root37/binary-identities.json'))
binary_checks = []
for row in metadata['root37_fixed_binaries'] + metadata['prior_current31_binaries_rechecked']:
    path = Path(row['path'])
    before = path.stat()
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    assert (after.st_size, digest) == (row['bytes'], row['sha256'])
    binary_checks.append({'path': str(path), 'bytes': after.st_size, 'sha256': digest, 'executed': False, 'copied': False})
assert len(binary_checks) == 7

(stage / 'evidence/parallel-progress').mkdir(parents=True)
for r in old_records:
    target = stage / r['path']
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(read(base / r['path']))
    if 'diagnostic_raw_path' in r:
        target = stage / r['diagnostic_raw_path']
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(read(base / r['diagnostic_raw_path']))
known = {r['source'] for r in old_records}


def archive(source, kind, raw_path=None):
    assert source.name not in known
    content = read(source)
    compressed = gzip.compress(content, mtime=0)
    relative = Path('evidence/parallel-progress') / (source.name+'.gz')
    (stage / relative).write_bytes(compressed)
    r = dict(source=source.name, path=str(relative), kind=kind, source_location=str(source),
             raw_bytes=len(content), raw_sha256=sha(content), gzip_bytes=len(compressed), gzip_sha256=sha(compressed))
    if raw_path:
        target = stage / raw_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        r['diagnostic_raw_path'] = str(raw_path)
    d['records'].append(r)
    known.add(source.name)


artifacts = sorted(raw.glob('pr05-parallel-focused-39*'))
assert len(artifacts) == 17 and all(p.is_file() for p in artifacts)
for source in artifacts:
    archive(source, 'frozen-command-evidence')
for name in ('verify-progress-through39.py', 'prepare-progress-through39.py'):
    archive(Path('/workspace/core-next') / name, 'evidence-preparation-script-no-product-execution', Path('diagnostics/preparation') / name)
assert len(d['records']) == 470

d['latest_runtime_code_commit'] = head
d['latest_runtime_tree'] = tree
d['latest_runtime_lock_sha256'] = lock
d['latest_alltargets_pass_commit'] = head
d['round39'] = manifest
d['root39'] = '7a2670a4: fmt/alltargets/layout PASS; transport195/0/1 platform92/0/1 contracts82/0; Windows GNU compile-only and VMess minimal compile PASS; VMess25/0 proxy648/0/2 dispatch99/0 VLESS52/0/1; WG5/2/1 FAIL original recovery assertions; runtime186/3 FAIL; architecture26/0; strict FAIL three transport fixture lints, later private deadcode not qualified.'
d['post39_status'] = 'Frozen39 remains partial and failed. Later integration/repairs and any current31 profiling are not final-head or ALL17 qualification. Historical failures retained; no whole-workspace tests/release/downstream/platform-runtime/performance qualification added.'
d['through39_preparation'] = {
    'previous_stage': str(base), 'previous_records': 451,
    'previous_manifest_sha256': sha(old_manifest), 'previous_progress_sha256': sha(old_progress),
    'previous_stage_verification_sha256': sha(old_verification),
    'inherited_records_and_gzip_and_raw_byte_identical': True,
    'new_frozen_command_artifacts': 17, 'new_preparation_scripts': 2,
    'independent_log_checks': log_checks, 'fixed_git_tree_verified': True,
    'seven_original_binaries_rehashed_no_execution': binary_checks,
    'does_not_include_later_smaps_run': True,
    'all17_passed': False,
}
d['root39_successor_observations'] = {
    'root37_original_logical_seven_second_regression': 'actual ok; no historical root37 aggregate counts invented',
    'root38_new_head_cleanup_fixture': 'actual ok; original root38 BrokenPipe failure preserved',
    'root38_proxy_compile_failures': 'frozen39 alltargets actual exit0; historical 4lib/10libtest errors preserved',
    'runtime_guard_regression': '186/3, inline noStandby test declarations/tokens in generic listener file; original assertions retained',
    'wireguard_original_recovery_expectations': '5/2/1; two actual failed-reload/failed-bind recovery timeouts retained; audit still read-only and unimplemented',
}
d['records'].sort(key=lambda r: r['source'])
(stage / 'pr05-parallel-progress-validation.json').write_text(json.dumps(d, ensure_ascii=False, indent=2)+'\n')
appendix = '''
## root39 冻结后继记录（旧 451 条与原失败不改写）

此节只推进到 **root39 `7a2670a4ac50e92e37758806df6d40552fc7509d`**，tree `de87a8e6cf751b31e6cbd4b94cfee2c51e9aa1d1`，lock SHA `ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764`。原 [manifest](evidence/parallel-progress/pr05-parallel-focused-39.json.gz) 和全部 16 条命令的日志逐项归档；16 条均记载 `head_unchanged/source_clean/lock_unchanged/coverage_ok=true`。本次只读核验固定 Git tree、原日志逐项状态/计数与原 SHA；没有重新运行这些命令。前述截止 root38 的段落和字段仍是原历史，后继成功不删除失败。

| 实际 root39 范围 | 结果（passed/failed/ignored） | 资格边界 |
| --- | --- | --- |
| fmt、workspace all-features/all-targets check、test-layout | 全部 exit 0；layout **313 source files / 95 targets / 0 errors**。 | check 编译通过与测试执行分开。没有 workspace 全量测试或 release 构建结论。 |
| transport library | **195/0/1**；原七秒 logical 挂起回归与新 Head cleanup fixture 均实际 ok。 | root37 SIGTERM/无 aggregate counts 与 root38 BrokenPipe 原失败继续保留；忽略项不计通过。 |
| platform unit / platform contracts | **92/0/1 + 82/0/0**。 | 两个独立范围不合并重复计数，也不等于所有平台运行资格。 |
| Windows GNU target / VMess minimal | 两项 `cargo check` exit 0。 | Windows 是 `--tests --target x86_64-pc-windows-gnu` **编译检查**，未运行 Windows 测试；VMess minimal 是 `--no-default-features --features vmess` 编译检查。 |
| VMess library / Proxy library / dispatch / VLESS integration | **25/0/0；648/0/2；99/0/0；52/0/1**。 | 各自实际命令通过；不跨范围相加、忽略项不计运行通过。 |
| WireGuard inbound | **5/2/1，exit 101**。 | 原 `zero_wireguard_inbound_routes_tcp_and_udp_from_zero_outbound` 在 `wireguard_inbound.rs:452` 失败：failed reload 后旧 listener 恢复超时。原 `linked_wireguard_endpoints_share_socket_and_peer_state_in_both_directions` 在 `:214` 失败：failed bind 后丢失 session 的恢复超时。保留两条原恢复预期，未放宽断言。 |
| runtime source guard / architecture source guard | **186/3/0，exit 101；26/0/0，exit 0**。 | runtime 三项为 `generic_proxy_sources_gate_capabilities_not_protocol_lists`、`concrete_protocol_feature_gates_exist_only_in_adapters_and_registration`、`udp_ingress_runtime_collapses_proxy_and_services_for_session_loops`；日志均指向 generic `listeners/inbound.rs` 的 inline noStandby 测试具体协议 cfg / `proxy: &Proxy` token。后继修复尚不由本冻结证明。 |
| workspace strict clippy | **exit 101**。 | 原 `grpc/response.rs:36/116` 两处 `redundant_async_block`，`xhttp/managed_h2/wire.rs:56` 一处 `never_loop`；在 transport lib-test 三项错误处停止，**不把尚未完成的后续 private deadcode 等 strict 检查视为通过**。 |

root39 为前两轮源码修复提供具体后继证据：workspace all-targets 现 exit 0，原 logical 与 Head cleanup case 现都实际通过。源码自锁诊断、七秒原测试、原二进制 SHA、instrumentation、三文件 restore、每 PID strace 原始证据和 strace 一次成功的调度反例全部保持原字节。root35–38 的 451 条记录 JSON 字段、gzip 及其 raw 诊断副本逐字节继承，没有覆盖此前失败、取消或未运行状态。五个 root37 固定二进制和两个 old/current31 固定二进制仅重新读取并核 SHA，未复制、执行或删除。

[WG 恢复只读审计](diagnostics/wireguard-stopold-recovery-review.md) 继续明确未实施；原 gzip、portable 派生副本及各自 digest 均未变。root39 两个真实恢复失败不被设计方案覆盖。后续 file/packet 或其他修复的新 HEAD、root39 之后的 current31 smaps 诊断不属于此冻结，也不被此记录授予资格。**ALL17、RuntimeHost M4、完整资源替换、最终 head、全 workspace/release、跨平台运行、下游和性能资格仍未完成。**

本 stage 共 **470 条**证据记录（旧 451 + 本轮 manifest/日志 17 + 本轮制备/只读复验脚本 2）。每个 gzip 的长度/SHA、解压 raw 长度/SHA、可移植诊断副本与源码链接均核验；脚本不运行 Cargo、产品/测试 binary 或诊断负载，不修改根仓库源码/报告，不 commit/push。
'''
(stage / 'pr05-parallel-progress.md').write_bytes(old_progress + appendix.encode())

verified = []
for r in d['records']:
    compressed = (stage / r['path']).read_bytes()
    content = gzip.decompress(compressed)
    assert (len(compressed), sha(compressed), len(content), sha(content)) == (r['gzip_bytes'], r['gzip_sha256'], r['raw_bytes'], r['raw_sha256'])
    if 'diagnostic_raw_path' in r:
        assert (stage / r['diagnostic_raw_path']).read_bytes() == content
    original = raw / r['source']
    if original.is_file():
        assert read(original) == content
    verified.append({'source': r['source'], 'gzip_sha256': r['gzip_sha256'], 'raw_sha256': r['raw_sha256'], 'gzip_and_raw_verified': True})
assert {r['source']: r for r in d['records'] if r['source'] in {old['source'] for old in old_records}} == {r['source']: r for r in old_records}
for path, digest in inputs.items():
    assert sha(Path(path).read_bytes()) == digest, path
for name, content in root_reports.items():
    assert (repo / 'design/v0.0.3/implementation' / name).read_bytes() == content
for path in stage.rglob('*'):
    if path.is_file():
        with path.open('rb') as stream:
            assert stream.read(4) != b'\x7fELF', path
summary = {
    'stage': str(stage), 'scope': 'through39 partial evidence only; ALL17 and overall qualification incomplete',
    'old_records': 451, 'new_runtime_records': 17, 'new_preparation_script_records': 2,
    'total_records': 470, 'all_old_and_new_gzip_and_raw_verified': True,
    'all_old_records_metadata_and_gzip_and_raw_byte_identical': True,
    'all_source_inputs_unchanged': True, 'root_report_files_unchanged': True,
    'seven_fixed_binaries_rehashed': True, 'product_binary_archived_or_executed': False,
    'cargo_or_workload_run': False, 'commit_or_push': False,
    'latest_runtime_code_commit': head, 'latest_runtime_tree': tree,
    'overall_passed': False,
    'manifest_sha256': sha((stage / 'pr05-parallel-progress-validation.json').read_bytes()),
    'progress_sha256': sha((stage / 'pr05-parallel-progress.md').read_bytes()),
    'previous_manifest_sha256': sha(old_manifest), 'previous_progress_sha256': sha(old_progress),
    'previous_stage_verification_sha256': sha(old_verification),
}
(stage / 'stage-verification.json').write_text(json.dumps({'summary': summary, 'verified': verified}, ensure_ascii=False, indent=2)+'\n')
print(json.dumps(summary, ensure_ascii=False, indent=2))

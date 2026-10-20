"""Offline-only reconstruction from actual pair metadata and saved proc files; never executes workloads."""
from pathlib import Path
from collections import defaultdict
import hashlib
import argparse
import json
import re
import runpy
import shutil
import statistics

raw = Path('/workspace/core-next/current31-smaps-diagnostic-run-after-authorization')
prepared = Path('/workspace/core-next/current31-smaps-diagnostic-prepared')
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, default=Path('/workspace/core-next/current31-smaps-offline-derived'))
output = parser.parse_args().output
assert not output.exists(), 'derived outputs are immutable; select a new directory'
sha = lambda b: hashlib.sha256(b).hexdigest()
inputs = {}


def read(path):
    content = path.read_bytes()
    row = {'path': str(path), 'bytes': len(content), 'sha256': sha(content)}
    assert inputs.setdefault(str(path), row) == row, path
    return content


def load(path):
    return json.loads(read(path))


def digest_file(path):
    before = path.stat()
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    return {'path': str(path), 'bytes': after.st_size, 'sha256': digest, 'executed': False, 'copied': False}


# Inventory ALL original output files, not only the inputs used by aggregation.
original_files = sorted(p for p in raw.rglob('*') if p.is_file())
for p in original_files:
    read(p)
pair = load(raw / 'pair.json')
profile = load(prepared / 'profile.json')
assert sha(read(prepared / 'profile.json')) == pair['profile_sha256']
for name, expected in pair['scripts_sha256'].items():
    assert sha(read(prepared / name)) == expected, name
# These modules are pure parsers/summarizers; run_path does not enter their __main__.
original_summarize = runpy.run_path(str(prepared / 'summarize_fanin.py'))['summarize']
parse_smaps = runpy.run_path(str(prepared / 'smaps_parse.py'))['parse']
assert not list(raw.glob('*/launcher.json'))

launchers, summaries, roles, binaries = {}, {}, {}, []
full_labels = profile['sampling']['full_points']
expected_labels = ['popen_return', *full_labels, 'shutdown_after_stdin_close', 'process_exit_or_unavailable']
for role in ('old', 'new'):
    result = pair['results'][role]
    manifest = load(raw / role / 'data/manifest.json')
    assert result['exit_code'] == 0 and result['summary_exit_code'] == 1
    assert sha(read(raw / role / 'data/manifest.json')) == result['manifest_sha256']
    assert manifest['binary_sha256'] == result['actual_binary_sha256'] == profile['binary'][role]['sha256']
    assert manifest['build_info'] == result['actual_build_info']
    recorded_features = re.search(r'^features: (.+)$', manifest['build_info'], re.M).group(1).split(',')
    assert recorded_features == profile['features']
    build_git = re.search(r'^git_hash: (.+)$', manifest['build_info'], re.M).group(1)
    assert profile['binary'][role]['source'].startswith(build_git)
    binary = digest_file(Path(profile['binary'][role]['path']))
    assert binary['sha256'] == manifest['binary_sha256']
    binaries.append(binary)
    command = pair['commands'][role]
    assert f'type=bind,src={binary["path"]},dst=/kernel,readonly' in command
    assert profile['image'] in command
    assert '--network=none' in command and '--cpus=1' in command
    assert '--memory=1073741824' in command and '--memory-swap=1073741824' in command
    summary_error = read(raw / role / 'summary.log').decode()
    assert 'FileNotFoundError' in summary_error and str(raw / role / 'launcher.json') in summary_error
    launcher = {'scope': 'OFFLINE_DERIVED_FROM_ACTUAL_PAIR_NOT_ORIGINAL_LAUNCHER',
                'not_an_original_launcher_file': True, 'original_launcher_absent': True,
                'pair_path': str(raw / 'pair.json'), 'pair_sha256': sha(read(raw / 'pair.json')),
                'actual_recorded_command': command, 'recorded_pair_result': result,
                'actual_binary_rehashed': binary, 'profile_source_claim': profile['binary'][role]['source'],
                'actual_recorded_build_git_prefix': build_git,
                'manifest_path': str(raw / role / 'data/manifest.json'),
                'actual_container_id_recorded': read(raw / role / 'container.cid').decode().strip(),
                'original_summary_failure_retained': {'path': str(raw / role / 'summary.log'), 'sha256': sha(summary_error.encode()),
                                                     'exit_code': 1, 'reason': 'original launcher.json was never generated'},
                'no_new_product_docker_or_cargo_run': True}
    launchers[role] = launcher
    trial_summaries, trial_memory = [], []
    assert len(manifest['trials']) == 3
    for t in manifest['trials']:
        trial = t['trial']
        root = raw / role / f'data/trial-{trial}'
        assert load(root / 'raw.json') == t
        assert load(root / 'shutdown.json') == t['shutdown']
        assert t['shutdown']['graceful'] and t['shutdown']['exit_code'] == 0
        # Reuse the original function unchanged; its CLI alone required launcher.json.
        trial_summaries.append(original_summarize(t))
        memory = t['memory_diagnostic']
        index = load(root / 'memory/index.json')
        assert index['events'] == memory['events'] and index['errors'] == memory['errors']
        assert not index['errors'] and not t['sampler_errors']
        assert [e['label'] for e in index['events']] == expected_labels
        identity = load(root / 'memory/binary-elf-identity.json')
        assert identity['sha256'] == binary['sha256']
        points = []
        for number, event in enumerate(index['events']):
            directory = root / 'memory' / f'{number:02}-{event["label"]}'
            assert load(directory / 'capture.json') == event
            row = {'label': event['label'], 'full': event['full'], 'snapshot_complete': event['snapshot_complete'],
                   'expected_post_exit_unavailable': event['expected_post_exit_unavailable'], 'errors': event['errors'],
                   'capture_path': str(directory / 'capture.json'), 'capture_elapsed_ns': event['capture_elapsed_ns'],
                   'capture_started_after_popen_return_ns': event['capture_started_ns'] - event['popen_returned_ns']}
            if event['expected_post_exit_unavailable']:
                assert not event['snapshot_complete'] and event['process_exit_observed'] == 0
                # No fabricated final zero RSS / no projection of earlier memory as final state.
                row['address_space_metrics'] = None
                points.append(row)
                continue
            assert event['snapshot_complete'] and not event['errors']
            assert event['exe']['matches_frozen_binary_identity']
            assert event['exe']['frozen_sha256'] == binary['sha256']
            assert all(event['exe'][k] == identity[k] for k in ('inode', 'device_major', 'device_minor'))
            for name, info in event['files'].items():
                data = read(directory / name)
                assert len(data) == info['bytes'] and sha(data) == info['sha256']
            def proc_metrics(path):
                fields = {}
                for line in read(path).decode().splitlines():
                    match = re.match(r'^(\w+):\s+(\d+) kB$', line)
                    if match:
                        fields[match[1]] = int(match[2]) * 1024
                return fields
            status = proc_metrics(directory / 'status')
            rollup = proc_metrics(directory / 'smaps_rollup')
            row['status_bytes'] = status
            row['rollup_bytes'] = rollup
            row['status_minus_rollup_rss_bytes'] = status.get('VmRSS', 0) - rollup['Rss']
            if event['full']:
                account_bytes = read(directory / 'mapping-account.json')
                assert sha(account_bytes) == event['mapping_account_sha256']
                account = json.loads(account_bytes)
                mappings = parse_smaps(read(directory / 'smaps').decode())
                assert len(mappings) == event['mapping_count'] == len(account['mappings'])
                # Verify original classification without re-opening container ELF paths on the host.
                for parsed, saved in zip(mappings, account['mappings']):
                    assert all(saved[k] == v for k, v in parsed.items())
                totals = {}
                for mapping in account['mappings']:
                    category = mapping['category']
                    bucket = totals.setdefault(category, {k: 0 for k in account['categories_bytes'][category]})
                    for k, v in mapping['metrics_bytes'].items():
                        bucket[k] += v
                    if category.startswith('binary_'):
                        assert all(mapping[k] == identity[k] for k in ('inode', 'device_major', 'device_minor'))
                assert totals == account['categories_bytes']
                headers = lambda items: [(r['start'], r['end'], r['permissions'], r['offset'], r['inode']) for r in items]
                same_headers = headers(parse_smaps(read(directory / 'maps').decode())) == headers(mappings)
                assert same_headers == account['maps_and_smaps_same_headers'] == event['maps_and_smaps_same_headers']
                row.update(categories_bytes=totals, mapping_count=len(mappings), maps_and_smaps_same_headers=same_headers,
                           smaps_sum_bytes={k: sum(v[k] for v in totals.values()) for k in next(iter(totals.values()))},
                           account_path=str(directory / 'mapping-account.json'), mappings=account['mappings'])
                row['smaps_sum_minus_rollup_rss_bytes'] = row['smaps_sum_bytes']['Rss'] - rollup['Rss']
            points.append(row)
        initial = next(p for p in points if p['label'] == 'ready_before_first_apply')
        rotation = next(p for p in points if p['label'] == 'after_rotation_0')
        initial_ranges = {(r['start'], r['end'], r['permissions']) for r in initial['mappings']}
        changed = [r for r in rotation['mappings'] if r['category'] == 'other_anonymous_not_assumed_heap'
                   and (r['start'], r['end'], r['permissions']) not in initial_ranges]
        samples_by_phase = defaultdict(list)
        for sample in t['samples']:
            samples_by_phase[sample['phase']].append(sample)
        sampled_phases = {phase: {'samples': len(rows), 'rss_min_bytes': min(r['rss_bytes'] for r in rows),
                                 'rss_max_bytes': max(r['rss_bytes'] for r in rows),
                                 'rss_median_bytes': statistics.median(r['rss_bytes'] for r in rows),
                                 'first_ns': rows[0]['monotonic_ns'], 'last_ns': rows[-1]['monotonic_ns']}
                          for phase, rows in samples_by_phase.items()}
        trial_memory.append({'trial': trial, 'binary_identity': identity,
                             'readiness_confirmed_after_popen_ns': memory['readiness_confirmed_ns'] - index['events'][0]['popen_returned_ns'],
                             'points': points, 'light_sample_phases': sampled_phases,
                             'light_timing': {k: memory[k] for k in ('light_start_requested_ns', 'light_thread_entered_ns', 'first_light_sample_ns', 'last_live_light_sample')},
                             'changed_exact_range_anonymous_vmas_at_rotation0': changed,
                             'changed_vma_limit': 'new/changed exact VMA ranges, not net allocation deltas; split/merge can change ranges; no area-proportional RSS attribution',
                             'shutdown': t['shutdown']})
    summaries[role] = {'scope': 'OFFLINE_DERIVED summary; original pure summarize(trial) reused unchanged',
                       'launcher_metadata_file': f'{role}/launcher-metadata.derived.json',
                       'limits_before': manifest['limits_before'], 'limits_after': manifest['limits_after'],
                       'trials': trial_summaries, 'original_summary_exit_code': result['summary_exit_code'],
                       'original_summary_failure_not_overwritten': True}
    roles[role] = trial_memory

# Actual corresponding-phase metrics, with raw per-trial values retained.
phase_comparison = []
for label in expected_labels[:-1]:
    role_rows = {}
    for role in ('old', 'new'):
        points = [next(p for p in t['points'] if p['label'] == label) for t in roles[role]]
        metrics = {'status_VmRSS': [p['status_bytes']['VmRSS'] for p in points],
                   'rollup_Rss': [p['rollup_bytes']['Rss'] for p in points],
                   'rollup_Pss': [p['rollup_bytes']['Pss'] for p in points],
                   'rollup_Anonymous': [p['rollup_bytes']['Anonymous'] for p in points],
                   'capture_elapsed_ns': [p['capture_elapsed_ns'] for p in points],
                   'capture_started_after_popen_return_ns': [p['capture_started_after_popen_return_ns'] for p in points]}
        if points[0]['full']:
            metrics['smaps_sum_Rss'] = [p['smaps_sum_bytes']['Rss'] for p in points]
            metrics['mapping_count'] = [p['mapping_count'] for p in points]
            for category in sorted({k for p in points for k in p['categories_bytes']}):
                for metric in ('Rss', 'Pss', 'Private_Dirty', 'Anonymous'):
                    metrics[f'{category}:{metric}'] = [p['categories_bytes'].get(category, {}).get(metric, 0) for p in points]
        role_rows[role] = metrics
    common = set(role_rows['old']) & set(role_rows['new'])
    phase_comparison.append({'label': label, 'per_trial_values': role_rows,
                             'medians': {role: {k: statistics.median(v) for k, v in rows.items()} for role, rows in role_rows.items()},
                             'paired_new_minus_old': {k: [b-a for a,b in zip(role_rows['old'][k],role_rows['new'][k])] for k in sorted(common)},
                             'note': 'old/new are sequential processes, not simultaneous snapshots; at popen_return different elapsed startup times are particularly non-comparable'})

# All original output bytes and input fixture SHA checked again; binary content checked again.
for path, row in inputs.items():
    content = Path(path).read_bytes()
    assert len(content) == row['bytes'] and sha(content) == row['sha256'], path
for binary in binaries:
    assert digest_file(Path(binary['path'])) == binary

output.mkdir()
def write(relative, value):
    path = output / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n')
for role in ('old', 'new'):
    write(f'{role}/launcher-metadata.derived.json', launchers[role])
    write(f'{role}/fanin-summary.derived.json', summaries[role])
write('memory-phases.derived.json', {'scope': 'saved VMA/rollup measurements only, not allocator/owner causation', 'roles': roles, 'comparison': phase_comparison})
write('input-verification.json', {'scope': 'offline reconstruction only; all original pair/log/raw/snapshot bytes retained',
    'original_output_files_count': len(original_files), 'all_original_output_files_verified_unchanged': True,
    'all_input_files': sorted(inputs.values(), key=lambda r: r['path']), 'binaries_rehashed_twice': binaries,
    'no_original_launcher_created': True, 'original_summary_failures_preserved': True,
    'new_cargo_docker_product_workload_runs': False, 'source_head_qualification': False})
shutil.copyfile(Path(__file__), output / 'derive-current31-smaps.py')
print(json.dumps({'output': str(output), 'original_files_verified': len(original_files),
                  'trials': {role: len(rows) for role, rows in roles.items()},
                  'full_snapshots_verified': sum(p['full'] for rows in roles.values() for t in rows for p in t['points']),
                  'expected_post_exit_unavailable': sum(p['expected_post_exit_unavailable'] for rows in roles.values() for t in rows for p in t['points']),
                  'all_maps_smaps_headers_agree': all(p['maps_and_smaps_same_headers'] for rows in roles.values() for t in rows for p in t['points'] if p['full']),
                  'steady_per_trial': {r: [t['steady'] for t in s['trials']] for r,s in summaries.items()},
                  'overall_qualification': False},ensure_ascii=False,indent=2))

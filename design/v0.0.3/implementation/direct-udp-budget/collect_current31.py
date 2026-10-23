#!/usr/bin/env python3
"""Derive current31 observations from immutable raw fixture outputs."""
import json
from pathlib import Path

BASE = Path('/workspace/core-next/direct-udp-budget')
OLD_LIBRARY = '25b58d67'
CURRENT_LIBRARY = '5ddbf5ac'
PAIR = BASE / 'matched-cli-pair-current31-b649836f'


def load(path):
    return json.loads(path.read_text())


def aggregate(directory, library):
    summary = load(directory / 'summary.json')
    manifest = load(directory / 'data/manifest.json')
    counts = {name: {'sent': 0, 'received': 0, 'missing': 0}
              for name in ['steady', 'overlap', 'burst']}
    observations = []
    for number, row in enumerate(summary['trials']):
        p = row['packets_and_os'] if library else row
        for name, record in [('steady', p['steady']), ('overlap', p['overlapping_reload'])]:
            counts[name]['sent'] += record['sent']
            counts[name]['received'] += record['unique_received']
            counts[name]['missing'] += record['not_observed']
        for record in p['bursts'].values():
            counts['burst']['sent'] += record['sent']
            counts['burst']['received'] += record['unique_received']
            counts['burst']['missing'] += record['not_observed']
        raw = manifest['trials'][number]
        missing = [sample['phase'] for sample in raw['samples'] if sample['udp'] and
                   not any(u['inode'] == raw['initial_listener_inode'] for u in sample['udp'])]
        record = {'trial': number,
            'process_vmhwm_bytes': p['kernel_vmhwm_max_observed_bytes'],
            'native_threads_peak_sampled': p['native_threads_peak_sampled'],
            'process_fd_peak_sampled': p['process_fd_peak_sampled'],
            'process_udp_fd_peak_sampled': p['process_udp_fd_peak_sampled'],
            'echo_port_correlated_fd_peak_sampled': p['echo_correlated_direct_fd_peak_sampled'],
            'historical_correlated_inodes_sampled': p['distinct_echo_correlated_direct_socket_inodes_sampled'],
            'active_business_flows_peak_observed': p['active_business_flows_peak_observed'],
            'historical_business_flow_ids_observed': p['distinct_business_flow_ids_observed'],
            'own_udp_kernel_rx_queue_peak_sampled_bytes': p['own_udp_kernel_rx_queue_peak_sampled_bytes'],
            'egress_generations_observed': p['egress_generations_observed'],
            'samples_omitting_initial_ingress_inode': missing,
            'apply_failures': p['apply_failures'], 'packet_errors': p['packet_errors'],
            'burst_errors': p['burst_errors'], 'other_errors': p['errors'],
            'shutdown': p['shutdown']}
        if library:
            k = row['tokio']
            tasks = raw['library_observer']['task_observation']['samples']
            record.update({'tokio_owned_task_peak_sampled': k['task_peak_sampled'],
                'scheduler_injection_queue_peak_sampled': k['tokio_global_queue_depth_peak_sampled'],
                'task_samples_truncated': k['truncated'], 'task_first_sample': tasks[0],
                'task_final_sample': tasks[-1], 'task_phase_ranges': k['phases'],
                'actual_network_invalidations': row['actual_network_invalidations']})
        else:
            record['tokio_owned_task_peak_sampled'] = None
        observations.append(record)
    for c in counts.values():
        assert c['sent'] - c['received'] == c['missing']
    return {'directory': str(directory), 'launcher': summary['launcher'],
        'payload_counts': counts, 'trials': observations,
        'limits_before': summary['limits_before'], 'limits_after': summary['limits_after'],
        'build_info': manifest.get('build_info'),
        'application_owner_pending_payload_bytes_peak': None, 'exact_native_open_attempts': None}


def main():
    results = {'scope': 'intermediate current31 observations; not ALL17 qualification, calibration, or defaults',
        'old_source': '2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a',
        'current_source': '9e3f3ed33024e32f61685b0bff5bb4611f365109',
        'current_tree': '12fdc3ad23e89a88ccf91ed56a46c0cd04128182',
        'current_source_lock_sha256': 'ffed93de069ad338a786490fc65e5cbc01dfc6a2c9116ab7ba853bc5abbc8764',
        'library': {}, 'matched_cli_pair': {}, 'suggested_new_capacity_defaults': None,
        'measurement_window': 'parent explicitly CARGOFINISHED before first run and withheld Cargo until measurement DONE',
        'raw_label_corrections': [
            'launcher concurrent_validation is preserved historical generic wording; no parent Cargo ran in this window',
            'CLI summary.scope legacy old Direct Combined wording applies both artifacts; actual build_info/source/SHA identify each',
            'default in pair runner means ordinary production execution path, not Cargo default features'],
        'caveats': [
            'historical old library and current library trials were taken at different times; not paired causal regression evidence',
            'fresh same-window old/new CLI pair has same 17 compiled feature leaves and unchanged fixture',
            'reload offered counts depend on actual apply interval and real sender schedule; not identical offered traffic or throughput qualification',
            'library includes bounded Rust sampler and two actual native probe threads; CLI RSS is separate',
            'task gauge excludes spawn_blocking and CLI tasks; scheduler injection queue is not application payload bytes',
            'phase before_proxy spans startup until explicit phase command; final owner-drop phase may include a 1-to-0 tail',
            'UDP API generation invalidation is not physical route rotation',
            'non-atomic procfs samples and echo-port correlation are not exact open attempts or native close receipts',
            'current31 endpoint queue library is not production-activated; do not attribute losses to that unused policy',
            'public shutdown response duration includes fixed 300ms observer tail; not product native shutdown latency']}
    for profile in ['policy', 'network']:
        results['library'][profile] = {
            'old': aggregate(BASE / f'library-old-{profile}-{OLD_LIBRARY}', True),
            'current31': aggregate(BASE / f'library-current-{profile}-{CURRENT_LIBRARY}', True)}
    for role in ['old', 'new']:
        results['matched_cli_pair'][role] = aggregate(PAIR / role, False)
    features = []
    for role, data in results['matched_cli_pair'].items():
        info = data['build_info'].splitlines()
        feature = next(line.removeprefix('features: ') for line in info if line.startswith('features: '))
        features.append(feature.split(','))
        assert len(features[-1]) == 17
        expected = '2eedc2b8' if role == 'old' else '9e3f3ed3'
        assert f'git_hash: {expected}' in info
    assert features[0] == features[1]
    results['matched_cli_features_exact'] = features[0]
    output = BASE / 'current31-observations.json'
    output.write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps({'output': str(output),
        'library': {p: {r: d['payload_counts'] for r, d in rows.items()} for p, rows in results['library'].items()},
        'cli': {r: d['payload_counts'] for r, d in results['matched_cli_pair'].items()}}, indent=2))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Keep steady, overlapping reload, and burst loss separate in actual fan-in evidence."""
import argparse
import ipaddress
import json
from pathlib import Path


def summarize(trial):
    ports = {(ipaddress.ip_address(p['sender'][0]).version, p['sender'][1])
             for p in trial['echo_packets']}
    samples = trial.get('samples', [])
    inodes, peaks = set(), []
    for sample in samples:
        matched = {r['inode'] for r in sample['udp'] if (r['family'], r['local_port']) in ports
                   and r['inode'] != trial['initial_listener_inode']}
        inodes.update(matched)
        peaks.append(len(matched))
    ids, generations, active = set(), set(), []
    for step in trial['steps']:
        flows = step['active_flows'].get('result', {}).get('active_flows', [])
        active.append(len(flows))
        for flow in flows:
            ids.add(flow['id'])
            egress = flow.get('record', {}).get('path', {}).get('network', {}).get('egress', {})
            if 'generation' in egress:
                generations.add(egress['generation'])
    steady = [s['steady'] for s in trial['steps']]
    overlapping = [s['overlap'] for s in trial['steps'] if 'overlap' in s]
    overlap_echo = sum(sum(o['apply_started_ns'] <= p['monotonic_ns'] <=
        o['apply_started_ns'] + o['apply_elapsed_ns'] for p in trial['echo_packets']) for o in overlapping)
    return {'trial': trial['trial'], 'payload_bytes': trial['payload_bytes'],
        'actual_source_peer_count': len(trial['actual_source_peers']),
        'steady': {'sent': sum(r['sent'] for r in steady),
                   'unique_received': sum(r['unique_received'] for r in steady),
                   'not_observed': sum(r['not_observed'] for r in steady)},
        'overlapping_reload': {'sent': sum(r['sent'] for r in overlapping),
            'unique_received': sum(r['unique_received'] for r in overlapping),
            'not_observed': sum(r['not_observed'] for r in overlapping),
            'actual_sends_within_apply_interval': sum(r['sent_during_apply'] for r in overlapping),
            'echo_receives_within_apply_interval': overlap_echo},
        'apply_failures': [s['rotation'] for s in trial['steps'] if
            not s.get('initial_apply', s.get('overlap', {}).get('apply', {})).get('ok')],
        'packet_errors': [r for r in steady + overlapping if r['errors'] or r.get('send_errors')],
        'bursts': {name: {key: row[key] for key in
            ('sent', 'unique_received', 'not_observed', 'duplicates')}
            for name, row in trial['bursts'].items()},
        'burst_errors': [row for row in trial['bursts'].values()
                         if row['errors'] or row.get('send_errors')],
        'active_business_flows_peak_observed': max(active, default=None),
        'distinct_business_flow_ids_observed': len(ids),
        'echo_correlated_direct_fd_peak_sampled': max(peaks, default=None),
        'distinct_echo_correlated_direct_socket_inodes_sampled': len(inodes),
        'process_fd_peak_sampled': max((s['fd_count'] for s in samples), default=None),
        'process_udp_fd_peak_sampled': max((len(s['udp']) for s in samples), default=None),
        'kernel_vmhwm_max_observed_bytes': max((s['high_water_rss_bytes'] for s in samples), default=None),
        'native_threads_peak_sampled': max((s['native_threads'] for s in samples), default=None),
        'own_udp_kernel_rx_queue_peak_sampled_bytes': max((sum(r['rx_queue_kernel_bytes'] for r in s['udp']) for s in samples), default=None),
        'ingress_inode_observed_in_all_samples_with_udp': all(any(r['inode'] == trial['initial_listener_inode'] for r in s['udp']) for s in samples if s['udp']),
        'egress_generations_observed': sorted(generations),
        'actual_wire_targets': sorted({tuple(p['target'][:2]) for p in trial['echo_packets']}),
        'tokio_task_peak': None, 'user_queue_bytes_peak': None,
        'exact_native_open_attempts': None, 'sample_count': len(samples),
        'errors': [e for e in [trial.get('error'), *trial.get('sampler_errors', [])] if e],
        'shutdown': trial['shutdown']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.directory / 'data/manifest.json').read_text())
    result = {'launcher': json.loads((args.directory / 'launcher.json').read_text()),
        'limits_before': manifest['limits_before'], 'limits_after': manifest['limits_after'],
        'source_provenance_correction': (json.loads((args.directory / 'provenance-correction.json').read_text())
            if (args.directory / 'provenance-correction.json').exists() else None),
        'scope': 'old Direct Combined; sixteen source peers, one fixed domain, actual overlapping policy reload traffic',
        'caveats': ['sampled FD peaks are lower bounds, not close receipts',
                    'kernel UDP bytes are not user queue bytes',
                    'no numeric default inferred; only generation0 observed',
                    'steady, overlapping reload and burst missing payloads reported separately'],
        'trials': [summarize(t) for t in manifest['trials']]}
    (args.directory / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()

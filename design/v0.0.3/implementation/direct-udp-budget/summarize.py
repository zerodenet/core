#!/usr/bin/env python3
"""Summarize measured cardinalities without inventing unobservable task/heap data."""
import argparse
import ipaddress
import json
from pathlib import Path


def summarize(trial):
    samples = trial.get('samples', [])
    direct_ports = {(ipaddress.ip_address(row['sender'][0]).version, row['sender'][1])
                    for row in trial['echo_packets']}
    direct_inodes = set()
    direct_peak = 0
    for sample in samples:
        matched = {row['inode'] for row in sample['udp']
                   if (row['family'], row['local_port']) in direct_ports
                   and row['inode'] != trial['initial_listener_inode']}
        direct_peak = max(direct_peak, len(matched))
        direct_inodes.update(matched)
    ids, generations, active_peak = set(), set(), 0
    for step in trial['steps']:
        flows = step['active_flows'].get('result', {}).get('active_flows', [])
        active_peak = max(active_peak, len(flows))
        for flow in flows:
            ids.add(flow['id'])
            network = flow.get('record', {}).get('path', {}).get('network', {})
            if 'generation' in network.get('egress', {}):
                generations.add(network['egress']['generation'])
    exchanges = [item for step in trial['steps'] for item in step['exchanges']]
    return {'profile': trial['profile'],
        'verified_roundtrips': sum(item['success'] for item in exchanges),
        'attempted_roundtrips': len(exchanges),
        'roundtrip_failures': [item for item in exchanges if not item['success']],
        'policy_apply_failures': [step for step in trial['steps']
                                  if 'apply' in step and not step['apply'].get('ok')],
        'active_business_flows_peak_observed': active_peak,
        'distinct_business_flow_ids_observed': len(ids),
        'egress_generations_observed': sorted(generations),
        'actual_wire_targets': sorted({tuple(packet['target'][:2]) for packet in trial['echo_packets']}),
        'process_fd_peak_sampled': max((sample['fd_count'] for sample in samples), default=None),
        'process_udp_fd_peak_sampled': max((len(sample['udp']) for sample in samples), default=None),
        'echo_correlated_direct_fd_peak_sampled': direct_peak,
        'distinct_echo_correlated_direct_socket_inodes_sampled': len(direct_inodes),
        'kernel_rss_peak_sampled_bytes': max((sample['rss_bytes'] for sample in samples), default=None),
        'kernel_vmhwm_max_observed_bytes': max((sample['high_water_rss_bytes'] for sample in samples), default=None),
        'native_threads_peak_sampled': max((sample['native_threads'] for sample in samples), default=None),
        'own_udp_kernel_rx_queue_peak_sampled_bytes': max((sum(row['rx_queue_kernel_bytes'] for row in sample['udp']) for sample in samples), default=None),
        'own_udp_kernel_tx_queue_peak_sampled_bytes': max((sum(row['tx_queue_kernel_bytes'] for row in sample['udp']) for sample in samples), default=None),
        'tokio_task_peak': None, 'user_queue_bytes_peak': None,
        'exact_native_open_attempts': None,
        'bursts': trial.get('bursts'), 'shutdown': trial.get('shutdown'),
        'errors': [trial.get('error'), *trial.get('sampler_errors', [])],
        'sample_count': len(samples)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.directory / 'data/manifest.json').read_text())
    result = {'launcher': json.loads((args.directory / 'launcher.json').read_text()),
              'limits_after': manifest['limits_after'],
              'measurement': 'fixed old Combined Direct only; no new-target qualification',
              'caveats': ['FD/inode observations are not native close receipts',
                          'echo-port correlation excludes only the known ingress inode; raw snapshots retained',
                          '10ms samples can miss short peaks; RSS VmHWM separately retained',
                          'kernel UDP queue accounting differs from payload/user heap bytes',
                          'no new numeric production defaults inferred from this profile'],
              'trials': [summarize(trial) for trial in manifest['trials']]}
    (args.directory / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Reuse the actual fixed-domain 16-source workload with a compiled library observer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
from fanin import trial
from observer_kernel import ObserverKernel
from worker import cgroup, write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--payload-bytes', type=int, default=1024)
    parser.add_argument('--invalidate-network', action='store_true')
    args = parser.parse_args()
    assert 1 <= args.trials <= 3 and 128 <= args.payload_bytes <= 65507
    args.output.mkdir(parents=True)
    before = cgroup()
    assert before['cpu.max'] == '100000 100000'
    assert before['memory.max'] == '1073741824' and before['memory.swap.max'] == '0'
    result = {'limits_before': before, 'command': os.sys.argv, 'uname': list(os.uname()),
        'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        'scope': 'actual public library observer; not CLI RSS; Python/native echo outside observer PID',
        'application_queue_bytes': None,
        'rotation': 'actual policy and EgressControl generation' if args.invalidate_network else 'policy only',
        'trials': []}
    expected_source = json.loads((args.output.parent / 'launcher.json').read_text())['observer_metadata']
    def create(binary, config, output):
        return ObserverKernel(binary, config, output, args.invalidate_network)
    for number in range(args.trials):
        output = args.output / f'trial-{number}'
        data = trial(args.binary, output, number, args.payload_bytes, kernel_type=create)
        data['library_observer'] = json.loads((output / 'task-observation.json').read_text())
        data['network_invalidations'] = json.loads((output / 'network-invalidations.json').read_text())
        reply = data['library_observer'] or {}
        embedded = json.loads(reply['observer_source']) if reply.get('observer_source') else None
        if embedded != expected_source:
            data['error'] = 'compiled observer source metadata differs from supplied source metadata'
        write(output / 'raw.json', data)
        result['trials'].append(data)
        write(args.output / 'manifest.json', result)
    result['limits_after'] = cgroup()
    write(args.output / 'manifest.json', result)
    if any(t.get('error') or t.get('sampler_errors') or not t['shutdown']['graceful']
           for t in result['trials']):
        raise SystemExit('actual library fixture/shutdown failed; raw evidence retained')


if __name__ == '__main__':
    main()

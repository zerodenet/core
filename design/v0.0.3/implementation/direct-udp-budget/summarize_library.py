#!/usr/bin/env python3
"""Combine actual library Tokio metrics with independently observed OS/packet data."""
import argparse
import json
from pathlib import Path
from summarize_fanin import summarize


def tasks(trial):
    reply = trial.get('library_observer') or {}
    observation = reply.get('task_observation') or {}
    values = observation.get('samples') or []
    phases = {}
    for sample in values:
        phases.setdefault(sample['phase'], []).append(sample)
    return {'shutdown_response_ok': reply.get('ok'),
        'runtime_workers': reply.get('runtime_workers'),
        'scope': reply.get('scope'), 'truncated': observation.get('truncated'),
        'observer_tokio_tasks_spawned': reply.get('harness_tokio_tasks_spawned'),
        'observer_source': reply.get('observer_source'),
        'task_peak_sampled': max((s['alive_tasks'] for s in values), default=None),
        'tokio_global_queue_depth_peak_sampled': max((s['global_queue_depth'] for s in values), default=None),
        'application_pending_payload_bytes_peak': None,
        'application_queue_reason': observation.get('application_queue_reason', 'no actual owner metrics available'),
        'phases': {phase: {'samples': len(rows),
            'alive_tasks_min': min(s['alive_tasks'] for s in rows),
            'alive_tasks_max': max(s['alive_tasks'] for s in rows),
            'global_queue_depth_max': max(s['global_queue_depth'] for s in rows)}
            for phase, rows in phases.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.directory / 'data/manifest.json').read_text())
    result = {'launcher': json.loads((args.directory / 'launcher.json').read_text()),
        'limits_before': manifest['limits_before'], 'limits_after': manifest['limits_after'],
        'scope': manifest['scope'], 'rotation': manifest['rotation'],
        'caveats': ['Tokio global queue is scheduler work, not UDP messages/payload bytes',
            'native threads are separate; library probe adds two actual joined native threads',
            'process RSS includes bounded Rust sampler storage; no application queue byte estimate',
            'library task scope excludes CLI/IPC/Connector; default CLI pair is a separate measurement',
            'sampled FD/task peaks are not linearizable and not native close receipts'],
        'trials': [{'packets_and_os': summarize(t), 'tokio': tasks(t),
            'actual_network_invalidations': t.get('network_invalidations')}
            for t in manifest['trials']]}
    (args.directory / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()

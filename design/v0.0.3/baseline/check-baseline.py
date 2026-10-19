#!/usr/bin/env python3
"""Check stored evidence consistency without claiming fresh runtime execution."""

import gzip
import hashlib
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    fixture = root / 'fixtures'
    manifest = json.loads((fixture / 'manifest.json').read_text())
    actual = {str(p.relative_to(fixture)) for p in fixture.rglob('*')
              if p.is_file() and p.name != 'manifest.json'}
    assert actual == set(manifest['files']), 'unlisted or missing fixture'
    for name, digest in manifest['files'].items():
        assert hashlib.sha256((fixture / name).read_bytes()).hexdigest() == digest, name
    frames = json.loads((fixture / 'wire/ipc-frames.json').read_text())
    assert len(frames) == 22
    assert all(f['response']['api_id'] == 'zero.api.v1' for f in frames)
    events = [json.loads(line) for line in (fixture / 'wire/events.jsonl').read_text().splitlines()]
    for event in events:
        if 'event_type' in event:
            assert event['schema_id'] == 'zero.event.v1'
    for name in ['flow.snapshot', 'flow.completed', 'config.changed', 'stats.reset']:
        assert any(event.get('event_type') == name for event in events), name
    for name in ['resource-baseline.json', 'futex-observation.json']:
        report = json.loads((root / name).read_text())
        trace = report.get('raw_trace', report.get('trace'))
        compressed = (root / trace['path']).read_bytes()
        assert hashlib.sha256(compressed).hexdigest() == trace['sha256']
        raw = gzip.decompress(compressed)
        assert hashlib.sha256(raw).hexdigest() == trace['uncompressed_sha256']
        if name == 'resource-baseline.json':
            measured = json.loads(raw)
            assert len(measured['trials']) == 5
            assert measured['limits']['cpu.max'] == '100000 100000'
            assert measured['limits']['memory.max'] == 1073741824
            assert measured['limits']['memory.swap.max'] == '0'
            memory_events = dict(line.split() for line in measured['cgroup_memory_events'].splitlines())
            assert int(memory_events['oom']) == 0 and int(memory_events['oom_kill']) == 0
            assert all(t['shutdown']['graceful'] and t['shutdown']['exit_code'] == 0
                       for t in measured['trials'])
    validation = json.loads((root / 'validation-results.json').read_text())
    assert validation['workspace']['passed'] == 2635
    assert validation['workspace']['failed'] == 0
    assert validation['workspace']['ignored'] == 159
    print(f'PASS: {len(actual)} fixture hashes; {len(frames)} V1 replies; {len(events)} event frames')
    print('PASS: complete raw resource/futex traces, real 1CPU/1GiB limits and zero OOM')
    print('INFO: validates recorded evidence only; does not execute kernels or close outstanding qualifications')


if __name__ == '__main__':
    main()

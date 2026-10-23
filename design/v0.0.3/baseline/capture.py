#!/usr/bin/env python3
"""Capture old executable behavior; does not synthesize expected responses."""

import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil
import socket
import subprocess
import time

from runtime_probe import DisposableKernel, EchoServer, read_exact


def capture(binary, source, output, expected_commit):
    output.mkdir(parents=True, exist_ok=True)
    build = subprocess.check_output([str(binary), 'build-info'], text=True)
    fields = dict(line.split(': ', 1) for line in build.splitlines() if ': ' in line)
    source_commit = subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    assert source_commit == expected_commit, source_commit
    reported_commit = fields['git_hash']
    assert len(reported_commit) >= 7 and expected_commit.startswith(reported_commit), reported_commit
    (output / 'build-info.txt').write_text(build)
    fixtures = output / 'v1-config'
    shutil.copytree(source / 'examples', fixtures, dirs_exist_ok=True)
    validation = []
    for config in sorted(fixtures.rglob('*.json')):
        result = subprocess.run([str(binary), 'validate', str(config)],
                                capture_output=True, text=True, timeout=30)
        # Preserve relative fixture identity; only replace the known root path.
        validation.append({
            'input': str(config.relative_to(output)),
            'origin': str(config.relative_to(fixtures)),
            'exit_code': result.returncode,
            'stdout': result.stdout.replace(str(output), '${CAPTURES}'),
            'stderr': result.stderr.replace(str(output), '${CAPTURES}'),
        })
    (output / 'validation-results.json').write_text(json.dumps(validation, indent=2) + '\n')
    observations = {}
    echo = EchoServer()
    try:
        with DisposableKernel(binary, output / 'wire') as kernel:
            for name in ['health', 'capabilities', 'config', 'runtime', 'stats',
                         'policies', 'active_flows', 'tun_status']:
                assert kernel.query(name)['ok'], name
            subscription = socket.socket(socket.AF_UNIX)
            subscription.settimeout(.25)
            subscription.connect(str(kernel.endpoint))
            subscription.sendall(json.dumps({'type': 'subscribe', 'id': 'baseline-events'}).encode() + b'\n')
            stream = kernel.socks_stream(echo.server_address)
            payload = b'old-kernel-real-flow\x00' * 64
            stream.sendall(payload)
            assert read_exact(stream, len(payload)) == payload
            observations['payload_bytes'] = len(payload)
            observations['same_config_response'] = kernel.apply(kernel.config)
            candidate = kernel.changed_rules(1)
            response = kernel.apply(candidate)
            assert response['ok'], response
            stream.sendall(payload)
            observations['ordinary_update_preserves_stream'] = read_exact(stream, len(payload)) == payload
            before = kernel.query('runtime')
            invalid = copy.deepcopy(candidate)
            invalid['schema_version'] = 999
            rejected = kernel.apply(invalid)
            assert not rejected['ok'], rejected
            after = kernel.query('runtime')
            observations['invalid_schema_keeps_revision'] = (
                before['result']['runtime']['config_revision'] ==
                after['result']['runtime']['config_revision'])
            observations['temporary_apply_keeps_file'] = json.loads(kernel.path.read_text()) == kernel.config
            persisted = kernel.apply(candidate, persistent=True)
            assert persisted['ok'], persisted
            written = json.loads(kernel.path.read_text())
            (output / 'wire' / 'persisted-config.json').write_text(json.dumps(written, indent=2) + '\n')
            observations['persistent_apply_preserves_requested_rules'] = (
                written['route']['rules'] == candidate['route']['rules'])
            observations['persistent_apply_normalizes_document'] = written != candidate
            kernel.command('stats.reset', {}, 'stats.reset rejects missing identity and targets')
            traffic = kernel.request({'type': 'query', 'request': {
                'traffic_stat': {'scope': {'kind': 'global'}}}}, 'global traffic before reset')
            assert traffic['ok'], traffic
            snapshot = traffic['result']['traffic_stat']
            reset = kernel.command('stats.reset', {
                'expected_core_instance_id': snapshot['core_instance_id'],
                'targets': [{'scope': {'kind': 'global'},
                             'expected_stats_epoch': snapshot['stats_epoch']}],
            }, 'stats.reset explicit global identity')
            assert reset['ok'], reset
            kernel.query('stats')
            kernel.query('active_flows')
            blocked = copy.deepcopy(candidate)
            blocked['inbounds'][0]['listen']['port'] = echo.server_address[1]
            failure = kernel.apply(blocked)
            assert not failure['ok'], failure
            observations['failed_bind_reply'] = failure
            try:
                stream.sendall(payload)
                observations['failed_bind_retains_old_stream'] = read_exact(stream, len(payload)) == payload
            except (OSError, EOFError):
                observations['failed_bind_retains_old_stream'] = False
            kernel.query('runtime')
            kernel.query('health')
            stream.close()
            time.sleep(.2)
            events = bytearray()
            try:
                while block := subscription.recv(65536):
                    events.extend(block)
            except TimeoutError:
                pass
            subscription.close()
            (output / 'wire' / 'events.jsonl').parent.mkdir(parents=True, exist_ok=True)
            (output / 'wire' / 'events.jsonl').write_bytes(events)
        observations['shutdown'] = kernel.stop_result
    finally:
        echo.close()
    assert observations['ordinary_update_preserves_stream']
    assert observations['invalid_schema_keeps_revision']
    assert observations['temporary_apply_keeps_file']
    assert observations['persistent_apply_preserves_requested_rules']
    assert observations['shutdown']['graceful'] and observations['shutdown']['exit_code'] == 0
    (output / 'observations.json').write_text(json.dumps(observations, indent=2) + '\n')
    manifest = {'source_commit': expected_commit,
                'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                'inputs': 'fixed source examples and existing management/live-status test shapes',
                'files': {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in sorted(output.rglob('*')) if p.is_file() and p.suffix != '.log'}}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Captured {len(validation)} real configuration results and {len(kernel.frames)} IPC replies')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--expected-commit', required=True)
    args = parser.parse_args()
    capture(args.binary.resolve(), args.source.resolve(), args.output.resolve(), args.expected_commit)

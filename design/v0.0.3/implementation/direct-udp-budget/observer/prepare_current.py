#!/usr/bin/env python3
"""Prepare only: exact current-source UDP observer and a sole-owner Cargo plan."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


def check_source(source, head, lock):
    actual = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    assert actual == head, (actual, head)
    actual_lock = hashlib.sha256((source / 'Cargo.lock').read_bytes()).hexdigest()
    assert actual_lock == lock, (actual_lock, lock)
    for args in (['diff', '--exit-code'], ['diff', '--cached', '--exit-code']):
        subprocess.run(['git', '-C', str(source), *args], check=True, capture_output=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=Path('/workspace/core'))
    parser.add_argument('--expected-head', required=True)
    parser.add_argument('--expected-lock-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--target-dir', type=Path, default=Path('/workspace/core-target'))
    args = parser.parse_args()
    assert re.fullmatch('[0-9a-f]{40}', args.expected_head)
    assert re.fullmatch('[0-9a-f]{64}', args.expected_lock_sha256)
    source, output = args.source.resolve(), args.output.resolve()
    assert args.target_dir.resolve() == Path('/workspace/core-target'), 'root requires existing /workspace/core-target; no second target cache'
    check_source(source, args.expected_head, args.expected_lock_sha256)
    prepare = source / 'design/v0.0.3/implementation/direct-udp-budget/observer/prepare.py'
    command = [sys.executable, '-B', str(prepare), '--source', str(source),
               '--expected-commit', args.expected_head, '--output', str(output)]
    prepared = subprocess.run(command, capture_output=True, text=True)
    if prepared.returncode:
        sys.stdout.write(prepared.stdout); sys.stderr.write(prepared.stderr)
        raise SystemExit(prepared.returncode)
    check_source(source, args.expected_head, args.expected_lock_sha256)
    metadata = json.loads((output / 'source.json').read_text())
    assert metadata['source_commit'] == args.expected_head
    assert metadata['source_lock_sha256'] == args.expected_lock_sha256
    verify = [sys.executable, '-B', str(prepare), '--output', str(output), '--verify-only']
    verified = subprocess.run(verify, capture_output=True, text=True, check=True)
    (output / 'prepared-verify.json').write_text(verified.stdout)
    manifest = str(output / 'Cargo.toml')
    environment = {'CARGO_TARGET_DIR': str(args.target_dir.resolve())} if args.target_dir else {}
    steps = [
        {'kind': 'verify_before', 'argv': verify},
        {'kind': 'metadata_resolve_standalone_lock', 'argv': ['cargo', 'metadata', '--offline', '--format-version', '1', '--manifest-path', manifest]},
        {'kind': 'verify_resolved_versions', 'argv': verify},
        {'kind': 'build', 'argv': ['cargo', 'build', '--release', '--offline', '--locked', '--manifest-path', manifest]},
        {'kind': 'verify_after', 'argv': verify},
    ]
    plan = {'status': 'prepared; no Cargo/compiler executed by preparation script',
        'source_head': args.expected_head, 'source_lock_sha256': args.expected_lock_sha256,
        'source_metadata': metadata, 'prepare_command': command,
        'manifest': manifest, 'cargo_environment_for_sole_owner': environment,
        'sole_cargo_owner_steps': steps,
        'artifact_warning': 'same package binary name overwrites the previous observer in a shared target; owner must copy and hash each actual completed artifact',
        'scope_warning': 'all observations belong only to this source/lock; later strict fixes require a new preparation and artifact',
        'runtime_plan': 'policy3 and actual API-generation+policy3 with existing run_library.py; default CLI pair separately against an actual same-head new CLI artifact',
        'pending': ['sole-owner metadata/build', 'frozen actual binary SHA', 'actual current-only runtime measurement', 'same-head default CLI pair']}
    (output / 'sole-owner-plan.json').write_text(json.dumps(plan, indent=2) + '\n')
    print(json.dumps(plan, indent=2))


if __name__ == '__main__':
    main()

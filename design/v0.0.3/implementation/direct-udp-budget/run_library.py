#!/usr/bin/env python3
"""Freeze a built binary, then run only loopback in a real 1 CPU/1 GiB container."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import os

IMAGE = 'python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--payload-bytes', type=int, default=1024)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--invalidate-network', action='store_true')
    parser.add_argument('--observer-metadata', type=Path, required=True)
    parser.add_argument('--observer-lock', type=Path, required=True)
    args = parser.parse_args()
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    frozen = destination / 'kernel'
    shutil.copyfile(args.binary, frozen)
    frozen.chmod(0o500)
    actual = hashlib.sha256(frozen.read_bytes()).hexdigest()
    assert actual == args.expected_sha256, (actual, args.expected_sha256)
    code = Path(__file__).resolve().parent
    command = ['docker', 'run', '--rm', '--pull=never', '--network=none', '--cpus=1',
               '--memory=1073741824', '--memory-swap=1073741824',
               '--mount', f'type=bind,src={frozen},dst=/kernel,readonly',
               '--mount', f'type=bind,src={code},dst=/probe,readonly',
               '--mount', f'type=bind,src={destination},dst=/results', IMAGE,
               'python3', '/probe/library_worker.py', '--binary', '/kernel', '--output', '/results/data',
               '--payload-bytes', str(args.payload_bytes), '--trials', str(args.trials)]
    if args.invalidate_network:
        command.append('--invalidate-network')
    metadata = json.loads(args.observer_metadata.read_text())
    assert metadata['source_commit'] == args.source_commit
    launcher = {'observer_metadata': metadata,
                'observer_build_lock_sha256': hashlib.sha256(args.observer_lock.read_bytes()).hexdigest(),
                'source_commit': args.source_commit, 'binary_sha256': actual,
                'container_image': IMAGE, 'command': command,
                'scripts_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in sorted(code.glob('*.py'))},
                'compile_processes': 'excluded; none started by harness',
                'reference_processes': 'Python echo/DNS/harness share container; accounted separately from kernel PID'}
    launcher['host_context'] = {'load_average': list(os.getloadavg()),
        'cpu.max': Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
        'memory.max': Path('/sys/fs/cgroup/memory.max').read_text().strip(),
        'concurrent_validation': 'parent is validating main; sizing observations only, not isolated RS01 throughput qualification'}
    (destination / 'launcher.json').write_text(json.dumps(launcher, indent=2) + '\n')
    with (destination / 'container.log').open('w') as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    launcher['exit_code'] = result.returncode
    (destination / 'launcher.json').write_text(json.dumps(launcher, indent=2) + '\n')
    sys.exit(result.returncode)


if __name__ == '__main__':
    main()

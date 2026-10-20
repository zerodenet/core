#!/usr/bin/env python3
"""Run the same actual default CLI workload against frozen old/new product binaries."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for role in ('old', 'new'):
        parser.add_argument(f'--{role}-binary', type=Path, required=True)
        parser.add_argument(f'--{role}-sha256', required=True)
        parser.add_argument(f'--{role}-source', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    for role in ('old', 'new'):
        assert re.fullmatch('[0-9a-f]{40}', getattr(args, role + '_source'))
        assert re.fullmatch('[0-9a-f]{64}', getattr(args, role + '_sha256'))
    args.output.mkdir(parents=True, exist_ok=False)
    code = Path(__file__).resolve().parent
    record = {'scope': 'actual old/new default CLI pair; no production/default changes',
        'profile': 'same fixed domain, sixteen sources, 16 family/policy rotations, 1024-byte payload',
        'trials': 3, 'results': {},
        'gate': 'measurement only; all steady/reload/burst failures retained; no guessed numeric thresholds'}
    for role in ('old', 'new'):
        output = args.output / role
        command = [sys.executable, '-B', str(code / 'run_fanin.py'),
            '--binary', str(getattr(args, role + '_binary').resolve()),
            '--expected-sha256', getattr(args, role + '_sha256'),
            '--source-commit', getattr(args, role + '_source'),
            '--output', str(output.resolve()), '--trials', '3', '--payload-bytes', '1024']
        with (args.output / (role + '-launcher.log')).open('w') as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        entry = {'command': command, 'exit_code': result.returncode}
        if (output / 'data/manifest.json').exists():
            manifest = json.loads((output / 'data/manifest.json').read_text())
            entry['actual_build_info'] = manifest.get('build_info')
            if 'limits_after' in manifest:
                summarize = [sys.executable, '-B', str(code / 'summarize_fanin.py'), str(output)]
                with (args.output / (role + '-summary.log')).open('w') as log:
                    summary = subprocess.run(summarize, stdout=log, stderr=subprocess.STDOUT)
                entry['summary_exit_code'] = summary.returncode
        record['results'][role] = entry
        (args.output / 'pair.json').write_text(json.dumps(record, indent=2) + '\n')
    if any(r['exit_code'] or r.get('summary_exit_code', 1) for r in record['results'].values()):
        raise SystemExit('actual pair execution/summarization failed; logs retained')


if __name__ == '__main__':
    main()

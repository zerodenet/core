#!/usr/bin/env python3
"""Materialize a standalone observer bound to an exact clean source and its lock. No Cargo."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tomllib

FEATURES = ['socks5', 'http', 'mixed', 'vless', 'hysteria2', 'shadowsocks',
            'trojan', 'vmess', 'mieru', 'wireguard', 'dns', 'host-network-stats']


def source_record(source, expected):
    actual = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    assert actual == expected, (actual, expected)
    for args in (['diff', '--exit-code'], ['diff', '--cached', '--exit-code']):
        subprocess.run(['git', '-C', str(source), *args], check=True, capture_output=True)
    data = (source / 'Cargo.lock').read_bytes()
    lock = tomllib.loads(data.decode())
    pins = {}
    for name in ('tokio', 'serde_json', 'rustls'):
        found = [p['version'] for p in lock['package'] if p['name'] == name]
        assert len(found) == 1, (name, found)
        pins[name] = found[0]
    return {'source_root': str(source), 'source_commit': actual,
            'source_lock_sha256': hashlib.sha256(data).hexdigest(),
            'exact_direct_dependency_pins': pins, 'proxy_features': FEATURES,
            'runtime_workers': 1, 'observer_tokio_tasks_spawned': 0,
            'measurement_scope': 'public proxy library; observer process also contains its own bounded Rust metrics storage; not CLI RSS or private-owner payload bytes'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path)
    parser.add_argument('--expected-commit')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    output = args.output.resolve()
    if args.verify_only:
        prior = json.loads((output / 'source.json').read_text())
        current = source_record(Path(prior['source_root']), prior['source_commit'])
        for key in ('source_commit', 'source_lock_sha256', 'exact_direct_dependency_pins'):
            assert current[key] == prior[key], key
        templates = Path(__file__).resolve().parent
        for file in (templates / 'src').glob('*.rs'):
            assert file.read_bytes() == (output / 'src' / file.name).read_bytes(), file.name
        # The owning source's resolved package set is the reference, not a
        # fresh latest-compatible registry resolution for the observer.
        owner_lock = tomllib.loads((Path(prior['source_root']) / 'Cargo.lock').read_text())
        expected = {(p['name'], p['version'], p.get('source'), p.get('checksum'))
                    for p in owner_lock['package'] if p.get('source')}
        observer_lock = tomllib.loads((output / 'Cargo.lock').read_text())
        unexpected = [p for p in observer_lock['package'] if p.get('source') and
            (p['name'], p['version'], p.get('source'), p.get('checksum')) not in expected]
        assert not unexpected, unexpected
        current['observer_lock_sha256'] = hashlib.sha256((output / 'Cargo.lock').read_bytes()).hexdigest()
        print(json.dumps(current, indent=2))
        return
    assert args.source and args.expected_commit
    source = args.source.resolve()
    record = source_record(source, args.expected_commit)
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(Path(__file__).resolve().parent / 'src', output / 'src')
    record['observer_source_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in sorted((output / 'src').glob('*.rs'))}
    (output / 'source.json').write_text(json.dumps(record, indent=2) + '\n')
    pins = record['exact_direct_dependency_pins']
    lines = ['[package]', 'name = "zero-udp-task-observer"', 'version = "0.0.0"',
             'edition = "2021"', 'publish = false', '', '[workspace]', '', '[dependencies]',
             f'serde_json = "={pins["serde_json"]}"',
             'tokio = { version = "=' + pins['tokio'] + '", features = ["macros", "rt-multi-thread", "sync", "time"] }',
             'rustls = { version = "=' + pins['rustls'] + '", default-features = false, features = ["ring", "std"] }']
    for crate in ('api', 'config', 'engine', 'proxy'):
        extra = ', default-features = false, features = ' + json.dumps(FEATURES) if crate == 'proxy' else ''
        lines.append(f'zero-{crate} = {{ path = {json.dumps(str(source / "crates" / crate))}{extra} }}')
    lines.extend(['', '[patch.crates-io]'])
    workspace = tomllib.loads((source / 'Cargo.toml').read_text())
    for name, patch in workspace['patch']['crates-io'].items():
        assert set(patch) == {'path'}, (name, patch)
        lines.append(f'{name} = {{ path = {json.dumps(str(source / patch["path"]))} }}')
    lines.extend(['', '[profile.release]', 'debug = 0', 'incremental = false', ''])
    (output / 'Cargo.toml').write_text('\n'.join(lines))
    # Standalone lock is a seed, never the source lock. The sole Cargo owner
    # resolves the added observer package offline, then builds --locked.
    shutil.copyfile(source / 'Cargo.lock', output / 'Cargo.lock')
    print(json.dumps(record, indent=2))


if __name__ == '__main__':
    main()

"""Read-only evidence verification. Does not invoke Cargo or any product binary."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--base', type=Path, required=True, help='stage or installed implementation directory')
parser.add_argument('--repo', type=Path, help='optional repository for fixed Git tree and portable source links')
parser.add_argument('--previous-stage', type=Path, help='optional unchanged through38 stage for exact inheritance check')
parser.add_argument('--originals', action='store_true', help='also read retained artifact sources and seven fixed binaries')
args = parser.parse_args()
base = args.base
sha = lambda content: hashlib.sha256(content).hexdigest()
manifest_bytes = (base / 'pr05-parallel-progress-validation.json').read_bytes()
progress_bytes = (base / 'pr05-parallel-progress.md').read_bytes()
d = json.loads(manifest_bytes)
v = json.loads((base / 'stage-verification.json').read_bytes())
assert sha(manifest_bytes) == v['summary']['manifest_sha256']
assert sha(progress_bytes) == v['summary']['progress_sha256']
assert d['latest_runtime_code_commit'] == '7a2670a4ac50e92e37758806df6d40552fc7509d'
assert d['latest_runtime_tree'] == 'de87a8e6cf751b31e6cbd4b94cfee2c51e9aa1d1'
assert len(d['records']) == 470 == v['summary']['total_records']
assert not v['summary']['overall_passed']
seen = set()
for r in d['records']:
    assert r['source'] not in seen
    seen.add(r['source'])
    compressed = (base / r['path']).read_bytes()
    content = gzip.decompress(compressed)
    assert (len(compressed), sha(compressed), len(content), sha(content)) == (
        r['gzip_bytes'], r['gzip_sha256'], r['raw_bytes'], r['raw_sha256']), r['source']
    if 'diagnostic_raw_path' in r:
        assert (base / r['diagnostic_raw_path']).read_bytes() == content, r['source']
    if args.originals and r.get('source_location', '').startswith('/'):
        assert Path(r['source_location']).read_bytes() == content, r['source']
if args.previous_stage:
    old_bytes = (args.previous_stage / 'pr05-parallel-progress-validation.json').read_bytes()
    assert sha(old_bytes) == d['through39_preparation']['previous_manifest_sha256']
    old = json.loads(old_bytes)
    current = {r['source']: r for r in d['records']}
    assert len(old['records']) == 451
    for r in old['records']:
        assert current[r['source']] == r, r['source']
        assert (base / r['path']).read_bytes() == (args.previous_stage / r['path']).read_bytes()
        if 'diagnostic_raw_path' in r:
            assert (base / r['diagnostic_raw_path']).read_bytes() == (args.previous_stage / r['diagnostic_raw_path']).read_bytes()
    assert progress_bytes.startswith((args.previous_stage / 'pr05-parallel-progress.md').read_bytes())
if args.repo:
    tree = subprocess.check_output(['git', '-C', str(args.repo), 'rev-parse', d['latest_runtime_code_commit'] + '^{tree}']).decode().strip()
    assert tree == d['latest_runtime_tree']
    portable = (base / d['wireguard_stopold_recovery_review']['portable_copy']).read_text()
    assert not re.search(r'\]\(/workspace/', portable)
    for item in next(r for r in d['records'] if r['source'] == 'wireguard-stopold-recovery-review.portable.md')['portable_source_links_verified']:
        path = args.repo / item['source_path']
        assert path.is_file(), path
        assert item['portable_target'] in portable
        line = item['original_audit_line']
        if line:
            assert 1 <= line <= len(path.read_bytes().splitlines()), (path, line)
if args.originals:
    metadata = json.loads((base / 'diagnostics/root37/binary-identities.json').read_bytes())
    for row in metadata['root37_fixed_binaries'] + metadata['prior_current31_binaries_rechecked']:
        path = Path(row['path'])
        before = path.stat()
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        after = path.stat()
        assert before.st_size == after.st_size == row['bytes']
        assert before.st_mtime_ns == after.st_mtime_ns
        assert digest == row['sha256'], path
for path in base.rglob('*'):
    if path.is_file():
        with path.open('rb') as stream:
            assert stream.read(4) != b'\x7fELF', path
print(json.dumps({'records_verified': len(d['records']), 'previous_records_byte_identical': 451 if args.previous_stage else 'not requested',
                  'frozen_head': d['latest_runtime_code_commit'], 'overall_passed': False,
                  'originals_and_seven_binaries_rehashed': args.originals,
                  'no_cargo_or_product_execution': True}, ensure_ascii=False, indent=2))

"""Read-only verification of the independent smaps/root40-41 archive; no Cargo or product execution."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--base', type=Path, default=Path(__file__).resolve().parents[1])
parser.add_argument('--originals', action='store_true', help='also rehash preserved absolute source files/binaries')
parser.add_argument('--repo', type=Path, help='also check fixed historical Git trees and unchanged through39 payload')
args = parser.parse_args()
base = args.base
sha = lambda b: hashlib.sha256(b).hexdigest()
manifest_bytes = (base/'archive-manifest.json').read_bytes()
m = json.loads(manifest_bytes)
v = json.loads((base/'archive-verification.json').read_bytes())
assert sha(manifest_bytes) == v['manifest_sha256']
assert not m['overall_qualification'] and m['independent_of_through39_records']
assert len(m['records']) == 438
seen = set()
counts = {}
for row in m['records']:
    assert row['path'] not in seen
    seen.add(row['path'])
    compressed = (base/row['path']).read_bytes()
    data = gzip.decompress(compressed)
    assert (len(compressed), sha(compressed), len(data), sha(data)) == (row['gzip_bytes'],row['gzip_sha256'],row['raw_bytes'],row['raw_sha256']), row['path']
    assert data[:4] != b'\x7fELF', row['path']
    if row.get('raw_copy'):
        assert (base/row['raw_copy']).read_bytes() == data
    if args.originals and row.get('source_location','').startswith('/'):
        assert Path(row['source_location']).read_bytes() == data, row['source_location']
    counts[row['category']] = counts.get(row['category'],0)+1
assert counts == m['category_counts']
assert counts['smaps-original-raw-diagnostic-only']==381
assert counts['smaps-derived-diagnostic-only']==12
assert counts['prepared-historical-input-not-execution']==22
if args.originals:
    for binary in m['binary_identities']:
        path=Path(binary['path']);before=path.stat()
        with path.open('rb') as stream:
            digest=hashlib.file_digest(stream,'sha256').hexdigest()
        after=path.stat()
        assert (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns)
        assert after.st_size==binary['bytes'] and digest==binary['sha256']
if args.repo:
    for n, item in m['rounds'].items():
        tree=subprocess.check_output(['git','-C',str(args.repo),'rev-parse',item['head']+'^{tree}']).decode().strip()
        assert tree==item['tree'], n
    implementation=args.repo/'design/v0.0.3/implementation'
    for name, expected in m['through39_unchanged_sentinels'].items():
        assert sha((implementation/name).read_bytes())==expected,name
    old=json.loads((implementation/'pr05-parallel-progress-validation.json').read_bytes())
    assert len(old['records'])==470
    for row in old['records']:
        compressed=(implementation/row['path']).read_bytes()
        assert sha(compressed)==row['gzip_sha256']
        assert sha(gzip.decompress(compressed))==row['raw_sha256']
for relative, expected in v['nonrecord_files'].items():
    assert sha((base/relative).read_bytes())==expected,relative
for target in re.findall(r'\]\(([^)]+)\)',(base/'README.md').read_text()):
    assert not target.startswith('/') and (base/target.split('#')[0]).is_file(),target
print(json.dumps({'records_verified':len(m['records']),'categories':counts,
                  'preserved_binaries_rehashed':len(m['binary_identities']) if args.originals else 'not requested',
                  'through39_470_records_unchanged':bool(args.repo),
                  'overall_qualification':False,'new_cargo_product_or_docker_run':False},indent=2))

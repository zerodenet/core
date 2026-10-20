"""Verify saved derived diagnostics and original digests; no workload invocation."""
from pathlib import Path
import hashlib
import json
import re

base = Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
outputs = json.loads((base/'output-digests.json').read_bytes())
for row in outputs['files']:
    content = (base/row['path']).read_bytes()
    assert (len(content), sha(content)) == (row['bytes'], row['sha256']), row['path']
inputs = json.loads((base/'input-verification.json').read_bytes())
for row in inputs['all_input_files']:
    content = Path(row['path']).read_bytes()
    assert (len(content), sha(content)) == (row['bytes'], row['sha256']), row['path']
for row in inputs['binaries_rehashed_twice']:
    path = Path(row['path'])
    before = path.stat()
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.stat()
    assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    assert after.st_size == row['bytes'] and digest == row['sha256']
memory = json.loads((base/'memory-phases.derived.json').read_bytes())
points = [p for rows in memory['roles'].values() for t in rows for p in t['points']]
assert len(points) == 66
assert sum(p['full'] for p in points) == 48
assert sum(p.get('maps_and_smaps_same_headers',False) for p in points if p['full']) == 47
assert sum(p['expected_post_exit_unavailable'] for p in points) == 6
for role in ('old','new'):
    launcher=json.loads((base/role/'launcher-metadata.derived.json').read_bytes())
    raw=Path(launcher['pair_path']).parent
    assert not (raw/role/'launcher.json').exists()
    assert 'FileNotFoundError' in (raw/role/'summary.log').read_text()
    summary=json.loads((base/role/'fanin-summary.derived.json').read_bytes())
    assert all(t['steady']=={'sent':816,'unique_received':816,'not_observed':0} for t in summary['trials'])
    assert all(t['actual_source_peer_count']==16 and not t['apply_failures'] for t in summary['trials'])
proof=json.loads((base/'elf-zerofill.derived.json').read_bytes())
assert proof['geometry']['new']['total_load_memsz_minus_filesz_bytes']-proof['geometry']['old']['total_load_memsz_minus_filesz_bytes']==544
assert len(proof['candidate_5888kib_mapping_observations'])==21
assert all(r['main_elf_exact_zerofill_overlap_bytes']==r['main_elf_zerofill_page_cover_overlap_bytes']==r['main_elf_any_load_overlap_bytes']==0 for r in proof['candidate_5888kib_mapping_observations'])
for target in re.findall(r'\]\(([^)]+)\)', (base/'report.md').read_text()):
    assert not target.startswith('/') and (base/target).is_file(),target
print(json.dumps({'derived_files_verified':len(outputs['files']),'original_output_files':inputs['original_output_files_count'],
                  'full_snapshots':48,'maps_smaps_header_matches':47,'expected_exit_unavailable':6,
                  'binary_sha_verified':2,'main_elf_zerofill_delta_bytes':544,'new_original_launcher_created':False,
                  'new_product_cargo_or_docker_run':False,'overall_qualification':False},indent=2))

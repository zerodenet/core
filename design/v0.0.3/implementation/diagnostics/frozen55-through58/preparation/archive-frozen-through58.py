"""Archive frozen55–58 originals without product execution or prior-record mutation."""
from pathlib import Path
import gzip, hashlib, json, re, subprocess
raw=Path('/workspace/core-baseline-artifacts')
repo=Path('/workspace/core')
stage=Path('/workspace/core-next/frozen55-through58-archive')
assert not stage.exists(), 'immutable stage already exists'
sha=lambda b: hashlib.sha256(b).hexdigest()
records=[]
archived=set()
rounds=[]
inputs={}
def read(p):
    b=p.read_bytes(); inputs[str(p)]=sha(b); return b
def archive(p,kind):
    if str(p) in archived: return
    archived.add(str(p))
    b=read(p); z=gzip.compress(b,mtime=0)
    rel='evidence/'+p.name+'.gz'
    (stage/rel).parent.mkdir(parents=True,exist_ok=True)
    (stage/rel).write_bytes(z)
    records.append(dict(source=str(p),path=rel,kind=kind,raw_bytes=len(b),raw_sha256=sha(b),gzip_bytes=len(z),gzip_sha256=sha(z)))
for n in list(range(55,59))+['57-preflight','57-preflight2','57-preflight3','57-preflight4']:
    p=raw/f'pr05-parallel-focused-{n}.json'
    d=json.loads(read(p))
    assert subprocess.check_output(['git','-C',str(repo),'rev-parse',d['head']+'^{tree}'],text=True).strip()==d['tree']
    assert len(d['commands'])>0
    checks=[]
    for row in d['commands']:
        if 'log' not in row:
            assert row['status']=='not_run_due_to_prior_compile_failure'
            checks.append(row); continue
        assert all(row[k] for k in ('head_unchanged','source_clean','lock_unchanged'))
        log=read(Path(row['log'])).decode()
        summaries=re.findall(r'^test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored;',log,re.M)
        counts=[sum(int(x[i]) for x in summaries) for i in range(3)] if summaries else None
        assert counts==row['counts'], (n,row['name'],counts,row['counts'])
        is_test = row['argv'][:2] == ['cargo', 'test']
        assert row['coverage_ok'] == (not is_test or bool(counts and sum(counts))), (n,row['name'])
        if is_test and not counts:
            assert row['exit'] != 0, (n,row['name'],'successful test with no aggregate')
            assert 'could not compile' in log or 'SIGABRT' in log or 'signal: 6' in log, (n,row['name'],'unknown no-aggregate cause')
        if counts:
            assert sum(counts)==row['selected_test_count']
        checks.append(dict(name=row['name'],exit=row['exit'],counts=counts,log_sha256=sha(log.encode()),coverage_ok=row['coverage_ok'],frozen_flags_verified=True))
    rounds.append(dict(number=n,head=d['head'],tree=d['tree'],lock_sha256=d['lock_sha256'],commands=checks))
    for p in sorted(raw.glob(f'pr05-parallel-focused-{n}*')):
        assert p.is_file(); archive(p,'frozen-command-evidence')
for p in [raw/'Cargo.lock-before-quinn-wintun-vendor53-cbfa65b5',raw/'pr05-parallel-lock-vendor-53.diff',raw/'pr05-parallel-resolved-metadata-vendor-53.json',Path(__file__),Path('/workspace/core-next/focused-parallel-v9.py'),Path('/workspace/core-next/focused-parallel-v10.py'),Path('/workspace/core-next/focused-parallel-v11.py'),Path('/workspace/core-next/focused-parallel-v12.py')]:
    archive(p,'lock-provenance-or-preparation-no-product-execution')
for name in ['pr05-parallel-progress-validation.json','diagnostics/current31-smaps-through41/manifest.json']:
    p=repo/'design/v0.0.3/implementation'/name
    if p.exists():
        read(p)
(stage/'manifest.json').write_text(json.dumps(dict(overall_passed=False,scope='Frozen55 through58 command evidence; no final ALL17 qualification',rounds=rounds,records=records,input_sha256=inputs),ensure_ascii=False,indent=2)+'\n')
text=['# Frozen rounds 55–58 evidence','', 'This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.','', '| Round | Source | Command | Exit | Passed / failed / ignored |','| --- | --- | --- | --- | --- |']
for r in rounds:
    for c in r['commands']:
        text.append(f"| {r['number']} | `{r['head'][:12]}` | {c['name']} | {c.get('exit',c.get('status'))} | {c.get('counts') or 'compile/static only or not run'} |")
text.extend(['','Round57 preflight1 failed formatting/module resolution before any Cargo compilation. Preflights2/3 recorded the actual non-Send future errors; preflight4 passed library compilation only. Whole round57 later failed a distinct new VMess fixture moved-value check. These results remain separate.','','Round58 actual Proxy runtime result is 751 passed / 9 failed / 6 ignored. Runtime source guard is 188 / 1 and strict platform checks failed three oversized error-return lints. No subset or compile success provides global ALL17 qualification. The old native ICMP proofs and earlier archives are unchanged.'])
(stage/'README.md').write_text('\n'.join(text)+'\n')
for row in records:
    z=(stage/row['path']).read_bytes(); b=gzip.decompress(z)
    assert (len(z),sha(z),len(b),sha(b))==(row['gzip_bytes'],row['gzip_sha256'],row['raw_bytes'],row['raw_sha256'])
for p,digest in inputs.items(): assert sha(Path(p).read_bytes())==digest,p
print(json.dumps(dict(stage=str(stage),records=len(records),rounds=len(rounds),manifest_sha256=sha((stage/'manifest.json').read_bytes()),all17_passed=False)))

#!/usr/bin/env python3
"""Prepare/print by default. Execute only after explicit parent authorization."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parent


def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def command(profile,role,output):
    return ['docker','run','--rm','--pull=never','--network=none','--cpus=1',
        '--memory=1073741824','--memory-swap=1073741824',
        '--cidfile',str(output/'container.cid'),
        '--label','org.zerodenet.diagnostic=current31-smaps',
        '--mount',f'type=bind,src={profile["binary"][role]["path"]},dst=/kernel,readonly',
        '--mount',f'type=bind,src={ROOT},dst=/probe,readonly',
        '--mount',f'type=bind,src={output},dst=/results',profile['image'],
        'python3','-B','/probe/fanin.py','--binary','/kernel','--output','/results/data',
        '--payload-bytes','1024','--trials','3','--diagnostic-role',role]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--authorization-note',default='')
    args=parser.parse_args()
    profile=json.loads((ROOT/'profile.json').read_text())
    for role,data in profile['binary'].items():
        assert digest(Path(data['path']))==data['sha256'],(role,'frozen binary changed')
    plan={'scope':'additional diagnostics only; no source/default/threshold changes',
        'status':'PREPARED_ONLY_NOT_EXECUTED' if not args.execute else 'EXPLICIT_AUTHORIZED_DIAGNOSTIC_REQUEST',
        'profile_sha256':digest(ROOT/'profile.json'),
        'commands':{role:command(profile,role,args.output.resolve()/role) for role in ('old','new')},
        'requires':'Parent must explicitly confirm CARGOFINISHED and authorize this pair; a CLI string alone is not permission'}
    if not args.execute:
        print(json.dumps(plan,indent=2));return
    if args.authorization_note!='CARGOFINISHED':
        parser.error('execution is gated pending explicit parent CARGOFINISHED authorization')
    args.output.mkdir(parents=True,exist_ok=False)
    plan['scripts_sha256']={p.name:digest(p) for p in sorted(ROOT.glob('*.py'))}
    plan['results']={}
    # Keep old->new, three trials each, as the original matched pair.
    for role in ('old','new'):
        output=args.output/role
        output.mkdir()
        started=time.monotonic_ns()
        with (output/'container.log').open('wb') as log:
            try:
                process=subprocess.run(plan['commands'][role],stdout=log,stderr=subprocess.STDOUT,
                    timeout=profile['estimated_seconds']['hard_watchdog_per_container'])
                row={'exit_code':process.returncode}
            except subprocess.TimeoutExpired as error:
                # Diagnostic global guard, not a changed product operation timeout.
                row={'exit_code':None,'diagnostic_watchdog_expired':True,'error':str(error)}
                cidfile=output/'container.cid'
                if cidfile.exists():
                    cid=cidfile.read_text().strip()
                    if len(cid)==64 and all(c in '0123456789abcdef' for c in cid):
                        # Docker client timeout alone does not stop its container.
                        # Remove only this invocation's exact container ID.
                        cleanup=subprocess.run(['docker','rm','--force',cid],capture_output=True,text=True,timeout=30)
                        row['owned_container_cleanup']={'container_id':cid,'exit_code':cleanup.returncode,
                            'stdout':cleanup.stdout,'stderr':cleanup.stderr}
                    else:row['owned_container_cleanup']={'uncertain':'invalid cidfile; no broad cleanup attempted'}
                else:row['owned_container_cleanup']={'uncertain':'no actual container ID was recorded'}
        row.update(started_ns=started,finished_ns=time.monotonic_ns())
        manifest=output/'data/manifest.json'
        if manifest.exists():
            data=json.loads(manifest.read_text())
            row['manifest_sha256']=digest(manifest)
            row['actual_binary_sha256']=data['binary_sha256']
            row['actual_build_info']=data['build_info']
            assert data['binary_sha256']==profile['binary'][role]['sha256']
            summary=[sys.executable,'-B',str(ROOT/'summarize_fanin.py'),str(output)]
            with (output/'summary.log').open('wb') as log:
                result=subprocess.run(summary,stdout=log,stderr=subprocess.STDOUT)
            row['summary_exit_code']=result.returncode
        plan['results'][role]=row
        (args.output/'pair.json').write_text(json.dumps(plan,indent=2)+'\n')
    if any(row.get('exit_code')!=0 or row.get('summary_exit_code')!=0 for row in plan['results'].values()):
        raise SystemExit('diagnostic failed/incomplete; original logs and drops retained')


if __name__=='__main__':main()

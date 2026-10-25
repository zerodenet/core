import pathlib,shutil,tempfile,hashlib,subprocess,json,time,os
out=pathlib.Path('design/v0.0.3/implementation/local-mieru-prefix-20261025').resolve()
tmp=pathlib.Path(tempfile.mkdtemp(prefix='zero-mieru-prefix-consumers-'))
source=pathlib.Path('target/debug/zero');binary=tmp/'zero';shutil.copy2(source,binary);binary.chmod(0o555)
sha=hashlib.sha256(binary.read_bytes()).hexdigest()
version=subprocess.run([str(binary),'--version'],text=True,capture_output=True,check=True).stdout.strip()
metadata={'binary':str(binary),'sha256':sha,'version':version,'source_snapshot':'source-final.json'}
(out/'consumer-binary.json').write_text(json.dumps(metadata,indent=2)+'\n')
checks={}
commands=[('gui',['python3','/tmp/zero-runtime-gui-check.py',str(binary),str(out/'gui-captures')],'/Volumes/tool/rust/zero'),('panel',['/tmp/zero-v003-boundaries.FGzrzH/zboard-zero.test','-test.run','^TestNativePublicationPreservesTrafficAcrossPublisherProcesses$','-test.v','-test.timeout','120s'],'/Volumes/tool/golang/zboard/backend')]
for name,command,cwd in commands:
 start=time.monotonic()
 env=os.environ.copy();env['ZERO_NATIVE_TEST_BINARY']=str(binary)
 with (out/(name+'.txt')).open('w') as f: result=subprocess.run(command,cwd=cwd,env=env,stdout=f,stderr=subprocess.STDOUT)
 checks[name]={'exit':result.returncode,'seconds':round(time.monotonic()-start,2),'command':command,'cwd':cwd,'sha256':sha}
 (out/'consumers.json').write_text(json.dumps(checks,indent=2)+'\n')
 print(name,checks[name],flush=True)
assert all(check['exit']==0 for check in checks.values()),checks

import importlib.util,json,sys,subprocess
from pathlib import Path
sys.dont_write_bytecode=True
binary=Path(sys.argv[1]);captures=Path(sys.argv[2]);captures.mkdir(parents=True,exist_ok=True)
spec=importlib.util.spec_from_file_location('zero_kernel_compat', '/Volumes/tool/rust/gui/scripts/check-kernel-compatibility.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
original=subprocess.Popen
processes=[]
class ObservedProcess(original):
    def __init__(self,args,*a,**kw):
        super().__init__(args,*a,**kw)
        self.observed_signals=[]
        processes.append(self)
    def terminate(self):
        self.observed_signals.append('terminate')
        return super().terminate()
    def kill(self):
        self.observed_signals.append('kill')
        return super().kill()
module.subprocess.Popen=ObservedProcess
try:
    module.check(binary, '0.0.3', captures)
finally:
    module.subprocess.Popen=original
    facts=[{'pid':p.pid,'args':list(map(str,p.args)), 'returncode':p.returncode,'signals':p.observed_signals} for p in processes]
    (captures/'process-exits.json').write_text(json.dumps(facts,indent=2)+'\n')
runtimes=[p for p in processes if 'run' in list(map(str,p.args))]
assert len(runtimes)==1
assert runtimes[0].returncode==0 and runtimes[0].observed_signals==[], facts
print('PASS actual GUI-owned runtime exited 0 after parent stdin closure; no terminate/kill')

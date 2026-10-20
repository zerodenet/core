"""Static integrity and pure text-parser checks. Never imports workload modules."""
import ast
import hashlib
import json
from pathlib import Path
import sys
from smaps_parse import parse, summarize

ROOT=Path(__file__).resolve().parent
profile=json.loads((ROOT/'profile.json').read_text())
for source in ROOT.glob('*.py'):
    compile(source.read_text(),str(source),'exec')
for name,expected in profile['baseline_script_sha256'].items():
    assert hashlib.sha256((ROOT/'originals'/name).read_bytes()).hexdigest()==expected
for name in ('peers.py','worker.py','summarize_fanin.py'):
    assert (ROOT/name).read_bytes()==(ROOT/'originals'/name).read_bytes()
# All actual payload/send/collect/overlap methods remain precisely the baseline AST.
def clients(path):
    return ast.dump(next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.ClassDef) and n.name=='Clients'))
assert clients(ROOT/'fanin.py')==clients(ROOT/'originals/fanin.py')
# Original timeout constants remain; the sole readiness deadline still starts
# from actual Popen return and includes early diagnostic overhead.
s=(ROOT/'kernel.py').read_text()
for constant in ('stream.settimeout(10)','self.process.wait(timeout=15)','self.process.wait(timeout=3)'):
    assert constant in s
for constant in ('timeout=1','ready.wait(2)','thread.join(timeout=3)','range(17)','(31, 32, 33)'):
    assert constant in (ROOT/'fanin.py').read_text()
assert 'until = popen_returned_ns / 1_000_000_000 + 20' in s
text='''1000-2000 r-xp 00000000 08:01 42 /kernel
Size:                  4 kB
Rss:                   3 kB
2000-3000 r--p 00001000 08:01 42 /kernel
Rss:                   2 kB
3000-4000 rw-p 00002000 08:01 42 /kernel
Rss:                   1 kB
5000-6000 rw-p 00000000 00:00 0 [heap]
Rss:                   4 kB
7000-8000 rw-p 00000000 00:00 0
Rss:                   5 kB
'''
binary={'inode':42,'device_major':8,'device_minor':1,'page_bytes':4096,'segments':[
 {'kind':'LOAD','vaddr':0,'offset':0,'filesz':4096},
 {'kind':'GNU_RELRO','vaddr':4096,'offset':4096,'memsz':4096}]}
result=summarize(parse(text),binary)['categories_bytes']
assert result['binary_RX']['Rss']==3072
assert result['binary_R_RELRO']['Rss']==2048
assert result['binary_RW']['Rss']==1024
assert result['brk_heap_label']['Rss']==4096
assert result['other_anonymous_not_assumed_heap']['Rss']==5120
print(json.dumps({'static_checks':'PASS','prepared_only':True,'executed_product':False,
 'executed_workload':False,'original_clients_ast_identical':True,'parser_fixture_only':True},indent=2))

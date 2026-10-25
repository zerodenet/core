import sys,re,json
from pathlib import Path
source=Path(sys.argv[1]).read_text(errors='replace')
segments=re.split(r'(?m)^\s+(?=(?:Running (?:unittests|tests/)|Doc-tests ))',source)
entries=[]
for part in segments:
    first=part.splitlines()[0] if part.splitlines() else ''
    if not first.startswith(('Running ', 'Doc-tests ')):continue
    summaries=list(re.finditer(r'(?m)^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', part))
    if summaries:
        last=summaries[-1]
        entries.append({'target':first,'status':last[1],'passed':int(last[2]),'failed':int(last[3]),'ignored':int(last[4])})
end=re.findall(r'TEST_END .*',source)
print(json.dumps({'end':end[-1] if end else None,'totals':{k:sum(t[k] for t in entries) for k in ('passed','failed','ignored')},'targets':len(entries),'entries':entries},indent=2))

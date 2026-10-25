import pathlib,re,json
raw=pathlib.Path('/tmp/zero-mieru-prefix-workspace-final-2.txt').read_text();entries=[];last=None
for part in re.split(r'(?m)^\s+(?=(?:Running (?:unittests|tests/)|Doc-tests ))',raw):
 lines=part.splitlines();name=lines[0] if lines else ''
 if not name.startswith(('Running ','Doc-tests ')):continue
 last=name;expected=re.search(r'(?m)^running (\d+) tests?$',part)
 summaries=list(re.finditer(r'(?m)^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',part))
 if not summaries:continue
 count=tuple(map(int,summaries[-1].groups()[1:]));n=sum(count)
 if expected and n!=int(expected[1]):continue
 entries.append(count)
ends=re.findall(r'TEST_END .*',raw)
print(json.dumps({'completed_targets':len(entries),'totals':dict(zip(['passed','failed','ignored'],[sum(x[i] for x in entries) for i in range(3)])),'current_or_last_target':last,'failed_cases':re.findall(r'^test (\S+) \.\.\. FAILED',raw,re.M),'end':ends[-1] if ends else None}))

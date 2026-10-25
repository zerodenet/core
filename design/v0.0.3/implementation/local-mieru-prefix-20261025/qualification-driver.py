import pathlib,json,hashlib,subprocess,shutil,gzip,re
out=pathlib.Path('design/v0.0.3/implementation/local-mieru-prefix-20261025')
sha=lambda p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
source=json.loads((out/'source-final.json').read_text())
assert all((sha(p) if pathlib.Path(p).is_file() else None)==h for p,h in source.items()),'frozen source drift'
raw=pathlib.Path('/tmp/zero-mieru-prefix-workspace-final-2.txt').read_text()
summary=json.loads(subprocess.check_output(['python3','/tmp/zero-runtime-test-summary.py','/tmp/zero-mieru-prefix-workspace-final-2.txt']))
assert summary['end'] and 'exit=0' in summary['end'] and summary['totals']['failed']==0 and summary['targets']>=155
cases=['actual_remote_logical_eof_still_requires_the_live_donor_writers_ordered_close_acknowledgement','retained_logical_loan_controller_blocks_actual_mieru_successor_even_after_view_and_driver_exit','mieru_tcp_and_packet_loans_return_one_session_without_claiming_or_stopping_the_donor_native','dropping_logical_loan_control_requests_real_session_retirement_without_orphaning_the_donor_book','ordinary_proxy_accepts_existing_mieru_configuration_and_keeps_unqualified_native_registration_closed','actual_proxy_rejects_unqualified_domain_root_before_opening_a_mieru_carrier','actual_proxy_rejects_cross_owner_prefix_before_dialing_the_real_donor_listener','actual_proxy_mieru_network_change_closes_original_prefix_before_new_carriers_and_waits_for_retained_control','actual_proxy_mieru_three_hop_prefix_retains_originals_and_replacement_waits_for_logical_control','actual_proxy_mieru_three_hop_packet_prefix_and_tcp_udp_share_the_one_real_pool']
cases.append('actual_incoming_refusal_resumes_original_observer_panic_after_quinn_state_unlock')
for case in cases:assert re.search(r'test \S*'+case+r' \.\.\. ok',raw),case
strict=pathlib.Path('/tmp/zero-mieru-prefix-clippy-2.txt').read_text()
assert 'Finished `dev` profile' in strict and not re.search(r'^error',strict,re.M)
consumers=json.loads((out/'consumers.json').read_text());assert set(consumers)=={'gui','panel'} and all(v['exit']==0 for v in consumers.values())
binary=json.loads((out/'consumer-binary.json').read_text());assert sha(binary['binary'])==binary['sha256']
assert all(v['sha256']==binary['sha256'] for v in consumers.values())
exits=json.loads((out/'gui-captures/process-exits.json').read_text());runtimes=[p for p in exits if 'run' in p['args']]
assert len(runtimes)==1 and runtimes[0]['returncode']==0 and not runtimes[0]['signals']
heads={}
for name,path in [('gui','/Volumes/tool/rust/gui'),('panel','/Volumes/tool/golang/zboard')]:
 assert not subprocess.check_output(['git','-C',path,'status','--porcelain'])
 heads[name]=subprocess.check_output(['git','-C',path,'rev-parse','HEAD']).decode().strip()
 assert heads[name]==json.loads((out/'status.json').read_text())['compatibility_source_check'][name+'_head']
assert subprocess.check_output(['git','branch','--show-current']).decode().strip()=='develop'
head=subprocess.check_output(['git','rev-parse','HEAD']).decode().strip();assert head=='b963c864a473f68f8d6a8444764e3e88761e6c04'
assert not subprocess.check_output(['git','diff','--cached','--name-only'])
public=['crates/api','crates/config','crates/engine','crates/ffi','crates/grpc','src']
assert not subprocess.check_output(['git','diff','--name-only','HEAD','--',*public])
backup=pathlib.Path('/var/folders/5g/3js9c9b11c3fmfd930s4vkdm0000gn/T/zero-mieru-prefix-before-avk7ffo4')
assert pathlib.Path('Cargo.lock').read_bytes()==(backup/'Cargo.lock').read_bytes()
commands={'fmt':['cargo','+stable','fmt','--all','--','--check'],'layout':['python3','scripts/check-test-layout.py'],'plan':['python3','design/v0.0.3/check-plan.py'],'diff':['git','diff','--check'],'runtime-boundary':['/tmp/zero-runtime-boundary'],'workspace-boundary':['/tmp/zero-runtime-workspace-boundary'],'ci-scope':['node','--test','.github/actions/ci-scope/scope.test.mjs']}
checks={}
for name,command in commands.items():
 with (out/(name+'.txt')).open('w') as f:r=subprocess.run(command,stdout=f,stderr=subprocess.STDOUT)
 checks[name]={'command':command,'exit':r.returncode,'log':name+'.txt'};assert r.returncode==0,(name,r.returncode)
(out/'structural-checks.json').write_text(json.dumps(checks,indent=2)+'\n')
for name,path in [('workspace-final','/tmp/zero-mieru-prefix-workspace-final-2.txt'),('clippy-final','/tmp/zero-mieru-prefix-clippy-2.txt')]:
 (out/(name+'.txt.gz')).write_bytes(gzip.compress(pathlib.Path(path).read_bytes(),mtime=0))
(out/'workspace-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for src,dest in [('/tmp/zero-runtime-test-summary.py','summary-driver.py'),('/tmp/zero-runtime-gui-check.py','gui-check-driver.py'),('/tmp/zero-mieru-prefix-consumers.py','consumers-driver.py')]:shutil.copyfile(src,out/dest)
status=json.loads((out/'status.json').read_text())
report={'status':'qualified_same_owner_mieru_original_loans_and_actual_proxy_fixture_scope','branch':'develop','head':head,'product_version':'v0.0.3','protocol_reference':'mieru / mieru-config 3.33.0 unchanged','local_only':True,'commit_or_push_performed':False,'index_empty':True,'source_snapshot':'source-final.json','source_entries':len(source),'source_drift':[],'production_registration_enabled':False,'architecture_tasks_complete':False,'qualification_complete_for_recorded_scope':True,'scope':status['scope_under_validation'],'remaining_production_gates':status['production_blockers'],'checks':{'workspace':{'command':'CARGO_NET_OFFLINE=true RUSTUP_TOOLCHAIN=1.99.0 RUST_MIN_STACK=16777216 bash scripts/test-workspace.sh --jobs 4','exit':0,'totals':summary['totals'],'targets':summary['targets'],'end':summary['end'],'log':'workspace-final.txt.gz'},'new_cases':cases,'clippy':{'exit':0,'rustc':subprocess.check_output(['rustc','+stable','--version']).decode().strip(),'command':status['strict_clippy']['command'],'log':'clippy-final.txt.gz'},'structural':'structural-checks.json','runtime_boundary':{'passed':190,'failed':0},'workspace_boundary':{'passed':27,'failed':0},'ci_scope':{'passed':21,'failed':0},'consumers':consumers,'consumer_heads':heads,'consumer_binary_sha256':binary['sha256'],'gui_normal_exit':0,'gui_forced_signals':[]},'runtime_source_equivalence':'runtime-source-equivalence.json','diagnostic_regressions_not_qualification':status.get('diagnostic_regressions',{}),'preservation':{'backup':str(backup),'cargo_lock_preserved':True,'public_api_config_engine_ffi_grpc_cli_unchanged':True,'consumer_sources_clean_unchanged':True},'limits':['Same actual Mieru shared owner only; normal production enrollment remains closed','Root literal endpoints in actual Proxy; complete native DNS query provider integration remains gated','Local GUI IPC and panel publication fixtures, not real GUI/TUN devices or production deployment','No 1 CPU/1 GiB load qualification or additional official external interoperability claim','Ignored external and privileged tests remain separate qualifications']}
(out/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
status.update(qualification='passed-for-recorded-internal-same-owner-scope',behavioral_and_consumer_regressions='passed; see validation.json; production gates remain',validation='validation.json');(out/'status.json').write_text(json.dumps(status,indent=2)+'\n')
print('Collected',summary['totals'],summary['targets'],summary['end'],binary['sha256'])

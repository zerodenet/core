//! Production acknowledged lookup/trace keep actual blocking and Query joins.
use crate::{Proxy, ProxyHandle};
use tokio::time::{timeout, Duration};
use zero_api::CommandService;
async fn actual_cancel(trace: bool) {
    timeout(Duration::from_secs(10),async{
  let wire=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
   "runtime":{"native_dns_max_originals":2,"dns":{"servers":{"wire":{"type":"udp","host":"127.0.0.1","port":wire.local_addr().unwrap().port()}},"default_server":"wire"}},
   "route":{"rules":[{"condition":{"type":"ip","values":["127.0.0.0/8"]},"action":{"type":"direct"}}],"final":{"type":"reject"}}
  }).to_string()).unwrap();
  let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
  let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
  handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
  let command=if trace{zero_api::CommandRequest::DiagnosticsTraceRoute(zero_api::DiagnosticsTraceRouteCommand{target:"trace.actual.example".into(),port:80,protocol:Some("tcp".into()),inbound_tag:None})}
   else{zero_api::CommandRequest::DiagnosticsDnsLookup(zero_api::DiagnosticsDnsLookupCommand{hostname:"lookup.actual.example".into()})};
  let observation=zero_runtime::ApplyObservation::new();let observed=observation.clone();
  let stop=zero_platform_tokio::contract::CancellationToken::new();let stopping=stop.clone();
  let request=tokio::spawn(async move{observed.scope_with_stop(stopping,handle.execute_acknowledged(command)).await});
  let mut types=Vec::new();
  for _ in 0..2{let mut bytes=[0;512];let(size,_)=wire.recv_from(&mut bytes).await.unwrap();let question=zero_dns::udp::parse_dns_question(&bytes[..size]).unwrap();
   assert_eq!(question.domain,if trace{"trace.actual.example"}else{"lookup.actual.example"});
   types.push(question.query_type);}
  types.sort_unstable();assert_eq!(types,[1,28]);assert!(!request.is_finished());
  let originals=loop{let originals=proxy.resolver.native_query_observations().into_iter().filter(|entry|entry.io_pending&&entry.original.is_some()).collect::<Vec<_>>();if originals.len()==2{break originals;}tokio::task::yield_now().await;};
  for original in &originals{assert!(!original.task.is_joined());assert!(original.original.as_ref().unwrap().closed().is_none());}
  stop.cancel();assert!(request.await.unwrap().is_err());
  assert!(observation.wait_joined().await.is_none(),"no Apply was accepted in the already running Host");
  assert!(observation.blocking_outcome().unwrap().failure().is_none(),"same real raw blocking job returned; not a fabricated Scope receipt");
  assert!(observation.completion_fault(None).is_none(),"ordinary caller cancellation is not an ownership fault");
  for original in &originals{
   let native=original.original.as_ref().unwrap();let closed=native.closed().unwrap();assert!(closed.matches(native)&&closed.had_native_acquisition());
   let joined=original.task.try_joined().unwrap();assert!(original.task.matches_joined(&joined)&&joined.registration_released());
  }
  assert!(proxy.engine().active_sessions().is_empty());
  running.shutdown().await.unwrap();
  for original in &originals{let native=original.original.as_ref().unwrap();assert!(native.owner().closed().unwrap().matches(native.owner()));assert!(native.owner().users_closed().unwrap().matches(native.owner()));}
  assert_eq!(proxy.host.original_budgets().carrier.snapshot().charged,0);
 }).await.unwrap();
}
#[tokio::test]
async fn acknowledged_lookup_cancellation_waits_same_raw_job_and_both_original_query_receipts() {
    actual_cancel(false).await;
}
#[tokio::test]
async fn acknowledged_trace_cancellation_waits_same_raw_job_and_both_original_query_receipts() {
    actual_cancel(true).await;
}

//! Production acknowledged lookup keeps the real async command and Query joins.
use crate::{Proxy, ProxyHandle};
use tokio::time::{timeout, Duration};
use zero_api::CommandService;
async fn actual_cancel() {
    timeout(Duration::from_secs(10),async{
  let wire=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
   "runtime":{"native_dns_max_originals":2,"dns":{"servers":{"wire":{"type":"udp","host":"127.0.0.1","port":wire.local_addr().unwrap().port()}},"default_server":"wire"}},
   "route":{"rules":[{"condition":{"type":"ip","values":["127.0.0.0/8"]},"action":{"type":"direct"}}],"final":{"type":"reject"}}
  }).to_string()).unwrap();
  let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
  let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
  handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
  let command=zero_api::CommandRequest::DiagnosticsDnsLookup(zero_api::DiagnosticsDnsLookupCommand{hostname:"lookup.actual.example".into()});
  let request=tokio::spawn(async move {handle.execute_acknowledged(command).await});
  let mut types=Vec::new();
  for _ in 0..2{let mut bytes=[0;512];let(size,_)=wire.recv_from(&mut bytes).await.unwrap();let question=zero_dns::udp::parse_dns_question(&bytes[..size]).unwrap();
   assert_eq!(question.domain,"lookup.actual.example");
   types.push(question.query_type);}
  types.sort_unstable();assert_eq!(types,[1,28]);assert!(!request.is_finished());
  let originals=loop{let originals=proxy.resolver.native_query_observations().into_iter().filter(|entry|entry.io_pending&&entry.original.is_some()).collect::<Vec<_>>();if originals.len()==2{break originals;}tokio::task::yield_now().await;};
  for original in &originals{assert!(!original.task.is_joined());assert!(original.original.as_ref().unwrap().closed().is_none());}
  assert_eq!(proxy.application_control_budget().usage().pending_commands,1);
  request.abort(); assert!(request.await.unwrap_err().is_cancelled());
  proxy.protocols.drive_component_supervision(proxy.embedded_command_owner().shutdown(tokio::time::Instant::now()+Duration::from_secs(5))).await.unwrap();
  assert!(proxy.embedded_command_owner().retirement_complete());
  assert_eq!(proxy.application_control_budget().usage().pending_commands,0);
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
async fn embedded_lookup_caller_drop_retains_same_async_command_and_both_actual_query_originals() {
    actual_cancel().await;
}

#[path = "tests/target.rs"]
mod target;

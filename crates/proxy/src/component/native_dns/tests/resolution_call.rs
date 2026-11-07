//! Both Default DNS originals belong to their canonical Host, not this caller.
use crate::Proxy;
use std::time::Duration;
use zero_traits::DnsResolver;
#[tokio::test]
async fn same_resolution_call_waits_both_actual_family_native_users_scope_and_host_ack() {
    tokio::time::timeout(Duration::from_secs(10),async{
 let peer=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
 let config=zero_config::RuntimeConfig::parse(&serde_json::json!({"runtime":{"native_dns_max_originals":2,"dns":{"servers":{"wire":{"type":"udp","host":"127.0.0.1","port":peer.local_addr().unwrap().port()}},"default_server":"wire"}},"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
 let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
 let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
 handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
 let call=proxy.resolver.resolution_call();let work=call.scope(proxy.resolver.resolve("dual.actual.example"));tokio::pin!(work);
 let mut types=Vec::new();
 for _ in 0..2{
 let mut bytes=[0;512];let (size,_)=tokio::select!{packet=peer.recv_from(&mut bytes)=>packet.unwrap(),result=&mut work=>panic!("real dual family lookup finished before upstream response: {result:?}")};
 let question=zero_dns::udp::parse_dns_question(&bytes[..size]).unwrap();
 assert_eq!(question.domain,"dual.actual.example");
 types.push(question.query_type);
 }
 types.sort_unstable();assert_eq!(types,[1,28]);
 let originals=loop {
 let actual=proxy.resolver.native_query_observations().into_iter().filter(|entry|entry.io_pending&&entry.original.is_some()).collect::<Vec<_>>();
 if actual.len()==2{break actual;}tokio::task::yield_now().await;
 };
 for entry in &originals{assert!(!entry.task.is_joined());assert!(entry.original.as_ref().unwrap().closed().is_none());}
 assert!(call.closed().is_none());call.cancel();assert_eq!(work.await.unwrap_err().kind(),std::io::ErrorKind::Interrupted);
 let end=tokio::time::Instant::now()+Duration::from_secs(3);
 let receipt=call.wait_closed(end).await.unwrap();assert!(receipt.matches(&call));
 assert_eq!(receipt.queries().iter().filter(|q|q.query().is_some()).count(),2);
 for entry in &originals{
 let native=entry.original.as_ref().unwrap();let closed=native.closed().expect("same actual DNS native original closed");assert!(closed.matches(native) && closed.had_native_acquisition());
 let joined=entry.task.try_joined().expect("same query joined");assert!(entry.task.matches_joined(&joined)&&joined.registration_released());
 assert!(receipt.queries().iter().any(|q|q.query().is_some_and(|w|w.task_id()==entry.task.task_id()&&w.scope_identity().same_scope(&entry.task.scope_identity()))));
 }
 running.shutdown().await.unwrap();
 for entry in &originals {
 let native=entry.original.as_ref().unwrap();
 assert!(native.owner().closed().unwrap().matches(native.owner()));
 assert!(native.owner().users_closed().unwrap().matches(native.owner()));
 }
 assert_eq!(proxy.host.original_budgets().carrier.snapshot().charged,0);
 }).await.unwrap();
}

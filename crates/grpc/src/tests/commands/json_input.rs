use super::*;
#[tokio::test]
async fn actual_wire_json_metadata_stays_with_pending_command_after_disconnect_until_real_ack() {
    tokio::time::timeout(Duration::from_secs(5),async{
  let budget=budget();let service=Commands::default();
  let owner=crate::prepare_with_budget("127.0.0.1:0".parse().unwrap(),GrpcServerSecurity::default(),budget.clone()).unwrap();
  owner.start(service.clone()).await.unwrap();let mut client=client(&owner).await;
  let payload=br#"{"params":{"config":{"many":[{},{},{},{},{},{},{},{}] }},"method":"config.validate"}"#.to_vec();
  let request=tonic::Request::new(ExecuteRequest{method:"config.validate".into(),payload:payload.clone()});
  let mut call=Box::pin(client.unary::<_,ExecuteResponse,_>(request,tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Execute"),tonic::codec::ProstCodec::default()));
  tokio::select!{_=service.pending.notified()=>{},r=&mut call=>panic!("actual command returned before final ACK: {r:?}")}
  let held=budget.usage().request_bytes;
  assert!(held>8*1024+payload.len()*4+4096,"JSON tree/Content rows exceed original protobuf input charge");
  assert_eq!(budget.usage().pending_commands,1);drop(call);drop(client);
  let mut shutdown=Box::pin(owner.shutdown());poll_fn(|cx|{assert!(shutdown.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
  assert_eq!(budget.usage().request_bytes,held);
  let mut shrink=budget.limits();shrink.messages.request_bytes_total=held-1;assert!(budget.prepare(shrink).is_err());
  service.release.notify_one();shutdown.await.unwrap();assert!(owner.retirement_complete());
  budget.command_owner().shutdown(crate::native::deadline()).await.unwrap();
  assert_eq!(budget.usage().request_bytes,0);budget.close_admission();budget.confirm_released().unwrap();
 }).await.unwrap();
}

//! Actual controlled TCP holds native flush for DATA and empty END separately.
use super::*;
use std::future::Future;
#[tokio::test]
async fn actual_grpc_data_and_empty_end_wait_same_native_flush_before_application_completion() {
    timeout(Duration::from_secs(8),async {
  for multi_mode in [false,true] {
   let(client,server,cleanup)=crate::native_flush::test_fixture::pair().await;
   let gate=client.gate.clone();
   let profile=OwnedGrpcProfile{service_names:vec!["actual.flush/Tun".into()],multi_mode,..Default::default()};
   let peer_profile=profile.clone();
   let(stop,stopped)=tokio::sync::oneshot::channel();
   let peer=tokio::spawn(async move {
    let incoming=accept_grpc_connection(server,&peer_profile).unwrap();
    let mut io=incoming.accept().await.unwrap();let mut body=Vec::new();
    io.read_to_end(&mut body).await.unwrap();assert_eq!(body,b"actual DATA, not queued delivery");
    AsyncWriteExt::write_all(&mut io,b"actual reverse tail").await.unwrap();
    AsyncWriteExt::shutdown(&mut io).await.unwrap();stopped.await.unwrap();
    drop(io);incoming.close();
   });
   let(mut io,driver)=PreparedSingleClient::prepare(client,&profile,"localhost",None).await.unwrap().into_parts();
   let driver=tokio::spawn(driver);
   gate.hold();
   AsyncWriteExt::write_all(&mut io,b"actual DATA, not queued delivery").await.unwrap();
   {
    let flushing=AsyncWriteExt::flush(&mut io);tokio::pin!(flushing);
    tokio::select!{biased;_=gate.blocked()=>{},result=&mut flushing=>panic!("DATA completed before its original native flush: {result:?}")}
    std::future::poll_fn(|cx|{assert!(flushing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
    gate.release();flushing.await.unwrap();
   }
   gate.hold();
   {
    let closing=AsyncWriteExt::shutdown(&mut io);tokio::pin!(closing);
    tokio::select!{biased;_=gate.blocked()=>{},result=&mut closing=>panic!("empty END completed before its original native flush: {result:?}")}
    std::future::poll_fn(|cx|{assert!(closing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
    gate.release();closing.await.unwrap();
   }
   let mut tail=Vec::new();io.read_to_end(&mut tail).await.unwrap();assert_eq!(tail,b"actual reverse tail");
   drop(io);stop.send(()).unwrap();peer.await.unwrap();
   // This explicit library driver task is actually joined. It supplies no
   // Component Scope/Host ACK receipt and does not stand in for native close.
   driver.abort();match driver.await{Ok(result)=>result.unwrap(),Err(error)=>assert!(error.is_cancelled())}
   cleanup.await;
  }
 }).await.expect("original eight-second native-flush and reverse EOF bound");
}

//! Both public H2 client entrypoints retain DATA/END through native flush.
use super::*;
use std::future::Future;
#[tokio::test]
async fn actual_h2_data_and_empty_end_wait_for_original_native_flush_before_reverse_eof() {
    tokio::time::timeout(std::time::Duration::from_secs(8),async {
  let(client,server,cleanup)=crate::native_flush::test_fixture::pair().await;
  let endpoint=client.io.peer_addr().unwrap();
  let gate=client.gate.clone();let(stop,stopped)=tokio::sync::oneshot::channel();
  let peer=tokio::spawn(async move {
   let mut connection=h2::server::handshake(server).await.unwrap();
   let(request,mut respond)=connection.accept().await.unwrap().unwrap();
   let mut send=respond.send_response(Response::new(()),false).unwrap();
   let operation=async move {
    let mut body=request.into_body();let mut bytes=Vec::new();
    while let Some(chunk)=body.data().await {let chunk=chunk.unwrap();body.flow_control().release_capacity(chunk.len()).unwrap();bytes.extend_from_slice(&chunk);}
    assert_eq!(bytes,b"actual DATA, not queued delivery");send.send_data(Bytes::from_static(b"actual reverse tail"),true).unwrap();stopped.await.unwrap();
   };
   tokio::pin!(operation);
   tokio::select!{_= &mut operation=>{},incoming=connection.accept()=>panic!("only the original H2 stream: {}",incoming.is_some())}
  });
  let profile=crate::profile::OwnedH2Profile{host:Some("actual.flush".into()),path:"/original".into()};
  let mut io=connect_h2(client,&profile,&endpoint.ip().to_string(),endpoint.port()).await.unwrap();
  gate.hold();AsyncWriteExt::write_all(&mut io,b"actual DATA, not queued delivery").await.unwrap();
  {
   let flushing=AsyncWriteExt::flush(&mut io);tokio::pin!(flushing);
   tokio::select!{biased;_=gate.blocked()=>{},result=&mut flushing=>panic!("H2 DATA completed before native flush: {result:?}")}
   std::future::poll_fn(|cx|{assert!(flushing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
   gate.release();flushing.await.unwrap();
  }
  gate.hold();
  {
   let closing=AsyncWriteExt::shutdown(&mut io);tokio::pin!(closing);
   tokio::select!{biased;_=gate.blocked()=>{},result=&mut closing=>panic!("H2 END completed before native flush: {result:?}")}
   std::future::poll_fn(|cx|{assert!(closing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
   gate.release();closing.await.unwrap();
  }
  let mut bytes=Vec::new();io.read_to_end(&mut bytes).await.unwrap();assert_eq!(bytes,b"actual reverse tail");
  stop.send(()).unwrap();peer.await.unwrap();drop(io);cleanup.await;
 }).await.expect("same eight-second native-flush/half-close bound");
}

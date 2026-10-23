use super::*;
use std::{future::Future, task::Poll};
#[tokio::test]
async fn actual_remote_h2_stream_limit_bounds_pending_demands_and_cancellation_preserves_shared_carrier(
) {
    timeout(Duration::from_secs(7),async {
  let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();let (finish,finished)=tokio::sync::oneshot::channel();
  let server=tokio::spawn(async move {
   let (io,_)=listener.accept().await.unwrap();let mut builder=h2::server::Builder::new();builder.max_concurrent_streams(1);let mut connection=builder.handshake(io).await.unwrap();let (request,mut respond)=connection.accept().await.unwrap().unwrap();
   let send=respond.send_response(http::Response::builder().status(200).header("content-type","application/grpc").body(()).unwrap(),false).unwrap();
   let mut io=crate::grpc::build_grpc_stream(send,request.into_body(),true).unwrap();
   let done=Arc::new(AtomicUsize::new(0));let app=done.clone();
   let operation=async move {for expected in [b"live",b"kept"] {let mut bytes=[0;4];io.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,expected);io.write_all(&bytes).await.unwrap();io.flush().await.unwrap();app.fetch_add(1,Ordering::SeqCst);}finished.await.unwrap();};tokio::pin!(operation);
   tokio::select! {
    _=&mut operation=>{},
    unexpected=connection.accept()=>{assert_eq!(done.load(Ordering::SeqCst),2,"a second HTTP/2 request cannot pass the real peer limit before the first finishes");drop(unexpected);operation.await;},
   }
  });
  let mut fixture=Fixture::cold(peer,1,2,None,None);fixture.start().await;let managed=fixture.managed();
  let mut first=fixture.options.open(fixture.actual,fixture.demand()).await.unwrap();first.write_all(b"live").await.unwrap();first.flush().await.unwrap();let mut bytes=[0;4];first.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"live");
  let demand=fixture.demand();let cancel=demand.cancel.clone();
  let mut second=fixture.options.open(fixture.actual,demand).await.unwrap();
  second.write_all(b"queued").await.unwrap();
  let mut flushing=Box::pin(second.flush());
  // A cloned h2 SendRequest can enqueue HEADERS before remote admission.
  // Actual received SETTINGS(max=1) prevents capacity for this stream's DATA;
  // its real write/flush remains pending while the first stream stays live.
  std::future::poll_fn(|cx|{assert!(flushing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
  assert_eq!(managed.budget.retained(),2);
  assert_eq!(fixture.options.open(fixture.actual,fixture.demand()).await.err().unwrap().reason,Reason::Capacity);
  cancel.cancel();
  std::future::poll_fn(|cx|{assert!(flushing.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
  drop(flushing);drop(second);
  assert_eq!(managed.budget.retained(),2,"dropping a stream cannot release its still-pending real worker");
  first.write_all(b"kept").await.unwrap();first.flush().await.unwrap();first.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"kept");assert_eq!(fixture.traffic.boundaries.load(Ordering::SeqCst),1);drop(first);
  let end=deadline();let (protocol,tasks)=tokio::join!(fixture.owner.lifecycle.shutdown(end),fixture.scope.shutdown(end));let report=protocol.unwrap();assert!(report.is_complete(),"{report:?}");assert_eq!(report.closed_io.len(),1);assert!(tasks.is_complete());assert_eq!(tasks.exited_tasks.len(),1);finish.send(()).unwrap();server.await.unwrap();
 }).await.unwrap();
}

use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct Withheld {
    io: tokio::net::TcpStream,
    pending: Arc<tokio::sync::Notify>,
}
impl AsyncRead for Withheld {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for Withheld {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.pending.notify_one();
        Poll::Pending
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn pending_real_vmess_tls_cancel_keeps_original_control_until_close_without_fabricating_task_exit(
) {
    timeout(Duration::from_secs(6),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();
        let directory=Directory::new();let (acceptor,pem)=tls_server(true);std::fs::write(directory.0.join("identity.pem"),&pem).unwrap();
        let pending=Arc::new(tokio::sync::Notify::new());let signal=pending.clone();let (finish,mut finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (io,_)=listener.accept().await.unwrap();
            let handshake=acceptor.accept(Withheld{io,pending:signal});tokio::pin!(handshake);
            tokio::select! {
                result=&mut handshake=>{assert!(result.is_err());finished.await.unwrap();},
                _=&mut finished=>{},
            }
        });
        let runtime=vmess::transport::VmessTransportRuntime::default();let mut tls=tls_profile();tls.alpn=vec!["h2".into()];
        let mut fixture=Fixture::cold(peer,&runtime,Some(&tls),&profile(false),Some(&directory.0),(pem.len(),1),None);fixture.start().await;
        let context=fixture.context();let cancel=context.cancel.clone();
        {
            let opening=fixture.owner.roles.stream.as_ref().unwrap().connect(ConnectRequest{destination:destination()},context);tokio::pin!(opening);
            tokio::select! {result=opening.as_mut()=>panic!("real peer TLS response is pending: {}",result.err().unwrap()),_=pending.notified()=>{}}
            std::future::poll_fn(|cx|{assert!(opening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            cancel.cancel();let failure=opening.await.err().unwrap();
            assert_eq!(failure.reason,Reason::Cancelled);assert_eq!(failure.stage,Stage::Handshake);assert_eq!(failure.delivery,DeliveryState::Unknown);
        }
        // No original H2 driver was prepared/registered, but the actual socket
        // is still owned and must receive its own physical close confirmation.
        let (report,tasks)=fixture.close().await;
        assert_eq!(report.closed_io.len(),1);assert!(tasks.exited_tasks.is_empty());
        assert_eq!(fixture.observations.file_reads.load(Ordering::Relaxed),1);
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}
fn original_h2<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a h2::Error> {
    if let Some(source) = error.downcast_ref::<h2::Error>() {
        return Some(source);
    }
    if let Some(source) = error
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
        .and_then(|source| original_h2(source))
    {
        return Some(source);
    }
    error.source().and_then(original_h2)
}
#[tokio::test]
async fn controlled_vmess_grpc_logical_reset_preserves_original_h2_source_without_false_peer_confirmation(
) {
    timeout(Duration::from_secs(6),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();
        let (finish,mut finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (io,_)=listener.accept().await.unwrap();let mut connection=h2::server::handshake(io).await.unwrap();
            let (_request,mut respond)=connection.accept().await.unwrap().unwrap();
            respond.send_reset(h2::Reason::REFUSED_STREAM);
            // Keep driving the real RST frame; no socket-close shortcut.
            tokio::select! {
                _=&mut finished=>{},
                _=connection.accept()=>{finished.await.unwrap();},
            }
        });
        let runtime=vmess::transport::VmessTransportRuntime::default();
        let mut fixture=Fixture::cold(peer,&runtime,None,&profile(false),None,(0,1),None);fixture.start().await;
        let result=fixture.owner.roles.stream.as_ref().unwrap().connect(ConnectRequest{destination:destination()},fixture.context()).await;
        let mut joined=false;
        match result {
            Err(error)=>assert_eq!(original_h2(&error).unwrap().reason(),Some(h2::Reason::REFUSED_STREAM)),
            Ok(mut connected)=>{
                let mut byte=[0];
                let failure={
                    let reading=connected.io.read(&mut byte);tokio::pin!(reading);
                    loop {tokio::select! {
                        result=reading.as_mut()=>break result.unwrap_err(),
                        exit=fixture.scope.next_exit(),if !joined=>{assert!(exit.is_some());joined=true;}
                    }}
                };
                assert_eq!(original_h2(&failure).unwrap().reason(),Some(h2::Reason::REFUSED_STREAM));
                drop(connected);
            }
        }
        assert!(!fixture.observations.facts.lock().unwrap().iter().any(|(_,fact)|*fact==ConnectFact::PeerConfirmed));
        let (report,tasks)=fixture.close().await;assert_eq!(report.closed_io.len(),1);
        assert_eq!(tasks.exited_tasks.len(),usize::from(!joined));
        finish.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}

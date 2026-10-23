use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
fn profile(insecure: bool) -> OwnedClientTlsProfile {
    OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: None,
        insecure,
        alpn: vec!["h2".into()],
        client_fingerprint: None,
    }
}
fn acceptor() -> tokio_rustls::TlsAcceptor {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![certificate.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
    )
    .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}
#[tokio::test]
async fn original_tls_authentication_rejects_untrusted_peer_and_closes_without_fake_driver_join() {
    timeout(Duration::from_secs(6), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let tls = acceptor();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            assert!(tls.accept(io).await.is_err());
        });
        let mut fixture = Fixture::cold(peer, 1, 2, Some(profile(false)), None);
        fixture.start().await;
        let failure = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .err()
            .unwrap();
        assert_eq!(failure.stage, Stage::Handshake);
        assert!(std::error::Error::source(&failure).is_some());
        let record = fixture.managed().records.lock().unwrap()[0].clone();
        assert!(record.witness.lock().unwrap().is_none());
        assert!(record.adoption.closed().is_none());
        let (protocol, tasks) = fixture.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert_eq!(protocol.closed_io.len(), 1);
        assert!(tasks.exited_tasks.is_empty());
        assert!(record.closed());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn original_tls_h2_wire_uses_one_owned_driver_and_retains_half_close() {
    timeout(Duration::from_secs(6), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let tls = acceptor();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let io = tls.accept(io).await.unwrap();
            assert_eq!(io.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
            let incoming = crate::grpc::accept_grpc_connection(
                io,
                &crate::profile::OwnedGrpcProfile {
                    service_names: vec!["owned".into()],
                    ..Default::default()
                },
            )
            .unwrap();
            let mut io = incoming.accept().await.unwrap();
            let mut bytes = Vec::new();
            io.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"original-TLS-and-grpc");
            io.write_all(&bytes).await.unwrap();
            io.shutdown().await.unwrap();
            finished.await.unwrap();
            incoming.close();
        });
        let mut fixture = Fixture::cold(peer, 1, 2, Some(profile(true)), None);
        fixture.start().await;
        let mut io = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        io.write_all(b"original-TLS-and-grpc").await.unwrap();
        io.shutdown().await.unwrap();
        let mut bytes = Vec::new();
        io.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"original-TLS-and-grpc");
        drop(io);
        let (protocol, tasks) = fixture.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert_eq!(protocol.closed_io.len(), 1);
        assert!(protocol.exited_tasks.is_empty());
        assert!(tasks.is_complete());
        assert_eq!(tasks.exited_tasks.len(), 1);
        assert!(tasks.failed_tasks.is_empty());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
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
async fn cancellation_at_actual_pending_tls_response_retains_original_control_until_close_without_task_id(
) {
    timeout(Duration::from_secs(6),async {
  let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();let tls=acceptor();let observed=Arc::new(tokio::sync::Notify::new());let signal=observed.clone();let (finish,finished)=tokio::sync::oneshot::channel();
  let server=tokio::spawn(async move {let (io,_)=listener.accept().await.unwrap();let handshake=tls.accept(Withheld{io,pending:signal});tokio::pin!(handshake);tokio::select!{_=&mut handshake=>panic!("real server TLS response is withheld"),_=finished=>{}}});
  let mut fixture=Fixture::cold(peer,1,2,Some(profile(true)),None);fixture.start().await;let managed=fixture.managed();let demand=fixture.demand();let cancel=demand.cancel.clone();
  let record={
  let opening=fixture.options.open(fixture.actual,demand);tokio::pin!(opening);
  tokio::select!{_=opening.as_mut()=>panic!("opening is pending on actual peer TLS response"),_=observed.notified()=>{}}
  std::future::poll_fn(|cx|{assert!(opening.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
  let record=managed.records.lock().unwrap()[0].clone();assert!(record.adoption.closed().is_none());assert!(record.witness.lock().unwrap().is_none());cancel.cancel();let failure=opening.as_mut().await.err().unwrap();assert_eq!(failure.reason,Reason::Cancelled);
  record
  };
  assert_eq!(managed.grants.reserve().err().unwrap().reason,Reason::Capacity);assert!(record.adoption.closed().is_none());
  let end=deadline();let (protocol,tasks)=tokio::join!(fixture.owner.lifecycle.shutdown(end),fixture.scope.shutdown(end));let report=protocol.unwrap();assert!(report.is_complete(),"{report:?}");assert_eq!(report.closed_io.len(),1);assert!(tasks.exited_tasks.is_empty());assert!(record.closed());assert!(record.adoption.closed().unwrap().driver_exit().is_none());finish.send(()).unwrap();server.await.unwrap();
 }).await.unwrap();
}

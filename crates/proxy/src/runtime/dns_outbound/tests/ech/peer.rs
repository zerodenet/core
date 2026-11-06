//! A real TLS/H2 endpoint drives both address and HTTPS questions on one wire.
use super::*;
use std::{
    io::{self, Cursor},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot};
pub(super) struct Peer {
    pub(super) endpoint: std::net::SocketAddr,
    pub(super) ca: rustls::pki_types::CertificateDer<'static>,
    pub(super) accepted: Arc<AtomicUsize>,
    pub(super) requests: mpsc::Receiver<String>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}
impl Peer {
    pub(super) async fn start() -> Self {
        Self::with_security(true).await
    }
    pub(super) async fn cleartext() -> Self {
        Self::with_security(false).await
    }
    async fn with_security(secure: bool) -> Self {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let ca = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![ca.clone()], key.into())
        .unwrap();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let tls = secure.then(|| tokio_rustls::TlsAcceptor::from(Arc::new(tls)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let (request, requests) = mpsc::channel(8);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            loop {
                tokio::select! { biased;
                    _ = &mut stopped => break,
                    connection = listener.accept() => {
                        let (io, _) = connection?;
                        count.fetch_add(1, Ordering::AcqRel);
                        let tls = tls.clone(); let request = request.clone();
                        peers.spawn(async move {
                            let result = match tls { Some(tls) => serve(tls.accept(io).await?, request).await, None => serve(io, request).await };
                            match result {
                                Err(source) if matches!(source.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof) => Ok(()),
                                other => other,
                            }
                        });
                    }
                    joined = peers.join_next(), if !peers.is_empty() => { joined.unwrap().map_err(io::Error::other)??; }
                }
            }
            drop(listener);
            while let Some(joined) = peers.join_next().await {
                joined.map_err(io::Error::other)??;
            }
            Ok(())
        });
        Self {
            endpoint,
            ca,
            accepted,
            requests,
            stop,
            task,
        }
    }
    pub(super) async fn finish(self) {
        let _ = self.stop.send(());
        self.task.await.unwrap().unwrap();
    }
}
async fn serve(
    io: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    entered: mpsc::Sender<String>,
) -> io::Result<()> {
    let mut h2 = h2::server::Builder::new()
        .handshake::<_, Cursor<Vec<u8>>>(io)
        .await
        .map_err(io::Error::other)?;
    let mut pending = Vec::new();
    while let Some(request) = h2.accept().await {
        let (request, mut respond) = request.map_err(h2_error)?;
        assert_eq!(request.uri().path(), "/dns-query");
        let mut body = request.into_body();
        let mut query = Vec::new();
        while let Some(bytes) = body.data().await {
            let bytes = bytes.map_err(h2_error)?;
            query.extend_from_slice(&bytes);
            body.flow_control()
                .release_capacity(bytes.len())
                .map_err(h2_error)?;
        }
        let question = zero_dns::udp::parse_dns_question(&query)?;
        assert!(matches!(question.query_type, 1 | 65));
        entered
            .send(question.domain.clone())
            .await
            .map_err(io::Error::other)?;
        if question.domain == "pending.ech.test" {
            pending.push(respond);
            continue;
        }
        let addresses = if question.query_type == 1 {
            vec![IpAddress::V4([203, 0, 113, 9])]
        } else {
            Vec::new()
        };
        let response = zero_dns::udp::build_dns_response(&query, &addresses);
        let mut send = respond
            .send_response(
                http_uri::Response::builder().status(200).body(()).unwrap(),
                false,
            )
            .map_err(h2_error)?;
        send.send_data(Cursor::new(response), true)
            .map_err(h2_error)?;
    }
    Ok(())
}
fn h2_error(source: h2::Error) -> io::Error {
    io::Error::new(
        source
            .get_io()
            .map_or(io::ErrorKind::Other, io::Error::kind),
        source,
    )
}

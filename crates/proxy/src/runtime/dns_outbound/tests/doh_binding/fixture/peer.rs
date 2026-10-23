use super::*;
use std::{io::Cursor, net::SocketAddr};
use tokio::task::JoinSet;
pub struct TlsPeer {
    pub endpoint: SocketAddr,
    pub certificate: rustls::pki_types::CertificateDer<'static>,
    pub pending: Arc<Notify>,
    pub counts: Arc<Counts>,
    pub task: Task,
}
impl TlsPeer {
    pub async fn start() -> Self {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key.into())
        .unwrap();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let pending = Arc::new(Notify::new());
        let entered = pending.clone();
        let counts = Arc::new(Counts::default());
        let events = counts.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut peers = JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=&mut stopped=>break,
                    accepted=listener.accept()=>{
                        let (io,_) = accepted?;
                        events.accepted.fetch_add(1,Ordering::AcqRel);
                        let acceptor=acceptor.clone();let entered=entered.clone();let events=events.clone();
                        peers.spawn(async move {
                            let tls=acceptor.accept(io).await?;
                            let result=serve(tls,entered).await;
                            events.record_close();
                            terminal(result)
                        });
                    },
                    joined=peers.join_next(),if !peers.is_empty()=>{joined.unwrap().map_err(io::Error::other)??;},
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
            certificate,
            pending,
            counts,
            task: Task { stop, task },
        }
    }
}
async fn serve<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    io: T,
    pending: Arc<Notify>,
) -> io::Result<()> {
    let mut connection = h2::server::Builder::new()
        .handshake::<_, Cursor<Vec<u8>>>(io)
        .await
        .map_err(io::Error::other)?;
    let mut held = vec![];
    while let Some(request) = connection.accept().await {
        let (request, mut respond) = request.map_err(|source| {
            io::Error::new(
                source
                    .get_io()
                    .map_or(io::ErrorKind::Other, io::Error::kind),
                source,
            )
        })?;
        assert_eq!(request.uri().path(), "/dns-query");
        let mut body = request.into_body();
        let mut query = vec![];
        while let Some(chunk) = body.data().await {
            let chunk = chunk.map_err(io::Error::other)?;
            query.extend_from_slice(&chunk);
            body.flow_control()
                .release_capacity(chunk.len())
                .map_err(io::Error::other)?;
        }
        let question = zero_dns::udp::parse_dns_question(&query)?;
        assert_eq!(question.query_type, 1);
        if question.domain == "pending.selector.test" {
            held.push(respond);
            pending.notify_one();
            continue;
        }
        let response =
            zero_dns::udp::build_dns_response(&query, &[IpAddress::V4([198, 51, 100, 79])]);
        let mut send = respond
            .send_response(
                http_uri::Response::builder().status(200).body(()).unwrap(),
                false,
            )
            .map_err(io::Error::other)?;
        send.send_data(Cursor::new(response), true)
            .map_err(io::Error::other)?;
    }
    Ok(())
}

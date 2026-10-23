use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) struct Peer {
    pub(super) address: SocketAddr,
    pub(super) client: quinn::ClientConfig,
    pub(super) received: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub(super) async fn start(pending: bool) -> Self {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert = certificate.cert.der().clone();
        let key =
            rustls::pki_types::PrivateKeyDer::Pkcs8(certificate.signing_key.serialize_der().into());
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
        server.alpn_protocols = vec![b"doq".to_vec()];
        let server = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server).unwrap(),
        ));
        let endpoint = quinn::Endpoint::server(server, loopback()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let mut client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        client.alpn_protocols = vec![b"doq".to_vec()];
        let client = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client).unwrap(),
        ));
        let received = Arc::new(Notify::new());
        let notice = received.clone();
        let task = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let size = recv.read_u16().await.unwrap() as usize;
            let mut query = vec![0; size];
            recv.read_exact(&mut query).await.unwrap();
            assert_eq!(
                recv.read(&mut [0; 1]).await.unwrap(),
                None,
                "original DoQ sender finishes the actual stream"
            );
            let question = zero_dns::udp::parse_dns_question(&query).unwrap();
            assert_eq!(question.query_type, 1);
            notice.notify_one();
            if !pending {
                let response = zero_dns::udp::build_dns_response(
                    &query,
                    &[zero_traits::IpAddress::V4([198, 51, 100, 91])],
                );
                send.write_u16(response.len().try_into().unwrap())
                    .await
                    .unwrap();
                send.write_all(&response).await.unwrap();
                send.finish().unwrap();
                send.stopped().await.unwrap();
            }
            connection.closed().await;
            drop(recv);
            drop(send);
            drop(connection);
            endpoint.close(0u32.into(), b"actual peer finished");
            endpoint.wait_idle().await;
        });
        Self {
            address,
            client,
            received,
            task,
        }
    }
    pub(super) async fn finish(self) {
        self.task.await.unwrap();
    }
}

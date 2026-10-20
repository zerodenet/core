use super::*;
use bytes::{Buf, Bytes};

pub(super) struct Peer {
    pub address: SocketAddr,
    pub client: quinn::ClientConfig,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub async fn start(reject: bool) -> Self {
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
        server.alpn_protocols = vec![b"h3".to_vec()];
        let endpoint = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(server).unwrap(),
            )),
            loopback(),
        )
        .unwrap();
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
        client.alpn_protocols = vec![b"h3".to_vec()];
        let client = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client).unwrap(),
        ));
        let task = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let mut server = ::h3::server::builder()
                .build::<_, Bytes>(h3_quinn::Connection::new(connection.clone()))
                .await
                .unwrap();
            let (request, mut stream) = server
                .accept()
                .await
                .unwrap()
                .unwrap()
                .resolve_request()
                .await
                .unwrap();
            assert_eq!(request.method(), "POST");
            assert_eq!(request.uri().path(), "/prepared");
            let mut body = Vec::new();
            while let Some(mut bytes) = stream.recv_data().await.unwrap() {
                assert!(
                    body.len() + bytes.remaining() <= 1024,
                    "finite real fixture request"
                );
                let len = bytes.remaining();
                body.extend_from_slice(&bytes.copy_to_bytes(len));
            }
            assert_eq!(body, b"actual H3 upload"); // Includes actual request FIN.
            if reject {
                connection.close(0x101u32.into(), b"actual H3 protocol failure");
            } else {
                stream
                    .send_response(http_uri::Response::builder().status(200).body(()).unwrap())
                    .await
                    .unwrap();
                stream
                    .send_data(Bytes::from_static(b"actual H3 response"))
                    .await
                    .unwrap();
                stream.finish().await.unwrap();
            }
            // Keep the actual control-stream owner until the client closes.
            connection.closed().await;
            drop(stream);
            drop(server);
            drop(connection);
            endpoint.close(0u32.into(), b"actual peer ended");
            endpoint.wait_idle().await;
        });
        Self {
            address,
            client,
            task,
        }
    }
    pub async fn finish(self) {
        self.task.await.unwrap();
    }
}

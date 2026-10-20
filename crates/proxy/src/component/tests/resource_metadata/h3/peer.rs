use super::*;
use bytes::{Buf, Bytes};

#[derive(Clone, Copy)]
enum Mode {
    Response,
    Reject,
    #[cfg(feature = "hysteria2")]
    Hysteria2,
}
pub(super) struct Peer {
    pub address: SocketAddr,
    pub client: quinn::ClientConfig,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub async fn start(reject: bool) -> Self {
        Self::start_mode(if reject { Mode::Reject } else { Mode::Response }).await
    }
    #[cfg(feature = "hysteria2")]
    pub async fn start_hysteria2() -> Self {
        Self::start_mode(Mode::Hysteria2).await
    }
    async fn start_mode(mode: Mode) -> Self {
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
            #[cfg(feature = "hysteria2")]
            if matches!(mode, Mode::Hysteria2) {
                assert_eq!(request.uri(), "https://hysteria/auth");
                assert_eq!(request.headers()["hysteria-auth"], "actual-password");
                assert_eq!(request.headers()["hysteria-cc-rx"], "2000000");
            } else {
                assert_eq!(request.uri().path(), "/prepared");
            }
            #[cfg(not(feature = "hysteria2"))]
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
            #[cfg(feature = "hysteria2")]
            if matches!(mode, Mode::Hysteria2) {
                assert!(body.is_empty(), "authentication has an actual request FIN");
                stream
                    .send_response(
                        http_uri::Response::builder()
                            .status(233)
                            .header("Hysteria-UDP", "true")
                            .header("Hysteria-CC-RX", "3000000")
                            .body(())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                stream.finish().await.unwrap();
                // Same real QUIC connection, original HY2 UDP wire codec. The
                // legacy production dispatcher is deliberately not replaced.
                let datagram = connection.read_datagram().await.unwrap();
                use zero_traits::UdpDatagramFraming;
                let packet = ::hysteria2::Hysteria2Outbound
                    .decode_udp_datagram(&(), &datagram)
                    .unwrap();
                assert_eq!(packet.session_id(), 17);
                assert_eq!(
                    packet.target(),
                    &zero_core::Address::Domain("actual.hy2.test".into())
                );
                assert_eq!(packet.port(), 5353);
                assert_eq!(packet.payload(), b"actual HY2 datagram");
                connection.send_datagram(datagram).unwrap();
            } else {
                assert_eq!(body, b"actual H3 upload");
            }
            #[cfg(not(feature = "hysteria2"))]
            assert_eq!(body, b"actual H3 upload");
            if matches!(mode, Mode::Reject) {
                connection.close(0x101u32.into(), b"actual H3 protocol failure");
            } else if matches!(mode, Mode::Response) {
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

//! The actual TLS wrappers keep reverse data alive after a real upload EOF.
use super::*;

#[tokio::test]
async fn original_tls_wrappers_preserve_complete_reverse_data_after_upload_eof() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        for backend in [TlsBackend::Rustls, TlsBackend::OpenSsl] {
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                    .into(),
            )
            .unwrap();
            let prepared = OwnedPreparedClient::from_material(
                &profile(backend),
                None,
                "localhost",
                Some(cert.cert.pem().as_bytes()),
            )
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            // Match the original controlled connecting/accepted TCP providers.
            client.set_nodelay(true).unwrap();
            server.set_nodelay(true).unwrap();
            assert!(client.nodelay().unwrap() && server.nodelay().unwrap());
            let payload: Vec<u8> = (0..262144).map(|index| (index % 251) as u8).collect();
            let expected = payload.clone();
            let server = async move {
                let stream = crate::tls::record_boundary::accept_rustls_handshake(
                    &tokio_rustls::TlsAcceptor::from(Arc::new(config)),
                    server,
                )
                .await
                .unwrap();
                let mut stream = crate::tls::InboundTlsStream::new_generic(stream);
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(
                    received == expected,
                    "server upload {:?}: {} of {} bytes",
                    backend,
                    received.len(),
                    expected.len()
                );
                stream.write_all(&received).await.unwrap();
                stream.shutdown().await.unwrap();
            };
            let client = async move {
                let mut stream = prepared.connect(client).await.unwrap();
                stream.write_all(&payload).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(
                    received == payload,
                    "client reverse {:?}: {} of {} bytes, first difference {:?}",
                    backend,
                    received.len(),
                    payload.len(),
                    received
                        .iter()
                        .zip(&payload)
                        .position(|(left, right)| left != right)
                );
            };
            tokio::join!(client, server);
        }
    })
    .await
    .expect("original eight-second complete TLS half-close deadline");
}

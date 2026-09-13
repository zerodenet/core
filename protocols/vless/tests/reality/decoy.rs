use super::*;

#[tokio::test]
async fn real_site_certificate_completes_tls_but_never_admits_proxy_data() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let cert = rcgen::generate_simple_self_signed(vec!["decoy.test".into()]).unwrap();
        let trust =
            ztls::certificate::ServerTrust::certificates(&[cert.cert.der().to_vec()]).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
        let (mut client_io, server_io) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let mut stream = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config))
                .accept(server_io)
                .await
                .unwrap();
            let mut preface = [0; 24];
            stream.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        });
        let mut session = RealityClientConnection::new(RealityClientConfig {
            public_key: *PublicKey::from(&StaticSecret::from([21; 32])).as_bytes(),
            decoy_trust: Some(trust),
            server_name: "decoy.test".into(),
            hybrid_key_exchange: false,
            ..Default::default()
        })
        .unwrap();
        perform_reality_handshake(&mut session, &mut client_io)
            .await
            .unwrap();
        assert!(!session.is_authenticated_reality());
        let result = admit_client(
            client_io,
            session,
            "decoy.test",
            crate::reality_spider::Profile::parse("/").unwrap(),
        )
        .await;
        assert!(matches!(result, Err(ref e) if e.kind() == io::ErrorKind::PermissionDenied));
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rustls_key_update_gets_response_while_reality_client_only_reads() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let cert = rcgen::generate_simple_self_signed(vec!["decoy.test".into()]).unwrap();
        let trust =
            ztls::certificate::ServerTrust::certificates(&[cert.cert.der().to_vec()]).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();

        let (mut client_io, server_io) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let mut stream = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config))
                .accept(server_io)
                .await
                .unwrap();
            for _ in 0..3 {
                stream.get_mut().1.refresh_traffic_keys().unwrap();
                stream.flush().await.unwrap();
                // Require the update acknowledgement before producing any
                // application data. This catches read-only driver deadlocks.
                let mut wire = [0; 27];
                stream.get_mut().0.read_exact(&mut wire).await.unwrap();
                stream.get_mut().1.read_tls(&mut &wire[..]).unwrap();
                stream.get_mut().1.process_new_packets().unwrap();
                stream.write_all(b"updated").await.unwrap();
                stream.flush().await.unwrap();
                let mut reply = [0; 7];
                stream.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"updated");
            }
        });
        let mut session = RealityClientConnection::new(RealityClientConfig {
            public_key: *PublicKey::from(&StaticSecret::from([21; 32])).as_bytes(),
            decoy_trust: Some(trust),
            server_name: "decoy.test".into(),
            hybrid_key_exchange: false,
            ..Default::default()
        })
        .unwrap();
        perform_reality_handshake(&mut session, &mut client_io)
            .await
            .unwrap();
        // Exercise the TLS carrier only; real certificates still fail VLESS admission.
        let mut stream = RealityTlsStream::new(client_io, session);
        for _ in 0..3 {
            let mut data = [0; 7];
            stream.read_exact(&mut data).await.unwrap();
            assert_eq!(&data, b"updated");
            stream.write_all(&data).await.unwrap();
            stream.flush().await.unwrap();
        }
        server.await.unwrap();
    })
    .await
    .unwrap();
}

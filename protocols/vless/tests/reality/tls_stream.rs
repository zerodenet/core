use super::*;
#[tokio::test]
async fn custom_tls_retains_partial_writes_and_answers_key_updates_under_backpressure() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let cert = rcgen::generate_simple_self_signed(vec!["decoy.test".into()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            std::sync::Arc::new(roots),
            std::sync::Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .unwrap();
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

        // Smaller than a TLS record; every application write must resume.
        let (client_io, server_io) = tokio::io::duplex(128);
        let expected: Vec<u8> = (0..131073).map(|i| (i % 251) as u8).collect();
        let payload = expected.clone();
        let (resume, resume_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut stream = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config))
                .accept(server_io)
                .await
                .unwrap();
            stream.get_mut().1.refresh_traffic_keys().unwrap();
            stream.flush().await.unwrap();
            let mut response = [0; 27];
            stream.get_mut().0.read_exact(&mut response).await.unwrap();
            stream.get_mut().1.read_tls(&mut &response[..]).unwrap();
            stream.get_mut().1.process_new_packets().unwrap();
            stream.write_all(b"ready").await.unwrap();
            stream.flush().await.unwrap();
            resume_rx.await.unwrap();
            let mut received = vec![0; expected.len()];
            stream.read_exact(&mut received).await.unwrap();
            assert_eq!(received, expected);
            // Zero sends an authenticated close_notify after draining all data.
            let mut end = [0; 1];
            assert_eq!(stream.read(&mut end).await.unwrap(), 0);
        });
        let mut client = ztls::stream::Tls13Stream::connect_async(
            client_io,
            ztls::handshake::Tls13Config {
                server_name: "decoy.test".into(),
                server_verifier: Some(verifier),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut ready = [0; 5];
        client.read_exact(&mut ready).await.unwrap();
        assert_eq!(&ready, b"ready");
        let accepted = client.write(&payload).await.unwrap();
        assert!(accepted > 0 && accepted <= 16384);
        {
            // The peer is paused and its 128-byte carrier is full. Cancel a
            // pending second write, then retry it after releasing the peer.
            let mut pending = Box::pin(client.write(&payload[accepted..]));
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
        }
        resume.send(()).unwrap();
        client.write_all(&payload[accepted..]).await.unwrap();
        client.shutdown().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

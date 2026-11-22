use super::*;
#[tokio::test]
async fn ordinary_vmess_datagram_preserves_tls_ws_early_data_and_wss_carriers() {
    for (tls, ws) in [(true, false), (false, true), (true, true)] {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path().join("cert.pem");
        let key_path = material.path().join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        let profile = zero_transport::profile::OwnedServerTlsProfile {
            options: Default::default(),
            cert_path: cert_path.display().to_string(),
            key_path: key_path.display().to_string(),
            alpn: vec![],
            server_fingerprint: None,
        };
        let acceptor = zero_transport::tls::build_tls_acceptor(&profile, None).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut protocol = serde_json::json!({"type":"vmess","server":"127.0.0.1","port":port,"id":ID,"cipher":"aes-128-gcm"});
        if tls {
            protocol["tls"] = serde_json::json!({"server_name":"localhost","insecure":true});
        }
        if ws {
            protocol["ws"] = serde_json::json!({"path":"/native?ed=2048"});
        }
        let proxy = proxy_with_protocol(
            protocol,
            serde_json::json!({"final":{"type":"route","outbound":"node"}}),
            true,
        )
        .await;
        assert_standard_datagram(&proxy);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = zero_platform_tokio::TokioSocket::new(stream);
            let stream = if tls {
                zero_platform_tokio::TcpRelayStream::new(
                    zero_transport::tls::accept_tls_handshake(&acceptor, stream)
                        .await
                        .unwrap(),
                )
            } else {
                zero_platform_tokio::TcpRelayStream::from_client(stream)
            };
            let stream = if ws {
                zero_platform_tokio::TcpRelayStream::new(
                    zero_transport::ws::accept_ws(stream, "/native")
                        .await
                        .unwrap(),
                )
            } else {
                stream
            };
            // The native owner can close a cancelled association before TLS
            // close_notify or a WS close frame. Check data on wire and consume
            // the same client's native retirement receipt below.
            echo_io_with_close(
                stream,
                "aes-128-gcm".into(),
                53,
                vec![b"carrier".to_vec()],
                !tls && !ws,
            )
            .await;
        });
        let originals = crate::runtime::udp_ingress::test_source::run(
            &proxy,
            move |proxy, runtime| async move {
                let runtime = runtime.clone();
                let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
                let id = dispatch.dispatch(input(53, b"carrier")).await.unwrap();
                let (_, tasks, _) = dispatch.poll_sockets();
                let packet = timeout(Duration::from_secs(3), tasks.join_next_with_id())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(packet.2.as_ref(), b"carrier");
                assert_eq!(
                    proxy
                        .engine()
                        .active_sessions()
                        .iter()
                        .find(|s| s.id == id)
                        .unwrap()
                        .outbound_tx_bytes,
                    16,
                    "carrier establishment is Outer traffic, not Flow TX"
                );
                let originals = capture_native_originals(&proxy);
                assert_eq!(originals.len(), 1);
                let _ = dispatch.finish_all();
                originals
            },
        )
        .await;
        server.await.unwrap();
        confirm_native_retirement(&proxy, &originals).await;
    }
}

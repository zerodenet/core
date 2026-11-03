use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_ca_failures_never_dial_or_fall_back_to_legacy_udp() {
    for (contents, budget) in [
        (None, 4096),
        (Some(b"invalid-pem".as_slice()), 4096),
        (Some(b"too-large".as_slice()), 1),
    ] {
        let dir = tempfile::tempdir().unwrap();
        if let Some(contents) = contents {
            std::fs::write(dir.path().join("ca.pem"), contents).unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
            "runtime":{"file_read_max_bytes":budget},
            "outbounds":[{"tag":"node","protocol":{"type":"vmess","server":"127.0.0.1","port":listener.local_addr().unwrap().port(),"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["test/Tun"]},"tls":{"server_name":"localhost","ca_cert_path":"ca.pem"}}}],
            "route":{"final":{"type":"route","outbound":"node"}}
        }).to_string(),Some(dir.path().to_path_buf())).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let current = proxy.engine().runtime_snapshot().config().clone();
        // Retain the exact cold slot selected before start. A fresh inventory
        // lookup after failed-start retirement may legitimately prepare a new
        // cold generation, which has no original provider installed yet.
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("node").unwrap();
        assert!(proxy
            .protocols
            .start_components(current, Instant::now() + Duration::from_secs(3))
            .await
            .is_err());
        assert_no_legacy_udp(&proxy);
        let owner = slot
            .original_carrier_owner(1)
            .expect("original started File/Node slot");
        assert_eq!(owner.observe().unwrap().retained(), 0);
        // Bind remains held. Poll the actual accept instead of sleeping/probing.
        use std::{
            future::{poll_fn, Future},
            task::Poll,
        };
        let accept = listener.accept();
        tokio::pin!(accept);
        poll_fn(|cx| {
            assert!(accept.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let report = proxy
            .protocols
            .drive_component_supervision(slot.shutdown(Instant::now() + Duration::from_secs(3)))
            .await
            .unwrap();
        assert!(
            report.is_complete(),
            "original File, scope and native owner: {report:?}"
        );
        let _shutdown = proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(3))
            .await;
        assert!(owner.closed().unwrap().matches(&owner));
    }
}

#[tokio::test]
async fn ordinary_vmess_grpc_ca_untrusted_peer_fails_real_tls_before_udp_submission() {
    timeout(Duration::from_secs(10),async {
        let dir=tempfile::tempdir().unwrap();
        let (acceptor,_)=super::replacement::material(dir.path(),"server");
        let (_,wrong_ca)=super::replacement::material(dir.path(),"untrusted");
        std::fs::write(dir.path().join("ca.pem"),wrong_ca).unwrap();
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port=listener.local_addr().unwrap().port();
        let proxy=proxy_with_protocol_at(serde_json::json!({"type":"vmess","server":"127.0.0.1","port":port,"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["test/Tun"]},"tls":{"server_name":"localhost","ca_cert_path":"ca.pem","alpn":["h2"]}}),serde_json::json!({"final":{"type":"route","outbound":"node"}}),true,Some(dir.path().to_path_buf())).await;
        std::fs::remove_file(dir.path().join("ca.pem")).unwrap();
        let server=tokio::spawn(async move {
            let (stream,_)=listener.accept().await.unwrap();
            assert!(zero_transport::tls::accept_tls_handshake(&acceptor,zero_platform_tokio::TokioSocket::new(stream)).await.is_err());
        });
        let registration=original(&proxy);
        proxy.protocols.drive_component_supervision(async {
            let mut dispatch=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("untrusted").await.unwrap();
            assert!(dispatch.dispatch(input(53,b"must-not-submit")).await.is_err());
            dispatch.finish_all();server.await.unwrap();
            proxy.protocols.shutdown_components(Instant::now()+Duration::from_secs(3)).await.unwrap();
        }).await;
        assert!(registration.closed().unwrap().matches(&registration));
    }).await.unwrap();
}

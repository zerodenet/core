use crate::Proxy;
use hysteria2::transport::{
    Hysteria2AuthenticatedInboundProfile, Hysteria2InboundBindOptionsRef, Hysteria2InboundBindPlan,
    Hysteria2InboundOptionsRef, Hysteria2InboundUserRef,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Duration},
};
use zero_core::{Address, InboundClientResponse, Network, ProtocolType, Session};
use zero_engine::{ResolvedOutbound, RouteDecision};

#[tokio::test]
async fn source_relative_tcp_preparation_reuses_the_actual_authenticated_pool() {
    timeout(Duration::from_secs(10), async {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("ca.pem"), cert.cert.pem()).unwrap();
        std::fs::write(material.path().join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
        let server = Hysteria2InboundBindPlan::from_options_refs(Some(material.path()),
            Hysteria2InboundBindOptionsRef { cert_path: Some("ca.pem"), key_path: Some("key.pem") })
            .bind("127.0.0.1:0").await.unwrap();
        let port = server.local_addr().unwrap().port();
        let (done, wait_done) = tokio::sync::oneshot::channel();
        let worker = tokio::spawn(async move {
            // Two successful logical requests must arrive on this one actual
            // authenticated connection; a newly allocated pool cannot pass.
            let connection = server.accept_connection().await.unwrap();
            let profile = Hysteria2AuthenticatedInboundProfile::from_options_refs(Hysteria2InboundOptionsRef {
                users: [Hysteria2InboundUserRef { password: "pool-password", principal_key: None,
                    up_bps: None, down_bps: None, device_limit: None,
                    quota_remaining_bytes: None, policy_revision: None }],
            });
            let connection = profile.accept_authenticated_connection(connection).await.unwrap();
            for _ in 0..2 {
                let (session, mut stream) = connection.accept_next_tcp_stream().await.unwrap().unwrap();
                assert_eq!(session.port, 443);
                connection.response_protocol().send_ok(&mut stream).await.unwrap();
                let mut bytes = [0;4];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes,b"ping");
                stream.write_all(&bytes).await.unwrap();
                stream.shutdown().await.unwrap();
            }
            wait_done.await.unwrap();
            connection.close("test complete");
            connection.datagram_source().closed().await;
        });
        let config = zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
            "outbounds":[{"tag":"pool","protocol":{"type":"hysteria2","server":"127.0.0.1",
                "server_name":"localhost","port":port,"password":"pool-password","ca_cert_path":"ca.pem"}}],
            "route":{"final":{"type":"route","outbound":"pool"}}
        }).to_string(),Some(material.path().to_owned())).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let snapshot = proxy.engine().runtime_snapshot();
        for _ in 0..2 {
            let (resolved,_) = proxy.engine().resolve_route_decision(RouteDecision::Route("pool".into())).unwrap();
            let ResolvedOutbound::Single(leaf) = resolved else { panic!("actual pooled leaf") };
            let claimed = proxy.protocols.claim_outbound_leaf(snapshot.config(),leaf).unwrap();
            let prepared = claimed.tcp_connect_for_route(zero_engine::RouteMode::Flow).map_err(|failure| failure.error).unwrap();
            let mut session = Session::new(0,Address::Ipv4([203,0,113,1]),443,Network::Tcp,ProtocolType::new("test"));
            proxy.engine().prepare_session(&mut session,"incoming").unwrap();
            let established = prepared.execute(proxy.tcp_runtime_services().execution(),&session).await
                .map_err(|failure|failure.error).unwrap();
            let mut stream = established.into_relay_stream().unwrap();
            stream.write_all(b"ping").await.unwrap();
            let mut response = [0;4];
            stream.read_exact(&mut response).await.unwrap();
            assert_eq!(&response,b"ping");
            stream.shutdown().await.unwrap();
        }
        done.send(()).unwrap();
        worker.await.unwrap();
    }).await.unwrap();
}

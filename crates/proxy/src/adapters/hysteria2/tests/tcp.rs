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
use zero_engine::RouteDecision;

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
        let config=proxy.engine().runtime_snapshot().config().clone();
        proxy.protocols.start_components(config.clone(),tokio::time::Instant::now()+Duration::from_secs(3)).await.unwrap();
        proxy.protocols.commit_components(&config,||Ok(())).unwrap();
        for _ in 0..2 {
            let (resolved,_) = proxy.engine().resolve_route_decision(RouteDecision::Route("pool".into())).unwrap();
            let mut session = Session::new(0,Address::Ipv4([203,0,113,1]),443,Network::Tcp,ProtocolType::new("test"));
            proxy.engine().prepare_session(&mut session,"incoming").unwrap();
            let services=proxy.tcp_runtime_services().for_inbound("incoming".into()).with_business_identity(&session).unwrap();
            assert!(matches!(services.prepare_tcp_outbound(&resolved,zero_engine::RouteMode::Flow).unwrap_or_else(|failure|panic!("{}",failure.error)),
                crate::inventory::PreparedTcpOutbound::Single(crate::inventory::PreparedTcpCandidate {execution:crate::inventory::PreparedTcpCandidateExecution::Native(_),..})));
            let established=crate::runtime::tcp_dispatch::dispatch_tcp_outbound(services,&session,resolved,zero_engine::RouteMode::Flow,crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic).await
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
        proxy.protocols.shutdown_components(tokio::time::Instant::now()+Duration::from_secs(3)).await.unwrap();
    }).await.unwrap();
}

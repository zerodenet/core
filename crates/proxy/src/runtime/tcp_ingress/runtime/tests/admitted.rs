use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::SessionAuth;

#[tokio::test]
async fn actual_ticketed_ingress_keeps_full_auth_and_one_settlement_through_the_real_tcp_pipeline()
{
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            socket.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, b"ping");
            socket.write_all(b"pong").await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let proxy = crate::Proxy::new(
            RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        proxy
            .protocols
            .start_components(
                proxy.engine().config(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&proxy.engine().config(), || Ok(()))
            .unwrap();
        let identity = proxy.engine().mint_session_identity().unwrap();
        let id = identity.id();
        let mut session = Session::new(
            id.0,
            Address::Ipv4([127, 0, 0, 1]),
            peer.port(),
            Network::Tcp,
            ProtocolType::new("ticketed-fixture"),
        );
        session.source_ip = Some(Address::Ipv4([192, 0, 2, 7]));
        session.source_port = Some(4567);
        let mut auth = SessionAuth::new("verified");
        auth.principal_key = Some("ticketed-user".into());
        auth.up_bps = Some(1_000_000);
        auth.down_bps = Some(2_000_000);
        auth.device_limit = Some(1);
        auth.quota_remaining_bytes = Some(1024);
        auth.policy_revision = Some(7);
        session.apply_auth(auth.clone());
        let runtime = TcpIngressRuntime::new(proxy.tcp_runtime_services(), "in".into(), None);
        let (client, mut app) = tokio::io::duplex(64);
        let serving = tokio::spawn(async move {
            let protocol = crate::runtime::tcp_ingress::NoClientResponseStreamProtocol::new();
            super::super::super::lifecycle::serve::serve_with_identity(
                &runtime,
                session,
                client,
                &protocol,
                Some(&identity),
            )
            .await
        });
        app.write_all(b"ping").await.unwrap();
        app.shutdown().await.unwrap();
        let mut response = Vec::new();
        app.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
        serving.await.unwrap().unwrap();
        server.await.unwrap();
        assert!(proxy.engine().active_sessions().is_empty());
        let completed = proxy.engine().completed_sessions();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].id, id.0);
        assert_eq!(completed[0].auth, Some(auth));
        assert_eq!((completed[0].bytes_up, completed[0].bytes_down), (4, 4));
        let events = proxy.engine().events_snapshot(&EventFilter {
            event_types: vec![event_type::FLOW_STARTED.into()],
            ..Default::default()
        });
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["flow_id"], id.0.to_string());
    })
    .await
    .unwrap();
}

//! Reverse control and business frames pass through the original standard
//! driver. This tests logical framing/Scope joins, not native close evidence.
use super::*;
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
use zero_platform_tokio::contract::scope::TaskScope;

#[tokio::test]
async fn reverse_control_never_routes_and_metadata_reaches_the_same_standard_request_driver() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        scope.enable_registration().unwrap();
        let status = Arc::new(crate::reverse::worker::WorkerStatus::default());
        let uuid = [7; 16];
        let server = VlessInboundMuxServer::from_master_uuid_with_auth(
            uuid,
            None,
            MuxResponseBacklogPolicy::default(),
        )
        .with_reverse(status.clone());
        let (stream, mut wire) = tokio::io::duplex(1024);
        let (opened, mut requests) = mpsc::channel(2);
        let stop = CancellationToken::new();
        let facts = InboundConnectionFacts {
            original_source: None,
            effective_source: None,
            local_endpoint: None,
            sni: None,
        };
        scope
            .registrar()
            .spawn_tracked(
                TaskClass::Auxiliary,
                driver::run(
                    TcpRelayStream::new(stream),
                    server,
                    opened,
                    stop.clone(),
                    facts,
                    uuid,
                    None,
                ),
            )
            .unwrap();
        // Control's port 0 is legal and must be consumed before application
        // validation or identity allocation. Split the frame at a real await.
        let control =
            codec::encode_new_stream(1, NETWORK_TCP, 0, &Address::Domain("reverse".into()))
                .unwrap();
        wire.write_all(&control[..3]).await.unwrap();
        assert!(requests.try_recv().is_err());
        wire.write_all(&control[3..]).await.unwrap();
        wire.write_all(&codec::encode_data_frame(1, &[8, 1]).unwrap())
            .await
            .unwrap();
        let mut session = Session::new(
            0,
            Address::Ipv4([192, 0, 2, 1]),
            443,
            Network::Tcp,
            ProtocolType::new("vless"),
        );
        session.source_ip = Some(Address::Ipv4([198, 51, 100, 9]));
        session.source_port = Some(3210);
        session.inbound_local = Some((Address::Ipv4([192, 0, 2, 17]), 1080));
        let mut opened =
            codec::encode_new_stream(2, NETWORK_TCP, session.port, &session.target).unwrap();
        origin::Origin::from_session(&session)
            .append(&mut opened)
            .unwrap();
        wire.write_all(&opened).await.unwrap();
        let request = requests.recv().await.unwrap();
        assert_eq!(
            request.facts.effective_source,
            Some("198.51.100.9:3210".parse().unwrap())
        );
        assert_eq!(
            request.facts.local_endpoint,
            Some("192.0.2.17:1080".parse().unwrap())
        );
        assert_eq!(
            request.facts.original_source, None,
            "duplex has no native peer address"
        );
        assert!(
            !status.load().active,
            "DRAIN was consumed inside the original driver"
        );
        let VlessInboundMuxOpenedRouteState::Tcp {
            session: accepted,
            relay,
        } = request.route.state
        else {
            panic!("TCP request")
        };
        assert_eq!(accepted.target, session.target);
        assert_eq!(accepted.port, 443);
        assert!(
            requests.try_recv().is_err(),
            "control frames never enter the route queue"
        );
        drop(relay);
        stop.cancel();
        let joined = scope
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await;
        assert!(joined.is_complete());
        assert_eq!(joined.exited_tasks.len(), 1);
        assert!(joined.failed_tasks.is_empty());
        assert!(!status.load().active);
    })
    .await
    .unwrap();
}

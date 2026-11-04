//! Real standard Direct source and peer run under their retained original owner.
use super::*;
use tokio::net::UdpSocket;
#[tokio::test]
async fn actual_standard_udp_peer_returns_original_egress_and_joins_before_owner_confirmation() {
    timeout(Duration::from_secs(8), async {
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let reserved = crate::socket_fixture::tcp_udp();
        let port = reserved.0.local_addr().unwrap().port();
        let config = RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"direct","target":"127.0.0.1","port":target.local_addr().unwrap().port()}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let config = proxy.engine().config();
        proxy.protocols.start_components(config.clone(), Instant::now() + Duration::from_secs(2)).await.unwrap();
        proxy.protocols.commit_components(&config, || Ok(())).unwrap();
        let prepared = proxy.protocols.prepare_inbound_bind(config.inbounds[0].clone(), None).unwrap().unwrap();
        let (shutdown, _) = watch::channel(false);
        let mut lease = proxy.inbound_owners.reserve_listener("first", &config, &proxy.protocols, prepared, shutdown.clone(), NonZeroUsize::new(1).unwrap()).unwrap();
        drop(reserved);
        lease.bind_tcp().unwrap();
        let (definition, listener, installed) = promote_tcp(lease.finish().unwrap());
        let owner = installed.owner();
        let endpoint = listener.local_addr().unwrap();
        let recipe=definition;
        let limit = match recipe.spec().resources[0] {
            ResourceDeclaration::TcpListener { accepted: Some(limits), .. } => limits.max_sockets,
            _ => panic!("actual accepted declaration"),
        };
        let (source, peers) = installed.build_inbound(recipe, NativeListenerView::Tcp(listener), None).unwrap();
        let runtime = crate::runtime::route_runtime::InboundListenerRuntime::new(
            crate::runtime::route_runtime::SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
            "first".into(), Some(owner.task_access()),
        ).with_inbound_instance(owner.identity()).with_readiness(owner.readiness()).with_datagram_source(peers);
        owner.spawn_driver(crate::runtime::inbound_operation::source::run(source, runtime, shutdown.subscribe(), limit, "direct")).unwrap();
        owner.seal_listener_installation();
        let exercise = async {
            owner.wait_ready().await.unwrap();
            let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            client.send_to(b"actual", endpoint).await.unwrap();
            let mut bytes = [0;16];
            let (size, upstream) = target.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], b"actual");
            target.send_to(b"reply", upstream).await.unwrap();
            let (size, sender) = client.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], b"reply");
            assert_eq!(sender, endpoint);
            assert!(owner.terminal().is_none());
            assert_eq!(owner.peer_tasks_for_test().closed_io_receipts_count(), 0);
            shutdown.send_replace(true);
        };
        let (result, ()) = tokio::join!(owner.observe_driver(), exercise);
        result.unwrap();
        let receipt = proxy.inbound_owners.confirm_shutdown("first", Instant::now() + Duration::from_secs(2)).await.unwrap().unwrap();
        let terminal = receipt.terminal();
        assert!(terminal.failure().is_none());
        assert!(terminal.task_failures().is_empty());
        assert_eq!(terminal.joined_tasks(), 2, "actual source and actual peer Scope joins");
        assert_eq!(terminal.closed_io_receipts(), 3, "TCP listener, UDP listener and actual peer Direct egress originals");
        assert_eq!(owner.peer_tasks_for_test().closed_io_receipts_count(), 1, "real retained peer egress close, not view Drop or bind probe");
    }).await.unwrap();
}

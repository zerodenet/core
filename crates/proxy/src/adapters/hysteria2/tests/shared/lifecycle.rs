use super::*;
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::{BoxStream, ConnectRequest},
    BoxFuture, CancellationToken, ChannelError,
};
async fn start(proxy: &Proxy) {
    let config = proxy.engine().runtime_snapshot().config().clone();
    proxy
        .protocols
        .start_components(config.clone(), end())
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
}
async fn connect(proxy: &Proxy, tag: &str) -> zero_platform_tokio::TcpRelayStream {
    let mut session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 1]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route(tag.into()))
        .unwrap();
    let established = crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
        proxy.tcp_runtime_services().for_inbound("incoming".into()),
        &session,
        resolved,
        zero_engine::RouteMode::Flow,
        crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic,
    )
    .await
    .map_err(|failure| failure.error)
    .unwrap();
    let mut stream = established.into_relay_stream().unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ping");
    stream
}
struct Denied;
impl UpstreamOpener for Denied {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("retired role cannot acquire an upstream") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { panic!("retired role cannot acquire packet I/O") })
    }
}
#[tokio::test]
async fn hysteria2_identity_isolation_and_historical_role_revocation_use_actual_pool_generations() {
    tokio::time::timeout(Duration::from_secs(20),async {
        let server=Server::new().await;
        let proxy=Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1","port":server.port,"password":"p","insecure":true}},
                {"tag":"isolated","protocol":{"type":"hysteria2","server":"127.0.0.1","port":server.port,"password":"p","insecure":true}}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string()).unwrap()).unwrap();start(&proxy).await;
        let first=connect(&proxy,"hy").await;let same=connect(&proxy,"hy").await;assert_eq!(server.authenticated.load(Ordering::SeqCst),1);
        let isolated=connect(&proxy,"isolated").await;assert_eq!(server.authenticated.load(Ordering::SeqCst),2,"tag is part of the actual authentication/pool identity");
        let plan=proxy.tcp_runtime_services().execution().components.as_ref().unwrap().clone();let slot=plan.configured("hy").unwrap();let historical=slot.stream(end()).await.unwrap();
        drop(first);drop(same);drop(isolated);proxy.protocols.shutdown_components(end()).await.unwrap();
        let binding=UpstreamBinding {prefix:vec![],dial:Default::default(),network_generation:zero_core::component::NetworkGeneration(proxy.egress_interface.generation())};
        let result=historical.connect(ConnectRequest {destination:zero_core::request::Destination::new(Address::Ipv4([203,0,113,1]),443)},OpenContext {
            identity:RequestIdentity::Service(Arc::new(zero_core::request::ServiceRequestFacts {origin:zero_core::request::ServiceOrigin::Runtime,purpose:zero_core::request::ServicePurpose::DiagnosticProbe})),
            deadline:Some(end()),cancel:CancellationToken::new(),upstream:UpstreamTransport::new(binding,Arc::new(Denied)),observer:None,
        }).await;
        let error=match result{Err(error)=>error,Ok(_)=>panic!("historical role reopened a retired pool")};assert_eq!(error.reason,zero_platform_tokio::contract::Reason::StalePlan);assert_eq!(error.component,Some(slot.id));
        assert_eq!(server.authenticated.load(Ordering::SeqCst),2,"cached role cannot reach a replacement authentication");server.close().await;
    }).await.unwrap();
}

#[tokio::test]
async fn hysteria2_network_replacement_waits_for_the_actual_historical_stream_guard() {
    use std::future::{poll_fn, Future};
    use std::task::Poll;
    tokio::time::timeout(Duration::from_secs(20),async {
        let server=Server::new().await;let proxy=server.proxy();start(&proxy).await;
        let first=connect(&proxy,"hy").await;assert_eq!(server.authenticated.load(Ordering::SeqCst),1);
        let previous=proxy.egress_interface.generation();proxy.egress_interface.replace_tunnel_addresses([std::net::IpAddr::V4(std::net::Ipv4Addr::new(198,18,0,1))]);assert_ne!(proxy.egress_interface.generation(),previous);
        let mut next=Box::pin(connect(&proxy,"hy"));
        tokio::select! {biased;_=server.wait_closed(1)=>{},_=&mut next=>panic!("replacement authenticated before the actual old stream returned")}
        poll_fn(|cx|{assert!(next.as_mut().poll(cx).is_pending(),"old transferred stream still owns a real sealed endpoint user");Poll::Ready(())}).await;
        assert_eq!(server.authenticated.load(Ordering::SeqCst),1);
        drop(first);
        let replacement=next.await;assert_eq!(server.authenticated.load(Ordering::SeqCst),2);
        drop(replacement);proxy.protocols.shutdown_components(end()).await.unwrap();server.close().await;
    }).await.unwrap();
}

#[tokio::test]
async fn hysteria2_actual_node_candidates_fall_back_and_concurrent_users_share_the_survivor() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        // The first real candidate refuses its QUIC handshake. It cannot
        // authenticate, and the next candidate is the actual HY2 fixture.
        // macOS does not bind arbitrary 127/8 aliases. Use the same explicitly
        // assigned fixture address as the real device integration tests.
        let refused_ip = std::env::var("ZERO_TEST_HOST_IPV4")
            .map(|ip| {
                ip.parse::<std::net::Ipv4Addr>()
                    .expect("actual fixture IPv4")
            })
            .unwrap_or(std::net::Ipv4Addr::new(127, 0, 0, 2));
        assert_ne!(refused_ip, std::net::Ipv4Addr::LOCALHOST);
        let refused = zero_transport::quic::QuicInbound::bind(
            &format!("{refused_ip}:{}", server.port),
            "cert.pem",
            "key.pem",
            Some(server._material.path()),
            &[b"h3".to_vec()],
        )
        .await
        .unwrap();
        let mut dns = crate::dns_fixture::Dns::new().await;
        let mut config = zero_config::RuntimeConfig::parse(
            &serde_json::json!({
                "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2",
                    "server":"candidates.test","server_name":"localhost",
                    "port":server.port,"password":"p","insecure":true}}],
                "route":{"final":{"type":"route","outbound":"hy"}}
            })
            .to_string(),
        )
        .unwrap();
        config.runtime.dns = Some(dns.config());
        let proxy = Proxy::new(config).unwrap();
        start(&proxy).await;
        let exchanges = async { tokio::join!(connect(&proxy, "hy"), connect(&proxy, "hy")) };
        tokio::pin!(exchanges);
        let request = tokio::select! {
            request = dns.request() => request,
            _ = &mut exchanges => panic!("domain dialing bypassed the actual Node resolver"),
        };
        request.answer_ips(
            false,
            false,
            &[
                zero_traits::IpAddress::V4(refused_ip.octets()),
                zero_traits::IpAddress::V4([127, 0, 0, 1]),
            ],
        );
        let incoming = tokio::select! {
            incoming = refused.accept_incoming() => incoming.unwrap(),
            _ = &mut exchanges => panic!("the first actual candidate was skipped"),
        };
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 0);
        incoming.refuse();
        let (first, second) = exchanges.await;
        assert_eq!(
            server.authenticated.load(Ordering::SeqCst),
            1,
            "failed candidate cleanup precedes one shared surviving authentication"
        );
        drop(first);
        drop(second);
        proxy.protocols.shutdown_components(end()).await.unwrap();
        proxy.resolver.shutdown_queries(end()).await.unwrap();
        drop(refused);
        dns.close().await;
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hysteria2_original_logical_control_retains_custody_until_actual_views_return() {
    use zero_core::{component::AssociationId, request::DatagramTargets, request::Destination};
    use zero_platform_tokio::contract::{datagram::DatagramRequest, Reason};
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        let proxy = server.proxy();
        start(&proxy).await;
        let mut session = Session::new(
            0,
            Address::Ipv4([203, 0, 113, 2]),
            53,
            Network::Udp,
            ProtocolType::new("original-logical-control-test"),
        );
        proxy
            .engine()
            .prepare_session(&mut session, "incoming")
            .unwrap();
        let services = proxy
            .tcp_runtime_services()
            .for_inbound("incoming".into())
            .with_business_identity(&session)
            .unwrap();
        let slot = services
            .components
            .as_ref()
            .unwrap()
            .configured("hy")
            .unwrap();
        let opened = crate::component::stream_bridge::datagram::open(
            &services.execution(),
            &slot,
            "hy",
            Destination::new(Address::Ipv4([127, 0, 0, 1]), server.port),
            DatagramRequest {
                association: AssociationId(991),
                targets: DatagramTargets::Multiple,
            },
        )
        .await
        .unwrap();
        let zero_platform_tokio::contract::datagram::DatagramSession {
            tx,
            mut rx,
            mut control,
            ..
        } = opened.channel;
        std::future::poll_fn(|cx| {
            assert!(
                rx.as_mut().poll_recv(cx).is_pending(),
                "actual unique response view is pending on the original registration"
            );
            std::task::Poll::Ready(())
        })
        .await;
        let error = control
            .wait_closed(tokio::time::Instant::now())
            .await
            .unwrap_err();
        assert_eq!(
            error.reason,
            Reason::Timeout,
            "cancellation cannot stand in for returned original views"
        );
        drop(tx);
        let error = control
            .wait_closed(tokio::time::Instant::now())
            .await
            .unwrap_err();
        assert_eq!(
            error.reason,
            Reason::Timeout,
            "the unique original RX still retains custody"
        );
        drop(rx);
        let report = control.wait_closed(end()).await.unwrap();
        assert!(report.is_complete());
        assert!(
            report
                .released_resources
                .iter()
                .any(|identity| identity == "Hysteria2 original UDP logical registration 1"),
            "the report names the actual first dispatcher registration"
        );
        assert!(
            report.closed_io.is_empty() && report.exited_tasks.is_empty(),
            "logical retirement does not claim raw endpoint, native close or Scope evidence"
        );
        drop(control);
        assert_eq!(server.authenticated.load(Ordering::SeqCst), 1);
        proxy.protocols.shutdown_components(end()).await.unwrap();
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn hysteria2_finite_original_udp_session_capacity_refuses_then_reclaims_actual_registration()
{
    use zero_core::{component::AssociationId, request::DatagramTargets, request::Destination};
    use zero_platform_tokio::contract::{datagram::DatagramRequest, Reason};
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = Server::new().await;
        let proxy = server.proxy();
        start(&proxy).await;
        let mut session = Session::new(0, Address::Ipv4([203,0,113,2]), 53, Network::Udp, ProtocolType::new("bounded-udp-test"));
        proxy.engine().prepare_session(&mut session,"incoming").unwrap();
        let services=proxy.tcp_runtime_services().for_inbound("incoming".into()).with_business_identity(&session).unwrap().execution();
        let slot=services.components.as_ref().unwrap().configured("hy").unwrap();
        let endpoint=Destination::new(Address::Ipv4([127,0,0,1]),server.port);
        let open=|id| crate::component::stream_bridge::datagram::open(&services,&slot,"hy",endpoint.clone(),DatagramRequest{association:AssociationId(id),targets:DatagramTargets::Multiple});
        let mut channels=Vec::new();
        for id in 1..=4096 {channels.push(open(id).await.unwrap().channel);}
        let error=match open(4097).await {Err(error)=>error,Ok(_)=>panic!("finite dispatcher capacity was bypassed")};
        assert_eq!(error.reason,Reason::Capacity);
        assert_eq!(server.authenticated.load(Ordering::SeqCst),1,"capacity pressure cannot evict active original associations or allocate a parallel pool");
        let first=channels.remove(0);
        let zero_platform_tokio::contract::datagram::DatagramSession{tx,rx,mut control,..}=first;
        drop(tx);drop(rx);
        let report=control.wait_closed(end()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.released_resources,vec!["Hysteria2 original UDP logical registration 1"]);
        drop(control);
        let recovered=open(4097).await.unwrap().channel;
        channels.push(recovered);
        for channel in channels {
            let zero_platform_tokio::contract::datagram::DatagramSession{tx,rx,mut control,..}=channel;
            drop(tx);drop(rx);
            let report=control.wait_closed(end()).await.unwrap();assert!(report.is_complete());
            assert_eq!(report.released_resources.len(),1);
        }
        assert_eq!(server.authenticated.load(Ordering::SeqCst),1);
        proxy.protocols.shutdown_components(end()).await.unwrap();server.close().await;
    }).await.unwrap();
}

#[path = "lifecycle/budget.rs"]
mod budget;
#[path = "lifecycle/dns.rs"]
mod dns;

#[tokio::test]
async fn hysteria2_invalid_ca_start_keeps_actual_registered_metadata_cleanup_confirmable() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let material=tempfile::tempdir().unwrap();std::fs::write(material.path().join("bad-ca.pem"),b"not a certificate").unwrap();
        let config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1","port":443,"password":"p","ca_cert_path":"bad-ca.pem"}}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string(),Some(material.path().to_owned())).unwrap();
        let proxy=Proxy::new(config).unwrap();let config=proxy.engine().runtime_snapshot().config().clone();
        let failed=proxy.protocols.start_components(config,end()).await.unwrap_err();
        let error=crate::transport::channel_failure(&failed).expect("actual material preparation failure");
        assert_eq!((error.stage,error.reason),(zero_platform_tokio::contract::Stage::Prepare,zero_platform_tokio::contract::Reason::InvalidConfig));
        // Cold preparation already confirmed its actual original CA File.
        // Failed start still owes the actual bound pool owners' original
        // no-acquisition receipts; missing registration would remain Unknown.
        proxy.protocols.shutdown_components(end()).await.unwrap();
    }).await.unwrap();
}
#[path = "lifecycle/file_budget.rs"]
mod file_budget;
#[path = "lifecycle/hop.rs"]
mod hop;
#[path = "lifecycle/pending.rs"]
mod pending;
#[path = "lifecycle/reload.rs"]
mod reload;
#[path = "lifecycle/unchanged.rs"]
mod unchanged;
#[path = "lifecycle/stream_loan.rs"]
mod stream_loan;

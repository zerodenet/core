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
        proxy.tcp_runtime_services(),
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
        let error=match result{Err(error)=>error,Ok(_)=>panic!("historical role reopened a retired pool")};assert_eq!(error.reason,zero_platform_tokio::contract::Reason::Revoked);
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

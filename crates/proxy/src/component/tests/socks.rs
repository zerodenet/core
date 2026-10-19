use crate::{
    inventory::{PreparedTcpCandidateExecution, PreparedTcpOutbound},
    runtime::tcp_dispatch::{dispatch_tcp_outbound, TcpDispatchIntent},
    Proxy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{RouteDecision, RouteMode};

fn proxy(ports: &[u16]) -> Proxy {
    let outbounds:Vec<_>=ports.iter().enumerate().map(|(index,port)| serde_json::json!({"tag":format!("node-{index}"),"protocol":{"type":"socks5","server":"127.0.0.1","port":port}})).collect();
    let mut config = serde_json::json!({"outbounds":outbounds,"route":{"final":{"type":"route","outbound":"node-0"}}});
    if ports.len() > 1 {
        config["outbound_groups"] = serde_json::json!([{"tag":"fallback","type":"fallback","outbounds":["node-0","node-1"]}]);
    }
    Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap()
}
fn request(proxy: &Proxy) -> (Session, crate::protocol_registry::TcpRuntimeServices) {
    let mut session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
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
    (session, services)
}
async fn handshake(stream: &mut TcpStream) {
    let mut greeting = [0; 3];
    stream.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 1, 0]);
    stream.write_all(&[5, 0]).await.unwrap();
    let mut connect = [0; 10];
    stream.read_exact(&mut connect).await.unwrap();
    assert_eq!(connect, [5, 1, 0, 1, 203, 0, 113, 9, 1, 187]);
}
#[tokio::test]
async fn business_socks_uses_built_role_and_accounts_only_actual_handshake_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = proxy(&[port]);
    let (session, services) = request(&proxy);
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node-0".into()))
        .unwrap();
    let prepared = services
        .prepare_tcp_outbound(&resolved, RouteMode::Flow)
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(
        matches!(prepared,PreparedTcpOutbound::Single(ref candidate) if matches!(candidate.execution,PreparedTcpCandidateExecution::Native(_)))
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        handshake(&mut stream).await;
        stream
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
            .await
            .unwrap();
        stream.write_all(b"pre").await.unwrap();
        let mut payload = Vec::new();
        stream.read_to_end(&mut payload).await.unwrap();
        assert_eq!(payload, b"request");
        stream.write_all(b"tail").await.unwrap();
    });
    let established = dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        TcpDispatchIntent::Traffic,
    )
    .await
    .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
    let mut route = crate::transport::extract_tcp_stream(established).unwrap();
    assert_eq!(route.upstream_endpoint, Some(("127.0.0.1".into(), port)));
    assert!(!route.is_direct);
    let active = proxy.engine().active_sessions();
    assert_eq!(active[0].outbound_tx_bytes, 13);
    assert_eq!(active[0].outbound_rx_bytes, 12);
    route.upstream.write_all(b"request").await.unwrap();
    route.upstream.shutdown().await.unwrap();
    let mut reply = Vec::new();
    route.upstream.read_to_end(&mut reply).await.unwrap();
    assert_eq!(reply, b"pretail");
    server.await.unwrap();
    let active = proxy.engine().active_sessions();
    assert_eq!(active[0].outbound_tx_bytes, 13);
    assert_eq!(active[0].outbound_rx_bytes, 12);
}

#[tokio::test]
async fn revoked_socks_role_does_not_fall_back_to_a_per_request_legacy_claim() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(&[listener.local_addr().unwrap().port()]);
    let (session, services) = request(&proxy);
    let plan = services.execution().components.unwrap();
    let slot = plan.configured("node-0").unwrap();
    slot.stream(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    slot.revoke();
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node-0".into()))
        .unwrap();
    let failure = match dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        TcpDispatchIntent::Traffic,
    )
    .await
    {
        Ok(_) => panic!("revoked role connected"),
        Err(error) => error,
    };
    assert_eq!(failure.stage, "connect_upstream_socks5");
    assert_eq!(failure.error.code(), "io");
    let channel = crate::transport::channel_failure(&failure.error).unwrap();
    assert_eq!(channel.component, Some(slot.id));
    assert_eq!(
        channel.session,
        Some(zero_core::component::SessionId(session.id))
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(
        crate::transport::failure_origin(&failure.error),
        Some(crate::transport::TransportFailureOrigin::LocalNetwork)
    );
}

#[tokio::test]
async fn fallback_does_not_replay_an_unconfirmed_submitted_socks_connect() {
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(&[
        first.local_addr().unwrap().port(),
        second.local_addr().unwrap().port(),
    ]);
    let (session, services) = request(&proxy);
    let server = tokio::spawn(async move {
        let (mut stream, _) = first.accept().await.unwrap();
        handshake(&mut stream).await;
    });
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("fallback".into()))
        .unwrap();
    let result = dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        TcpDispatchIntent::Traffic,
    )
    .await;
    assert!(result.is_err());
    server.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), second.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn native_socks_preserves_existing_rejection_code_message_stage_and_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(&[listener.local_addr().unwrap().port()]);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut greeting = [0; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            stream.write_all(&[5, 255]).await.unwrap();
        }
    });
    let (session, native) = request(&proxy);
    let legacy = proxy.tcp_runtime_services();
    let mut failures = Vec::new();
    for services in [native, legacy] {
        let (resolved, _plan) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("node-0".into()))
            .unwrap();
        let failure = match dispatch_tcp_outbound(
            services,
            &session,
            resolved,
            RouteMode::Flow,
            TcpDispatchIntent::DiagnosticProbe,
        )
        .await
        {
            Ok(_) => panic!("authentication rejection connected"),
            Err(error) => error,
        };
        failures.push((
            failure.error.code(),
            failure.error.to_string(),
            failure.stage,
            failure.upstream_endpoint,
        ));
    }
    assert_eq!(failures[0], failures[1]);
    server.await.unwrap();
}

#[tokio::test]
async fn fallback_can_try_another_member_before_any_socks_connect_request_is_submitted() {
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(&[
        first.local_addr().unwrap().port(),
        second.local_addr().unwrap().port(),
    ]);
    let (session, services) = request(&proxy);
    let rejected = tokio::spawn(async move {
        let (mut stream, _) = first.accept().await.unwrap();
        let mut greeting = [0; 3];
        stream.read_exact(&mut greeting).await.unwrap();
        stream.write_all(&[5, 255]).await.unwrap();
    });
    let accepted = tokio::spawn(async move {
        let (mut stream, _) = second.accept().await.unwrap();
        handshake(&mut stream).await;
        stream
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
            .await
            .unwrap();
    });
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("fallback".into()))
        .unwrap();
    let outbound = dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        TcpDispatchIntent::Traffic,
    )
    .await
    .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert_eq!(
        crate::transport::extract_tcp_stream(outbound)
            .unwrap()
            .outbound_tag,
        "node-1"
    );
    rejected.await.unwrap();
    accepted.await.unwrap();
}

#[tokio::test]
async fn stream_only_factory_does_not_bypass_strict_packet_plane_validation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(&[listener.local_addr().unwrap().port()]);
    let (_session, services) = request(&proxy);
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node-0".into()))
        .unwrap();
    assert!(services
        .prepare_tcp_outbound(&resolved, RouteMode::Packet)
        .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

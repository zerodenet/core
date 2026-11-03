use crate::{
    inventory::{PreparedUdpLeafCandidate, PreparedUdpOutbound},
    protocol_registry::UdpAdapterContext,
    runtime::{pipe::UdpPipeInput, udp_ingress::UdpIngressRuntime},
    Proxy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{timeout, Duration, Instant},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{RouteDecision, RouteMode};

async fn proxy(port: u16, enabled: bool) -> Proxy {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(
            &serde_json::json!({
                "outbounds":[{"tag":"node","udp":{"enabled":enabled},"protocol":{
                    "type":"socks5","server":"127.0.0.1","port":port
                }}],"route":{"final":{"type":"route","outbound":"node"}}
            })
            .to_string(),
        )
        .unwrap(),
    )
    .unwrap();
    let config = proxy.engine().runtime_snapshot().config().clone();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    proxy
}
fn input(port: u16, payload: &[u8]) -> UdpPipeInput<'_> {
    UdpPipeInput {
        peer_identity: None,
        target: Address::Ipv4([203, 0, 113, 9]),
        port,
        payload,
        route_target: None,
        sniffed_original_target: None,
        sniffed_host_source: None,
        skip_fake_ip_restore: false,
        protocol: ProtocolType::new("test"),
        auth: None,
        source_addr: None,
        client_session_id: None,
        transparent_target: false,
        transparent_original_target: None,
        transparent_host_source: None,
    }
}

#[tokio::test]
async fn ordinary_socks_datagram_dispatch_needs_no_claim_and_preserves_wire_tx_and_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_port = udp.local_addr().unwrap().port();
    let proxy = proxy(listener.local_addr().unwrap().port(), true).await;
    let config = proxy.engine().runtime_snapshot().config().clone();
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node".into()))
        .unwrap();
    let zero_engine::ResolvedOutbound::Single(leaf) = &resolved else {
        panic!("single node");
    };
    let claimed = proxy
        .protocols
        .claim_outbound_leaf(&config, leaf.clone())
        .unwrap();
    assert!(
        claimed.into_udp_flow_for_route(RouteMode::Flow).is_err(),
        "no legacy UDP operation may remain"
    );
    let session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        53,
        Network::Udp,
        ProtocolType::new("test"),
    );
    let prepared = proxy
        .protocols
        .prepare_udp_outbound(
            UdpAdapterContext::new(&config, proxy.udp_runtime_services()),
            &session,
            &resolved,
            b"one",
            RouteMode::Flow,
        )
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(matches!(
        prepared,
        PreparedUdpOutbound::Single(PreparedUdpLeafCandidate::Datagram(_))
    ));

    let control = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut greeting = [0; 3];
        stream.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 1, 0]);
        stream.write_all(&[5, 0]).await.unwrap();
        let mut request = [0; 10];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request[..4], &[5, 3, 0, 1]);
        let mut response = vec![5, 0, 0, 1, 127, 0, 0, 1];
        response.extend_from_slice(&udp_port.to_be_bytes());
        stream.write_all(&response).await.unwrap();
        let mut remaining = Vec::new();
        stream.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.is_empty());
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err(),
            "all packets must reuse this control connection"
        );
    });
    let echoes = tokio::spawn(async move {
        let mut peer = None;
        for (port, payload) in [
            (53u16, b"one".as_slice()),
            (54, b"two".as_slice()),
            (53, b"".as_slice()),
        ] {
            let mut buf = [0; 1024];
            let (len, from) = udp.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..8], &[0, 0, 0, 1, 203, 0, 113, 9]);
            assert_eq!(&buf[8..10], &port.to_be_bytes());
            assert_eq!(&buf[10..len], payload);
            if let Some(peer) = peer {
                assert_eq!(from, peer);
            } else {
                peer = Some(from);
            }
            udp.send_to(&buf[..len], from).await.unwrap();
        }
    });
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
    let mut ids = Vec::new();
    for (port, payload) in [
        (53, b"one".as_slice()),
        (54, b"two".as_slice()),
        (53, b"".as_slice()),
    ] {
        ids.push(dispatch.dispatch(input(port, payload)).await.unwrap());
        let mut buf = [0; 1024];
        let (_, upstream, _, _, _) = dispatch.poll_refs();
        let response = timeout(Duration::from_secs(3), upstream.recv_response(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let (target, response_port, response_payload) = response.into_parts();
        assert_eq!(target, Address::Ipv4([203, 0, 113, 9]));
        assert_eq!(response_port, port);
        assert_eq!(response_payload, payload);
    }
    assert_eq!(ids[0], ids[2]);
    assert_ne!(ids[0], ids[1]);
    let sessions = proxy.engine().active_sessions();
    assert_eq!(
        sessions
            .iter()
            .find(|s| s.id == ids[0])
            .unwrap()
            .outbound_tx_bytes,
        13 + 13 + 10
    );
    assert_eq!(
        sessions
            .iter()
            .find(|s| s.id == ids[1])
            .unwrap()
            .outbound_tx_bytes,
        13
    );
    echoes.await.unwrap();
    let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
    let _ = dispatch.finish_all();
    let report = retired
        .wait_closed(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(
        !report.closed_io.is_empty(),
        "consume the retained original close report"
    );
    control.await.unwrap();
    proxy
        .protocols
        .shutdown_components(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn revoked_socks_datagram_never_dials_a_legacy_fallback() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(listener.local_addr().unwrap().port(), true).await;
    let execution = proxy.tcp_runtime_services().execution();
    let slot = execution
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap();
    slot.datagram(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    slot.revoke();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
    let error = dispatch.dispatch(input(53, b"query")).await.unwrap_err();
    let channel = crate::transport::channel_failure(&error).unwrap();
    // The actual host fence rejects the retired generation before invoking the
    // protocol role. Preserve that stronger, pre-I/O admission failure.
    assert_eq!(
        channel.reason,
        zero_platform_tokio::contract::Reason::StalePlan
    );
    assert_eq!(channel.stage, zero_platform_tokio::contract::Stage::Prepare);
    assert_eq!(
        channel.delivery,
        zero_platform_tokio::contract::DeliveryState::NotSubmitted
    );
    assert_eq!(channel.component, Some(slot.id));
    assert!(channel.session.is_some());
    assert!(timeout(Duration::from_millis(30), listener.accept())
        .await
        .is_err());
    let _ = dispatch.finish_all();
    proxy
        .protocols
        .shutdown_components(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn standard_socks_datagram_selection_keeps_udp_policy_and_strict_packet_rejection() {
    for (enabled, mode, stage) in [
        (false, RouteMode::Flow, "udp_policy"),
        (true, RouteMode::Packet, "data_plane_route"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = proxy(listener.local_addr().unwrap().port(), enabled).await;
        let config = proxy.engine().runtime_snapshot().config().clone();
        let (resolved, _plan) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("node".into()))
            .unwrap();
        let session = Session::new(
            0,
            Address::Ipv4([203, 0, 113, 9]),
            53,
            Network::Udp,
            ProtocolType::new("test"),
        );
        let failure = proxy
            .protocols
            .prepare_udp_outbound(
                UdpAdapterContext::new(&config, proxy.udp_runtime_services()),
                &session,
                &resolved,
                b"query",
                mode,
            )
            .err()
            .expect("policy denies before any I/O");
        assert_eq!(failure.stage, stage);
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
    }
}

//! Actual ordinary-catalog dispatch against the protocol's real wire codec.
#![cfg(feature = "vmess")]
use crate::{
    inventory::{PreparedUdpLeafCandidate, PreparedUdpOutbound},
    protocol_registry::UdpAdapterContext,
    runtime::{pipe::UdpPipeInput, udp_ingress::UdpIngressRuntime},
    Proxy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration, Instant},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{RouteDecision, RouteMode};
#[path = "vmess/fixtures.rs"]
mod fixtures;
use fixtures::*;
#[path = "vmess/admission.rs"]
mod admission;
#[path = "vmess/carriers.rs"]
mod carriers;
#[path = "vmess/grpc.rs"]
mod grpc;
#[path = "vmess/ownership.rs"]
mod ownership;
#[path = "vmess/retirement.rs"]
mod retirement;
#[tokio::test]
async fn ordinary_vmess_datagram_dispatch_preserves_framing_empty_packets_and_flow_tx_for_all_ciphers(
) {
    for (cipher, tag) in [
        ("aes-128-gcm", "node"),
        ("chacha20-poly1305", "choice"),
        ("none", "backup"),
        ("zero", "node"),
        ("zero-plus", "backup"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_port = listener.local_addr().unwrap().port();
        let proxy = proxy(
            upstream_port,
            serde_json::json!({"final":{"type":"route","outbound":tag}}),
            cipher,
        )
        .await;
        let payloads = vec![b"one".to_vec(), vec![], vec![7; 37000]];
        let server = tokio::spawn(echo(listener, cipher.into(), 53, payloads.clone()));
        let config = proxy.engine().runtime_snapshot().config().clone();
        let (resolved, _plan) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("node".into()))
            .unwrap();
        let zero_engine::ResolvedOutbound::Single(leaf) = &resolved else {
            panic!("single leaf");
        };
        let claimed = proxy
            .protocols
            .claim_outbound_leaf(&config, leaf.clone())
            .unwrap();
        assert!(
            claimed.into_udp_flow_for_route(RouteMode::Flow).is_err(),
            "no direct legacy operation for a migrated flow"
        );
        let session = Session::new(
            7,
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
            PreparedUdpOutbound::Single(PreparedUdpLeafCandidate::DatagramFlow(_))
        ));
        let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
        let mut ids = vec![];
        for payload in &payloads {
            ids.push(dispatch.dispatch(input(53, payload)).await.unwrap());
            let (_, tasks, _) = dispatch.poll_sockets();
            let packet = timeout(Duration::from_secs(3), tasks.join_next_with_id())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(
                packet,
                (
                    Address::Ipv4([203, 0, 113, 9]),
                    53,
                    payload.clone(),
                    Some(ids[0])
                )
            );
            let (target, port, payload, id) = packet;
            let response = crate::runtime::udp_delivery::record_chain_udp_response_parts(
                &proxy.udp_runtime_services(),
                &dispatch,
                target,
                port,
                payload,
                id,
            );
            assert_eq!(response.payload.as_slice(), payloads[ids.len() - 1]);
        }
        assert!(ids.iter().all(|id| *id == ids[0]));
        assert_eq!(
            proxy
                .engine()
                .active_sessions()
                .iter()
                .find(|s| s.id == ids[0])
                .unwrap()
                .outbound_tx_bytes,
            12 + 9 + 37009
        );
        assert_eq!(
            proxy
                .engine()
                .active_sessions()
                .iter()
                .find(|s| s.id == ids[0])
                .unwrap()
                .outbound_rx_bytes,
            37003
        );
        let active = proxy.engine().active_sessions();
        let remote = active
            .iter()
            .find(|s| s.id == ids[0])
            .unwrap()
            .path
            .remote
            .as_ref()
            .expect("keep the existing upstream observation");
        assert_eq!(remote.host, "127.0.0.1");
        assert_eq!(remote.port, upstream_port);
        let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
        let completed = dispatch.finish_all();
        assert_eq!(
            completed[0].upstream,
            Some(("127.0.0.1".into(), upstream_port)),
            "completion logs retain their actual upstream"
        );
        let report = retired
            .wait_closed(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert!(!report.closed_io.is_empty());
        server.await.unwrap();
        proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn ordinary_vmess_datagram_flows_keep_separate_carriers_and_reuse_their_own_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(
        listener.local_addr().unwrap().port(),
        serde_json::json!({"final":{"type":"route","outbound":"node"}}),
        "aes-128-gcm",
    )
    .await;
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        let first = tokio::spawn(echo_stream(
            first,
            "aes-128-gcm".into(),
            53,
            vec![b"first".to_vec(), b"again".to_vec()],
        ));
        let (second, _) = listener.accept().await.unwrap();
        let second = tokio::spawn(echo_stream(
            second,
            "aes-128-gcm".into(),
            54,
            vec![b"second".to_vec()],
        ));
        first.await.unwrap();
        second.await.unwrap();
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err(),
            "a repeated flow must reuse its own carrier"
        );
    });
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
    let mut ids = vec![];
    for (port, payload) in [
        (53, b"first".as_slice()),
        (54, b"second".as_slice()),
        (53, b"again".as_slice()),
    ] {
        ids.push(dispatch.dispatch(input(port, payload)).await.unwrap());
        let (_, tasks, _) = dispatch.poll_sockets();
        let packet = timeout(Duration::from_secs(3), tasks.join_next_with_id())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(packet.1, port);
        assert_eq!(packet.2, payload);
        assert_eq!(packet.3, Some(*ids.last().unwrap()));
    }
    assert_eq!(ids[0], ids[2]);
    assert_ne!(ids[0], ids[1]);
    let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
    let _ = dispatch.finish_all();
    assert!(retired
        .wait_closed(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap()
        .is_complete());
    server.await.unwrap();
    proxy
        .protocols
        .shutdown_components(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

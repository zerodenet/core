#![cfg(feature = "socks5")]

// Reuse the retained, actual TCP listeners and real SOCKS5 wire peer fixtures.
use super::*;
use crate::adapters::socks5::relay_tests::fixtures;
use crate::{
    protocol_registry::{OutboundAdapterContext, TcpRuntimeServices},
    Proxy,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Duration},
};
use zero_engine::{ResolvedOutbound, RouteDecision, RouteMode};

#[path = "tests/catalog.rs"]
mod catalog;
#[path = "tests/rejection.rs"]
mod rejection;

fn prepare(proxy: &Proxy, services: &TcpRuntimeServices, legacy: usize) -> PreparedTcpRelayChain {
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("relay".into()))
        .unwrap();
    let ResolvedOutbound::Relay { chain } = resolved else {
        panic!("actual relay group required")
    };
    let execution = services.execution();
    let context = OutboundAdapterContext::new(services.config());
    let mut prepared = proxy
        .protocols
        .prepare_tcp_relay_chain(context, &chain, execution.components.as_deref().ok())
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    let claimed = proxy
        .protocols
        .claim_relay_chain(services.config(), chain, |e| e, |e| e)
        .unwrap();
    assert!(matches!(
        prepared.first.execution,
        PreparedTcpCandidateExecution::Native(_)
    ));
    for hop in &prepared.relay_hops {
        let PreparedTcpRelayExecution::Native(selected) = &hop.execution else {
            panic!("compiled Stream role must precede its legacy operation")
        };
        assert_eq!(
            selected.instance(),
            execution
                .components
                .as_ref()
                .unwrap()
                .configured(&hop.tag)
                .unwrap()
                .instance_id()
        );
    }
    // Actual compatibility executors from the same assembly retain the whole
    // prefix through its last unmigrated node, as production selection does.
    // No protocol or close provider is invented by this fixture.
    prepared.first = proxy
        .protocols
        .prepare_claimed_tcp_candidate(context, claimed.first(), RouteMode::Flow)
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(matches!(
        prepared.first.execution,
        PreparedTcpCandidateExecution::Connect(_)
    ));
    for (index, leaf) in claimed.relay_hops().iter().take(legacy).enumerate() {
        prepared.relay_hops[index] = proxy
            .protocols
            .prepare_claimed_tcp_relay_hop(context, leaf)
            .unwrap();
        assert!(matches!(
            prepared.relay_hops[index].execution,
            PreparedTcpRelayExecution::Legacy(_)
        ));
    }
    prepared
}

#[tokio::test]
async fn mixed_stream_roles_preserve_real_three_hop_wire_halfclose_metadata_and_outer_bytes() {
    timeout(Duration::from_secs(5), async {
        for legacy in 0..3 {
            let (proxy, session, services, listeners) = fixtures::setup(3).await;
            let first_port = listeners[0].local_addr().unwrap().port();
            let prepared = prepare(&proxy, &services, legacy);
            let mut peers = fixtures::servers(listeners);
            let established =
                crate::runtime::tcp_dispatch::relay::dispatch_prepared_tcp_relay_chain(
                    services.execution(),
                    &session,
                    prepared,
                    crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic,
                )
                .await
                .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
            let mut route = crate::transport::extract_tcp_stream(established).unwrap();
            assert_eq!(route.outbound_tag, "node-2");
            assert_eq!(route.relay_chain.len(), 3);
            assert_eq!(
                route.upstream_endpoint,
                Some(("127.0.0.1".into(), first_port))
            );
            let mut prefix = [0; 3];
            route.upstream.read_exact(&mut prefix).await.unwrap();
            assert_eq!(&prefix, b"pre");
            route.upstream.write_all(b"request").await.unwrap();
            route.upstream.shutdown().await.unwrap();
            let mut tail = Vec::new();
            route.upstream.read_to_end(&mut tail).await.unwrap();
            assert_eq!(tail, b"tail");
            while let Some(peer) = peers.join_next().await {
                peer.unwrap();
            }
            for index in 0..3 {
                let traffic = proxy
                    .engine()
                    .traffic_snapshot(&zero_api::TrafficGetQuery {
                        scope: zero_api::TrafficScope::Outbound {
                            tag: format!("node-{index}"),
                        },
                    })
                    .unwrap();
                let outer = &traffic.planes[2].counters;
                let remaining = (3 - index) as u64;
                assert_eq!(
                    (outer.tx_bytes, outer.rx_bytes),
                    (
                        Some(remaining * 13 + (remaining - 1) * 6 + 7),
                        Some(remaining * 12 + 7)
                    )
                );
            }
            drop(route);
            proxy
                .protocols
                .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        }
    })
    .await
    .expect("actual mixed relays must finish");
}

#[tokio::test]
async fn mixed_retired_stream_role_blocks_the_real_prefix_before_any_socket_opens() {
    for revoked in [1, 2] {
        let (proxy, session, services, listeners) = fixtures::setup(3).await;
        let prepared = prepare(&proxy, &services, 0);
        let slot = services
            .execution()
            .components
            .unwrap()
            .configured(&format!("node-{revoked}"))
            .unwrap();
        slot.revoke();
        let failure = match crate::runtime::tcp_dispatch::relay::dispatch_prepared_tcp_relay_chain(
            services.execution(),
            &session,
            prepared,
            crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic,
        )
        .await
        {
            Ok(_) => panic!("retired Stream role opened a selected prefix"),
            Err(failure) => failure,
        };
        let original = crate::transport::channel_failure(&failure.error).unwrap();
        assert_eq!(original.component, Some(slot.id));
        assert_eq!(
            original.session,
            Some(zero_core::component::SessionId(session.id))
        );
        assert_eq!(
            original.reason,
            zero_platform_tokio::contract::Reason::Revoked
        );
        assert_eq!(
            crate::transport::failure_origin(&failure.error),
            Some(crate::transport::TransportFailureOrigin::LocalNetwork)
        );
        for listener in listeners {
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(listener.poll_accept(&mut cx).is_pending());
        }
        proxy
            .protocols
            .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn mixed_cached_standard_hop_does_not_keep_the_actual_retired_owner_alive() {
    let (proxy, _session, services, listeners) = fixtures::setup(3).await;
    let prepared = prepare(&proxy, &services, 0);
    let PreparedTcpRelayExecution::Native(selected) = &prepared.relay_hops[1].execution else {
        panic!("standard final role required")
    };
    let selected = selected.clone();
    let slot = selected.slot().unwrap();
    let original = std::sync::Arc::downgrade(&slot);
    let instance = slot.instance_id();
    // No carrier has been opened. Consume the real host lifecycle cleanup;
    // disappearance of a Weak is never offered as native I/O close evidence.
    proxy
        .protocols
        .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    drop(slot);
    drop(services);
    drop(proxy);
    drop(listeners);
    assert_eq!(selected.instance(), instance);
    assert!(
        original.upgrade().is_none(),
        "cached hop retained its original factory/owner"
    );
    assert!(selected.slot().is_err());
    drop(prepared);
}

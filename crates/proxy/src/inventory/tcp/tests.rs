#![cfg(feature = "socks5")]

// Reuse the retained, actual TCP listeners and real SOCKS5 wire peer fixtures.
use super::*;
use crate::adapters::socks5::relay_tests::fixtures;
use crate::{
    protocol_registry::{OutboundAdapterContext, TcpRuntimeServices},
    Proxy,
};
use tokio::time::Duration;
use zero_engine::{ResolvedOutbound, RouteDecision, RouteMode};

#[path = "tests/catalog.rs"]
mod catalog;
#[path = "tests/standard_selection.rs"]
mod standard_selection;

fn prepare(
    proxy: &Proxy,
    services: &TcpRuntimeServices,
) -> (PreparedTcpRelayPrefix, PreparedTcpRelayHop) {
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("relay".into()))
        .unwrap();
    let ResolvedOutbound::Relay { chain } = resolved else {
        panic!("actual relay group required")
    };
    let execution = services.execution();
    let context = OutboundAdapterContext::new(services.config());
    let (prepared, final_hop) = proxy
        .protocols
        .prepare_tcp_relay(context, &chain, execution.components.as_deref().ok())
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(matches!(
        prepared.first.execution,
        PreparedTcpCandidateExecution::Native(_)
    ));
    for hop in prepared
        .relay_hops
        .iter()
        .chain(std::iter::once(&final_hop))
    {
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
    (prepared, final_hop)
}

#[tokio::test]
async fn mixed_retired_stream_role_blocks_the_real_prefix_before_any_socket_opens() {
    for revoked in [1, 2] {
        let (proxy, session, services, listeners) = fixtures::setup(3).await;
        let prepared = prepare(&proxy, &services);
        let slot = services
            .execution()
            .components
            .unwrap()
            .configured(&format!("node-{revoked}"))
            .unwrap();
        slot.revoke();
        let failure = match crate::runtime::tcp_dispatch::relay::dispatch_tcp_relay(
            services.execution(),
            &session,
            prepared.0,
            prepared.1,
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
    let prepared = prepare(&proxy, &services);
    let PreparedTcpRelayExecution::Native(selected) = &prepared.1.execution else {
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

//! Aggregate proof: configured DNS -> actual host DoH pool -> real native provider ->
//! actual native SOCKS role -> actual TLS/H2 DNS peer. No substitute consumer.
use super::*;
use std::{io, sync::atomic::Ordering, time::Duration};
use zero_dns::test_support::doh_pool_witness;
mod fixture;
use fixture::Harness;
fn until() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}

#[tokio::test]
async fn selector_change_drains_actual_doh_users_and_driver_before_opening_the_new_socks_carrier() {
    let fixture = Harness::new(false).await;
    assert_eq!(
        fixture.query("warm.selector.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 0);
    assert_eq!(
        zero_dns::test_support::install_doh_ca(
            &fixture.proxy.resolver,
            "query",
            fixture.dns.certificate.clone()
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::WouldBlock,
        "fixture CA cannot replace a live or previously used TLS backend"
    );
    let old = fixture.pool_original().await;
    let pending = tokio::spawn({
        let resolver = fixture.proxy.resolver.clone();
        async move { resolver.resolve_real_type("pending.selector.test", 1).await }
    });
    tokio::time::timeout_at(until(), fixture.dns.pending.notified())
        .await
        .unwrap();
    let active = doh_pool_witness(&fixture.proxy.resolver, "query").unwrap();
    assert_eq!(active.registered_driver_tasks, 1);
    assert_eq!(active.active_leases, 1);
    fixture
        .proxy
        .engine
        .set_selector_target("choose", "two")
        .unwrap();
    // The next open must consume the original driver and native/borrowed
    // receipts; remote EOF is not substituted for local retirement.
    assert_eq!(
        fixture.query("new.selector.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    let source = tokio::time::timeout_at(until(), pending)
        .await
        .unwrap()
        .unwrap()
        .expect_err("the real pending old H2 user must exit");
    let mut native: &(dyn std::error::Error + 'static) = &source;
    while native.downcast_ref::<h2::Error>().is_none()
        && native
            .downcast_ref::<zero_engine::GroupSelectionChanged>()
            .is_none()
    {
        native = native.downcast_ref::<io::Error>()
            .and_then(|source| source.get_ref().map(|source| source as &(dyn std::error::Error + 'static)))
            .or_else(|| native.source())
            .unwrap_or_else(|| panic!("original H2 or actual selection revocation source must remain reachable: {source:?}"));
    }
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 1);
    assert!(fixture.connector.witnesses.lock().unwrap().is_empty());
    assert_retired(&old);
    fixture.socks[0].counts.wait_closed(1).await;
    fixture.dns.counts.wait_closed(1).await;
    assert_eq!(
        fixture.query("reuse.selector.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    assert_eq!(
        fixture.connector.opens.load(Ordering::Acquire),
        0,
        "the host path must never enter the legacy connector"
    );
    fixture.assert_no_business_sessions();
    fixture.finish().await;
}

#[tokio::test]
async fn actual_doh_pool_reuse_does_not_advance_round_robin_and_the_next_connection_picks_once() {
    let fixture = Harness::new(true).await;
    for index in 0..crate::component::native_dns::LIMIT + 2 {
        let name = format!("reuse-{index}.balance.test");
        assert_eq!(
            fixture.query(&name).await,
            vec![IpAddress::V4([198, 51, 100, 79])]
        );
    }
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 0);
    // A real physical-network event invalidates this carrier. Pure reuse
    // checks must not have consumed any intermediate load-balance picks.
    let old = fixture.pool_original().await;
    fixture.proxy.egress_interface.invalidate_network();
    assert_eq!(
        fixture.query("next.balance.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.socks[1].counts.accepted.load(Ordering::Acquire),
        1,
        "the second actual connection must pick the second round-robin member"
    );
    assert_eq!(
        fixture.query("last.balance.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    assert_eq!(fixture.dns.counts.accepted.load(Ordering::Acquire), 2);
    assert_retired(&old);
    fixture.assert_no_business_sessions();
    fixture.finish().await;
}

fn assert_retired(old: &zero_dns::OwnedDnsQueryObservation) {
    assert!(
        old.task.is_joined(),
        "same original Scope must consume the driver join"
    );
    let original = old.original.as_ref().unwrap();
    assert!(original.closed().unwrap().matches(original));
    let native = original.native_origin();
    assert!(native.closed().unwrap().matches(native));
    assert!(native.closed().unwrap().had_native_acquisition());
    assert!(
        !original.closed().unwrap().had_native_acquisition(),
        "the DNS host returns its actual loan instead of claiming the donor socket"
    );
    let donor = original
        .parent()
        .expect("actual native SOCKS donor lineage");
    assert!(donor.same_original(native));
    assert!(!original.owner().same_owner(native.owner()));
    assert!(
        native.driver().is_none(),
        "the actual SOCKS role is taskless"
    );
    assert!(donor.closed().unwrap().matches(donor));
}

#[tokio::test]
async fn actual_proxy_detour_replacement_confirms_old_dns_and_socks_originals_before_reopening() {
    let fixture = Harness::new(false).await;
    fixture.query("before.replacement.test").await;
    let old = fixture.pool_original().await;
    let mut config = fixture
        .proxy
        .engine
        .runtime_snapshot()
        .config()
        .as_ref()
        .clone();
    let zero_config::OutboundProtocolConfig::Socks5 { port, .. } =
        &mut config.outbounds[0].protocol
    else {
        panic!("actual SOCKS fixture")
    };
    *port = fixture.socks[1].endpoint.port();
    fixture
        .handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    assert_retired(&old);
    assert_eq!(
        fixture.socks[1].counts.accepted.load(Ordering::Acquire),
        0,
        "candidate must remain cold until old originals close"
    );
    zero_dns::test_support::install_doh_ca(
        &fixture.proxy.resolver,
        "query",
        fixture.dns.certificate.clone(),
    )
    .unwrap();
    fixture.query("after.replacement.test").await;
    let next = fixture.pool_original().await;
    assert!(!old.task.same_registration(&next.task));
    assert!(!old
        .original
        .as_ref()
        .unwrap()
        .same_original(next.original.as_ref().unwrap()));
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    fixture.assert_no_business_sessions();
    fixture.finish().await;
}

#[tokio::test]
async fn cancelling_real_pending_detoured_doh_setup_confirms_the_original_before_retry() {
    let fixture = Harness::with_paused_peer(false, true).await;
    let waiting = tokio::spawn({
        let resolver = fixture.proxy.resolver.clone();
        async move { resolver.resolve_real_type("cancel.original.test", 1).await }
    });
    tokio::time::timeout_at(until(), fixture.socks[0].greeting.notified())
        .await
        .unwrap();
    let old = fixture.pool_original().await;
    assert!(!waiting.is_finished());
    let original = old.original.as_ref().unwrap();
    assert!(original.closed().is_none());
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    fixture
        .proxy
        .engine
        .set_selector_target("choose", "two")
        .unwrap();
    fixture.query("retry.original.test").await;
    assert!(old.task.is_joined());
    assert!(original.closed().unwrap().matches(original));
    assert!(original.closed().unwrap().had_native_acquisition());
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn detour_candidate_failure_confirms_its_original_before_the_same_driver_tries_next() {
    let fixture = Harness::with_paused_peer(true, true).await;
    let waiting = tokio::spawn({
        let resolver = fixture.proxy.resolver.clone();
        async move {
            resolver
                .resolve_real_type("fallback.original.test", 1)
                .await
        }
    });
    tokio::time::timeout_at(until(), fixture.socks[0].greeting.notified())
        .await
        .unwrap();
    let old = fixture.pool_original().await;
    let original = old.original.as_ref().unwrap();
    assert!(original.closed().is_none());
    fixture.socks[0].reject_greeting.notify_one();
    tokio::time::timeout_at(until(), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(original.closed().unwrap().matches(original));
    assert!(original.closed().unwrap().had_native_acquisition());
    let next = fixture.pool_original().await;
    assert!(
        old.task.same_registration(&next.task),
        "there was no independent H2 driver before setup succeeded"
    );
    assert!(
        !old.task.is_joined(),
        "native close cannot fabricate a join for the still-running real driver"
    );
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 0);
    fixture.finish().await;
}

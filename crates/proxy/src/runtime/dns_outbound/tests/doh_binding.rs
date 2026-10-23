//! Aggregate proof: configured DNS -> real DoH pool -> real Proxy connector ->
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
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 1);
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
    // Start the next query immediately. ObservedConnector reads the actual
    // owner at the new-open boundary and requires both counts to be zero.
    // Remote EOF and outer caller polling order are deliberately not used as
    // substitutes for this local driver/user exit evidence.
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
    while native.downcast_ref::<h2::Error>().is_none() {
        native = native
            .source()
            .expect("original H2 source must remain reachable");
    }
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 2);
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.connector.witnesses.lock().unwrap().len(), 2);
    fixture.socks[0].counts.wait_closed(1).await;
    fixture.dns.counts.wait_closed(1).await;
    assert_eq!(
        fixture.query("reuse.selector.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    assert_eq!(
        fixture.connector.opens.load(Ordering::Acquire),
        2,
        "the current carrier remains reusable"
    );
    fixture.assert_no_business_sessions();
    fixture.finish().await;
}

#[tokio::test]
async fn actual_doh_pool_reuse_does_not_advance_round_robin_and_the_next_connection_picks_once() {
    let fixture = Harness::new(true).await;
    for name in [
        "first.balance.test",
        "reuse.balance.test",
        "again.balance.test",
    ] {
        assert_eq!(
            fixture.query(name).await,
            vec![IpAddress::V4([198, 51, 100, 79])]
        );
    }
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[0].counts.accepted.load(Ordering::Acquire), 1);
    assert_eq!(fixture.socks[1].counts.accepted.load(Ordering::Acquire), 0);
    // A real physical-network event invalidates this carrier. Pure reuse
    // checks must not have consumed any intermediate load-balance picks.
    fixture.proxy.egress_interface.invalidate_network();
    assert_eq!(
        fixture.query("next.balance.test").await,
        vec![IpAddress::V4([198, 51, 100, 79])]
    );
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 2);
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
    assert_eq!(fixture.connector.opens.load(Ordering::Acquire), 2);
    assert_eq!(fixture.dns.counts.accepted.load(Ordering::Acquire), 2);
    fixture.assert_no_business_sessions();
    fixture.finish().await;
}

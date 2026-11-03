//! Real erased factory/slot/scope registration, without protocol substitutes.
use super::*;
use std::num::NonZeroUsize;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::lifecycle::CloseReport;
use zero_platform_tokio::contract::scope::TaskJoinWitness;
use zero_platform_tokio::controlled_datagram::metadata::{MetadataLimits, MetadataTransmit};
use zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolPermit;
#[path = "resource_metapool/fixture.rs"]
mod fixture;
use fixture::Fixture;
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: n(1),
        probe_sockets: n(1),
        probe_attempts_per_initialization: n(4),
        concurrent_send_calls: n(1),
        write_pollers: n(1),
    }
}
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(4)
}
fn open(
    permit: &MetadataPoolPermit,
    query: &TaskJoinWitness,
) -> zero_platform_tokio::contract::component::GrantedMetadataListener {
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    permit
        .open(
            query,
            &egress,
            NetworkGeneration(egress.generation()),
            end(),
        )
        .unwrap()
}
#[tokio::test]
async fn actual_metadata_pool_requires_its_scope_and_returns_the_same_unused_original_before_capacity_reuse(
) {
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let a = Fixture::new(peer.local_addr().unwrap(), 1).await;
    let b = Fixture::new(peer.local_addr().unwrap(), 1).await;
    let (query, release) = a.query();
    let (foreign, foreign_release) = b.query();
    assert!(a.grants.reserve(&foreign).is_err());
    let permit = a.grants.reserve(&query).unwrap();
    let original = permit.origin();
    let (other, other_release) = a.query();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    assert!(permit
        .open(
            &other,
            &egress,
            NetworkGeneration(egress.generation()),
            end()
        )
        .is_err());
    assert!(original.endpoints().is_none());
    let mut grant = open(&permit, &query);
    assert!(a.grants.reserve(&query).is_err());
    assert!(permit.close(None, &a.tasks).is_err());
    let mut claim = grant.users_claim.take();
    let retained = grant.tx.clone();
    drop(grant);
    permit.return_unbound(&mut claim, &a.tasks).unwrap();
    assert!(permit.close(None, &b.tasks).is_err());
    assert!(permit.close(None, &a.tasks).is_err());
    assert!(original.closed().is_none());
    drop(retained);
    let closed = permit.close(None, &a.tasks).unwrap();
    assert!(closed.matches(&original) && closed.had_native_acquisition());
    assert_eq!(a.grants.retained(), Some(0));
    let next = a.grants.reserve(&query).unwrap();
    assert!(!original.same_original(&next.origin()));
    assert!(!closed.matches(&next.origin()));
    assert!(!next.close(None, &a.tasks).unwrap().had_native_acquisition());
    release.send(()).unwrap();
    foreign_release.send(()).unwrap();
    other_release.send(()).unwrap();
    a.drive(query.wait()).await;
    a.drive(other.wait()).await;
    b.drive(foreign.wait()).await;
    assert!(a.finish().await.is_complete());
    assert!(b.finish().await.is_complete());
}
#[tokio::test]
async fn actual_metadata_pool_keeps_original_views_and_prevents_native_send_outside_its_declared_peer(
) {
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let foreign = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let fixture = Fixture::new(peer.local_addr().unwrap(), 1).await;
    let (query, release) = fixture.query();
    let permit = fixture.grants.reserve(&query).unwrap();
    let original = permit.origin();
    let mut grant = open(&permit, &query);
    let result = grant
        .tx
        .try_send(&MetadataTransmit {
            destination: foreign.local_addr().unwrap(),
            contents: b"denied",
            source_ip: None,
            ecn: None,
            segment_size: None,
        })
        .unwrap_err();
    assert_eq!(result.kind(), std::io::ErrorKind::PermissionDenied);
    let mut claim = grant.users_claim.take();
    drop(grant);
    permit.return_unbound(&mut claim, &fixture.tasks).unwrap();
    release.send(()).unwrap();
    fixture.drive(query.wait()).await;
    assert!(permit
        .close(None, &fixture.tasks)
        .unwrap()
        .matches(&original));
    assert!(fixture.finish().await.is_complete());
    assert!(fixture.grants.reserve(&query).is_err());
}
#[tokio::test]
async fn actual_metadata_pool_reuses_confirmed_capacity_without_a_lifetime_bind_budget_or_private_history(
) {
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let fixture = Fixture::new(peer.local_addr().unwrap(), 1).await;
    let (query, release) = fixture.query();
    let mut previous = None;
    for _ in 0..32 {
        let permit = fixture.grants.reserve(&query).unwrap();
        let origin = permit.origin();
        let mut grant = open(&permit, &query);
        let mut claim = grant.users_claim.take();
        drop(grant);
        permit.return_unbound(&mut claim, &fixture.tasks).unwrap();
        assert!(permit
            .close(None, &fixture.tasks)
            .unwrap()
            .had_native_acquisition());
        if let Some(prior) = previous.take() {
            let prior: zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin =
                prior;
            assert!(prior.closed().unwrap().matches(&prior));
            assert!(prior.endpoints().is_none());
        }
        previous = Some(origin);
        drop(permit);
        assert_eq!(fixture.grants.retained(), Some(0));
    }
    release.send(()).unwrap();
    fixture.drive(query.wait()).await;
    assert!(fixture.finish().await.is_complete());
}

#[tokio::test]
async fn rejected_unbound_claim_keeps_original_custody_and_does_not_poison_the_other_slot() {
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let a = Fixture::new(peer.local_addr().unwrap(), 2).await;
    let b = Fixture::new(peer.local_addr().unwrap(), 1).await;
    let (query, release) = a.query();
    let first = a.grants.reserve(&query).unwrap();
    let second = a.grants.reserve(&query).unwrap();
    let mut first_grant = open(&first, &query);
    let mut second_grant = open(&second, &query);
    let mut first_claim = first_grant.users_claim.take();
    let mut second_claim = second_grant.users_claim.take();
    drop(first_grant);
    drop(second_grant);
    assert!(first.return_unbound(&mut first_claim, &b.tasks).is_err());
    assert!(
        first_claim.is_some(),
        "foreign scope preserves unique original claim"
    );
    assert!(first.return_unbound(&mut second_claim, &a.tasks).is_err());
    assert!(
        second_claim.is_some(),
        "foreign original preserves its unique claim"
    );
    first.return_unbound(&mut first_claim, &a.tasks).unwrap();
    assert!(first_claim.is_none());
    assert!(first.return_unbound(&mut second_claim, &a.tasks).is_err());
    assert!(
        second_claim.is_some(),
        "already-returned slot does not consume another original"
    );
    second.return_unbound(&mut second_claim, &a.tasks).unwrap();
    assert!(second_claim.is_none());
    let first_closed = first.close(None, &a.tasks).unwrap();
    let second_closed = second.close(None, &a.tasks).unwrap();
    assert!(first_closed.matches(&first.origin()));
    assert!(second_closed.matches(&second.origin()));
    assert!(!first_closed.matches(&second.origin()));
    assert_eq!(a.grants.retained(), Some(0));
    release.send(()).unwrap();
    a.drive(query.wait()).await;
    assert!(a.finish().await.is_complete());
    assert!(b.finish().await.is_complete());
}

use super::*;
use std::{net::Ipv4Addr, time::Duration};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{lifecycle::ResourceLifecycle, CloseReason};
#[path = "icmp_resources/native.rs"]
mod native;
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(641),
        generation: ResourceGeneration(9),
    }
}
fn declaration() -> ResourceDeclaration {
    ResourceDeclaration::NativeIcmp {
        max_sockets: NonZeroUsize::new(1).unwrap(),
    }
}
fn owner(cancel: CancellationToken) -> HostIcmpProbeOwner {
    HostIcmpProbeOwner::new(instance(), &declaration(), cancel).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn cold_revoked_and_expired_admission_never_acquire_native_icmp() {
    let mut owner = owner(CancellationToken::new());
    let grant = owner.grant();
    let failure = grant.bind(Ipv4Addr::LOCALHOST.into(), None).err().unwrap();
    assert_eq!(failure.reason, Reason::InvalidConfig);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert!(owner.start(Instant::now()).await.is_err());
    assert!(
        !owner.physical_confirmed(),
        "open admission cannot seal physical proof"
    );
    owner.revoke(CloseReason::Shutdown);
    assert_eq!(
        grant
            .bind(Ipv4Addr::LOCALHOST.into(), None)
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    assert!(owner.start(deadline()).await.is_err());
    assert!(
        owner.physical_confirmed(),
        "this exact pool acquired no originals"
    );
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "no acquisition is not one native close"
    );
    assert!(owner.take_closed_io().closed_io.is_empty());
}
#[tokio::test]
async fn missing_owner_wrong_generation_and_outer_reentry_are_rejected_without_io() {
    let mut owner = owner(CancellationToken::new());
    owner.start(deadline()).await.unwrap();
    let grant = owner.grant();
    let mut wrong = grant.clone();
    wrong.instance.generation = ResourceGeneration(10);
    assert_eq!(
        wrong
            .bind(Ipv4Addr::LOCALHOST.into(), None)
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    {
        let _held = owner.shared.state();
        let error = grant.bind(Ipv4Addr::LOCALHOST.into(), None).err().unwrap();
        assert_eq!(error.reason, Reason::Capacity);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert!(!owner.physical_confirmed());
    }
    assert!(owner
        .shutdown(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
    drop(owner);
    assert_eq!(
        grant
            .bind(Ipv4Addr::LOCALHOST.into(), None)
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
}
#[test]
fn native_icmp_requires_its_own_canonical_declaration_and_parent_fence() {
    assert!(HostIcmpProbeOwner::new(
        instance(),
        &ResourceDeclaration::BackgroundCarrier,
        CancellationToken::new()
    )
    .is_err());
    let parent = CancellationToken::new();
    let owner = owner(parent.clone());
    let grant = owner.grant();
    parent.cancel();
    assert_eq!(
        grant
            .bind(Ipv4Addr::LOCALHOST.into(), None)
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    assert!(owner.physical_confirmed());
    assert!(owner.take_closed_io().closed_io.is_empty());
}

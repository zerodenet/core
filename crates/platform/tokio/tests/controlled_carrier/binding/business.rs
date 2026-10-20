//! Capability negatives only: these self-bound library owners are not host admission.
use super::*;

#[tokio::test]
async fn business_issuer_requires_original_host_fence_and_actual_reserved_owner() {
    let mut fixture = Fixture::cold(1).await;
    let mut foreign = Fixture::cold(1).await;
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let host = Arc::new(CancellationToken::new());
    let copied = Arc::new(host.as_ref().clone());
    let registration = fixture.owner.registration();
    let binding = registration
        .bind_declared(
            &declaration(&fixture, ControlledStreamDemandClass::Business),
            &scope.registrar(),
            &host,
        )
        .unwrap();
    fixture.owner.start(deadline()).await.unwrap();
    foreign.owner.start(deadline()).await.unwrap();
    let original = fixture.grants.reserve().unwrap();
    let another_owner = foreign.grants.reserve().unwrap();
    assert_eq!(
        binding
            .business_issuer(&copied, &original)
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    assert_eq!(
        binding
            .business_issuer(&host, &another_owner)
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    // Same numerical component ID and token cancellation state grant no authority.
    assert_eq!(original.instance(), another_owner.instance());
    assert!(!copied.is_cancelled());
    assert!(!host.is_cancelled());
    let issuer = binding.business_issuer(&host, &original).unwrap();
    host.cancel();
    assert_eq!(
        binding
            .business_issuer(&host, &original)
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    drop(issuer);
    drop(original);
    drop(another_owner);
    assert_eq!(fixture.owner.retained(), 0);
    assert_eq!(foreign.owner.retained(), 0);
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(foreign
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

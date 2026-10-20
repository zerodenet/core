use super::*;

#[tokio::test]
async fn identity_matches_only_the_same_original_live_registration_authority() {
    let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let registrar = owner.registrar();
    let cloned = registrar.clone();
    let identity = registrar.scope_identity();
    let identity_clone = identity.clone();
    let foreign = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    assert!(identity.matches(&registrar));
    assert!(identity_clone.matches(&cloned));
    assert!(!identity.matches(&foreign.registrar()));
    assert!(!identity.registration_open(), "build is still suspended");
    owner.enable_registration().unwrap();
    assert!(identity.registration_open());
    let (id, witness) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let receipt = owner.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), id);
    assert!(
        identity.registration_open(),
        "admission is not spare capacity"
    );
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    assert_eq!(witness.wait().await.id(), id);
    owner.begin_shutdown();
    assert!(!identity.registration_open());
    assert!(
        identity.matches(&registrar),
        "closing preserves actual identity"
    );
    assert!(
        !owner.is_drained(),
        "identity is not a delivery/close proof"
    );
    owner.acknowledge(&receipt).unwrap();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn identity_is_weak_and_never_turns_empty_or_expired_scopes_into_authority() {
    let mut empty = TaskScope::suspended_hosted(Handle::current(), None);
    let empty_identity = empty.registrar().scope_identity();
    empty.enable_registration().unwrap();
    assert!(!empty_identity.registration_open());
    empty.begin_shutdown();
    assert!(empty.is_drained());

    let expired = {
        let original = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
        let registrar = original.registrar();
        let identity = registrar.scope_identity();
        assert!(identity.registration_open());
        assert_eq!(Arc::strong_count(&original.0), 2);
        let copy = identity.clone();
        assert_eq!(Arc::strong_count(&original.0), 2);
        drop(copy);
        drop(registrar);
        assert_eq!(Arc::strong_count(&original.0), 1);
        identity
    };
    assert!(
        expired.0.upgrade().is_none(),
        "metadata cannot retain the old Scope"
    );
    assert!(!expired.registration_open());
    for _ in 0..32 {
        let new = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
        assert!(!expired.matches(&new.registrar()));
        assert!(!expired.registration_open());
    }
}

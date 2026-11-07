use super::*;

fn auth() -> SessionAuth {
    let mut auth = SessionAuth::new("test");
    auth.principal_key = Some("retained-quota-owner".to_owned());
    auth.quota_remaining_bytes = Some(100);
    auth.policy_revision = Some(1);
    auth
}

#[test]
fn active_registration_retains_actual_worker_and_native_lease_after_registry_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.json");
    let source = Arc::new(ConfigurationSource::default());
    let registry = PrincipalQuotaRegistry::open(Some(path.clone()), source.clone()).unwrap();
    assert!(!registry.retirement_complete());
    let registration = registry.acquire(Some(&auth())).unwrap().unwrap();
    registration.consume(7);
    assert_eq!(
        registry.shutdown().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(registry.acquire(Some(&auth())).is_err());
    drop(registry);
    assert_eq!(
        Arc::new(ConfigurationSource::default())
            .open_quota_storage(path.clone())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(source.confirm_quota_storage_closed().is_err());
    registration.consume(3);
    drop(registration);
    source.confirm_quota_storage_closed().unwrap();
    let restored =
        PrincipalQuotaRegistry::open(Some(path), Arc::new(ConfigurationSource::default())).unwrap();
    assert_eq!(
        restored
            .inner
            .lock()
            .unwrap()
            .states
            .values()
            .next()
            .unwrap()
            .remaining_bytes,
        90
    );
    restored.shutdown().unwrap();
}

#[test]
fn explicit_shutdown_joins_original_worker_before_matching_native_lease_close() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.json");
    let source = Arc::new(ConfigurationSource::default());
    let registry = PrincipalQuotaRegistry::open(Some(path.clone()), source.clone()).unwrap();
    assert!(!registry.retirement_complete());
    let registration = registry.acquire(Some(&auth())).unwrap().unwrap();
    registration.consume(17);
    drop(registration);
    registry.shutdown().unwrap();
    registry.shutdown().unwrap();
    assert!(registry.retirement_complete());
    let owner = registry.persistence.as_ref().unwrap();
    let worker = owner.worker.lock().unwrap();
    assert!(worker.original.is_none());
    assert!(matches!(worker.joined, Some(Ok(()))));
    drop(worker);
    source.confirm_quota_storage_closed().unwrap();
    source.confirm_original_io_closed().unwrap();
    let reopened =
        PrincipalQuotaRegistry::open(Some(path), Arc::new(ConfigurationSource::default())).unwrap();
    assert_eq!(
        reopened
            .inner
            .lock()
            .unwrap()
            .states
            .values()
            .next()
            .unwrap()
            .remaining_bytes,
        83
    );
    reopened.shutdown().unwrap();
}

#[test]
fn last_registration_returns_real_worker_fault_to_same_source_before_a_fresh_native_lease() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.json");
    let source = Arc::new(ConfigurationSource::default());
    let registry = PrincipalQuotaRegistry::open(Some(path.clone()), source.clone()).unwrap();
    let owner = registry.persistence.as_ref().unwrap();
    let actual_worker = owner.worker.clone();
    let storage = owner.storage.clone();
    let book = registry.inner.clone();
    // The actual old snapshot is absent. Its real final rename must fail at
    // this directory, not in a fake worker or synthesized native-close result.
    std::fs::create_dir(&path).unwrap();
    let registration = registry.acquire(Some(&auth())).unwrap().unwrap();
    registration.consume(7);
    drop(registry);
    registration.consume(3);
    assert_eq!(
        book.lock()
            .unwrap()
            .states
            .values()
            .next()
            .unwrap()
            .remaining_bytes,
        90
    );
    drop(registration); // Last original owner drives std.join, then lock close.
    let worker = actual_worker.lock().unwrap();
    assert!(worker.original.is_none());
    let original = worker
        .joined
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap_err()
        .clone();
    drop(worker);
    assert!(Arc::ptr_eq(
        &storage.retained_worker_failure().unwrap(),
        &original
    ));
    assert_eq!(
        book.lock()
            .unwrap()
            .states
            .values()
            .next()
            .unwrap()
            .references,
        0
    );
    source.confirm_quota_storage_closed().unwrap();
    source.confirm_original_io_closed().unwrap();
    let observed = PrincipalQuotaRegistry::open(Some(path.clone()), source.clone()).unwrap_err();
    fn contains(actual: &(dyn std::error::Error + 'static), original: &io::Error) -> bool {
        if actual
            .downcast_ref::<io::Error>()
            .is_some_and(|error| std::ptr::eq(error, original))
        {
            return true;
        }
        actual
            .source()
            .is_some_and(|source| contains(source, original))
    }
    assert!(
        contains(&observed, original.as_ref()),
        "same actual joined error, not its Display text: {observed}"
    );
    source.confirm_quota_storage_closed().unwrap();
    std::fs::remove_dir(&path).unwrap();
    let fresh = PrincipalQuotaRegistry::open(Some(path), source.clone()).unwrap();
    assert!(!Arc::ptr_eq(
        &fresh.persistence.as_ref().unwrap().storage,
        &storage
    ));
    fresh.shutdown().unwrap();
    source.confirm_quota_storage_closed().unwrap();
    assert!(
        contains(&observed, original.as_ref()),
        "old typed fault receiver survives the fresh generation"
    );
}

#[test]
fn explicitly_received_worker_fault_is_not_unreported_again_by_later_fallback_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quota.json");
    let source = Arc::new(ConfigurationSource::default());
    let registry = PrincipalQuotaRegistry::open(Some(path.clone()), source.clone()).unwrap();
    std::fs::create_dir(&path).unwrap();
    let registration = registry.acquire(Some(&auth())).unwrap().unwrap();
    registration.consume(17);
    drop(registration);
    let original = registry.shutdown().unwrap_err();
    assert!(registry.retirement_complete());
    source.confirm_quota_storage_closed().unwrap();
    source.confirm_original_io_closed().unwrap();
    drop(registry);
    std::fs::remove_dir(&path).unwrap();
    let fresh = PrincipalQuotaRegistry::open(Some(path), source).unwrap();
    fresh.shutdown().unwrap();
    assert!(
        std::error::Error::source(&original).is_some(),
        "the explicitly received typed original survives recovery"
    );
}

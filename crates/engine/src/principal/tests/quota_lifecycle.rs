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

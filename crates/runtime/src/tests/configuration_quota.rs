use super::*;

#[test]
fn quota_lease_and_snapshot_keep_actual_originals_and_do_not_write_the_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.json");
    let path = directory.path().join("quota.json");
    let source = Arc::new(ConfigurationSource::new(&config));
    let storage = source.open_quota_storage(&path).unwrap();
    assert!(storage.read().unwrap().is_none());
    storage.replace(br#"{"version":1,"balances":[]}"#).unwrap();
    assert_eq!(
        storage.read().unwrap().unwrap(),
        br#"{"version":1,"balances":[]}"#
    );
    let parsed = storage
        .read_with(|reader| {
            serde_json::from_reader::<_, serde_json::Value>(reader).map_err(io::Error::other)
        })
        .unwrap()
        .unwrap();
    assert_eq!(parsed["version"], 1);
    assert!(!config.exists());
    assert_eq!(source.path(), Some(config.as_path()));
    source.confirm_original_io_closed().unwrap();
    assert!(source.confirm_quota_storage_closed().is_err());
    assert!(
        source.open_quota_storage(&path).is_err(),
        "one source cannot lend its lease to another accounting registry"
    );
    storage.close_lease().unwrap();
    {
        let lease = storage.lease.lock().unwrap();
        assert!(lease
            .original
            .as_ref()
            .unwrap()
            .matches_closed(lease.closed.as_ref().unwrap()));
    }
    source.confirm_quota_storage_closed().unwrap();
    assert_eq!(
        storage.replace(b"bad").unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    let next = source.open_quota_storage(&path).unwrap();
    next.close_lease().unwrap();
    source.confirm_quota_storage_closed().unwrap();
}

#[test]
fn quota_snapshot_decode_failure_returns_only_after_the_same_original_file_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quota.json");
    std::fs::write(&path, b"not-json").unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let (bytes, decoded) = source
        .read_quota_snapshot(&path, |reader| {
            serde_json::from_reader::<_, serde_json::Value>(reader)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        })
        .unwrap();
    assert_eq!(bytes, 8);
    let error = decoded.unwrap_err();
    assert!(error
        .get_ref()
        .unwrap()
        .downcast_ref::<serde_json::Error>()
        .is_some());
    source.confirm_original_io_closed().unwrap();
    let (acquired, closed, retained, unknown) = source.io.original_observation();
    assert_eq!((acquired, closed), (1, 1));
    assert!(!retained && !unknown);
    assert!(
        !lock_path(&path).exists(),
        "inspection cannot acquire a writer lease"
    );
    assert_eq!(source.read_initial_material(&path).unwrap(), b"not-json");
}

#[test]
fn failed_contending_lease_closes_its_actual_original_and_keeps_snapshot_loading_independent() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quota.json");
    let first = Arc::new(ConfigurationSource::default());
    let owner = first.open_quota_storage(&path).unwrap();
    owner.replace(b"snapshot").unwrap();
    let second = Arc::new(ConfigurationSource::default());
    assert_eq!(
        second.open_quota_storage(&path).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let rejected = second.quota.0.lock().unwrap().as_ref().unwrap().clone();
    let lease = rejected.lease.lock().unwrap();
    assert!(lease
        .original
        .as_ref()
        .unwrap()
        .matches_closed(lease.closed.as_ref().unwrap()));
    drop(lease);
    assert_eq!(second.read_initial_material(&path).unwrap(), b"snapshot");
    second.confirm_quota_storage_closed().unwrap();
    owner.close_lease().unwrap();
    let next = second.open_quota_storage(&path).unwrap();
    next.close_lease().unwrap();
}

#[test]
fn interrupted_quota_operation_retains_the_same_native_lease_and_fences_new_preparation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quota.json");
    let source = Arc::new(ConfigurationSource::default());
    let storage = source.open_quota_storage(&path).unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: io::Result<()> = storage.with_source(|_| panic!("original quota worker operation"));
    }));
    assert!(failed.is_err());
    assert!(source.ensure_resource_io_available().is_err());
    assert!(super::super::super::startup::confirm_closed().is_err());
    assert!(source.open_quota_storage(&path).is_err());
    // Recover only in this fixture by returning the actual retained original;
    // no fake receipt, replacement descriptor or Drop counts as completion.
    {
        let mut lease = storage
            .lease
            .lock()
            .err()
            .expect("actual poisoned lease")
            .into_inner();
        QuotaStorage::close_original(&mut lease).unwrap();
        assert!(lease
            .original
            .as_ref()
            .unwrap()
            .matches_closed(lease.closed.as_ref().unwrap()));
    }
    storage.lease.clear_poison();
    source.confirm_quota_storage_closed().unwrap();
    super::super::super::startup::release_confirmed(&source).unwrap();
    let next = source.open_quota_storage(&path).unwrap();
    next.close_lease().unwrap();
}

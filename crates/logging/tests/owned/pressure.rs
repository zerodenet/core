use super::*;
#[tokio::test]
async fn a_full_fault_archive_keeps_the_same_original_until_real_budget_growth() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pressure.log");
    let mut config = config(&path);
    let (mut scope, mut owner) = prepare(&config);
    initialized(&mut scope, &mut owner).await;
    let mut writer = owner.writer(0).unwrap();
    writer.write_all(b"old\n").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    // Retain the exact existing ring and worker promises: the next real job
    // can run, but creating the first archive row requires additional storage.
    let cap = owner.usage().storage_bytes;
    config.max_storage_bytes = Some(cap);
    let handle = owner.budget_handle();
    let mut candidate = handle.prepare_limits(&config).unwrap();
    candidate.commit_deferred();
    candidate.publish_committed();
    drop(candidate);
    fs::create_dir(path.with_extension("1")).unwrap();
    writer.write_all(b"bad\n").unwrap();
    let rejected = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| match owner.poll_workers(&mut scope, cx) {
            Err(source) => Poll::Ready(source),
            Ok(()) => Poll::Pending,
        }),
    )
    .await
    .unwrap();
    assert_eq!(rejected.kind(), io::ErrorKind::WouldBlock);
    let original = owner.peek_io_failure().unwrap();
    assert!(!original.accepted());
    assert_eq!(original.failed_bytes(), Some(b"bad\n".as_slice()));
    assert!(owner.failures().is_empty());
    assert_eq!(owner.usage().pending_bytes, 4);
    assert!(!scope.is_drained());
    config.max_storage_bytes = Some(cap.checked_add(4096).unwrap());
    let mut candidate = handle.prepare_limits(&config).unwrap();
    candidate.commit_deferred();
    candidate.publish_committed();
    drop(candidate);
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    assert!(original.accepted());
    assert_eq!(owner.failures().len(), 1);
    assert!(matches!(&owner.failures()[0], LoggingFailure::Io(actual) if actual.same(&original)));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
    retire(&mut scope, &mut owner).await;
}
#[tokio::test]
async fn retained_original_failure_has_no_scope_controller_or_book_back_edge() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("is-a-directory");
    fs::create_dir(&path).unwrap();
    let config = config(&path);
    let (mut scope, mut owner) = prepare(&config);
    let weak = Arc::downgrade(&owner.shared.book);
    let entry =
        zero_traits::packet_metadata::PacketMetadataArc::downgrade(&owner.shared.entries[0]);
    until(&mut scope, &mut owner, |owner| {
        owner.initialization_failed() && owner.drained()
    })
    .await;
    let retained = match &owner.failures()[0] {
        LoggingFailure::Io(source) => source.clone(),
        _ => panic!("real open error"),
    };
    assert!(retained.failed_bytes().is_none());
    retire(&mut scope, &mut owner).await;
    drop(owner);
    drop(scope);
    assert!(
        entry.upgrade().is_none(),
        "paid diagnostics cannot own their IO controller"
    );
    assert!(
        weak.upgrade().is_none(),
        "pure error has only a weak book identity"
    );
    assert!(
        std::error::Error::source(&retained).is_some(),
        "actual IO source remains available"
    );
    drop(retained);
    drop(entry);
}

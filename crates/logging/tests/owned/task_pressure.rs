//! The actual main pump must recover an offered failed exit after full archive.
use super::*;
#[tokio::test]
async fn actual_task_panic_full_archive_redelivers_same_original_after_budget_growth() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("task-pressure.log");
    let mut config = config(&path);
    config.files[0].max_bytes = u64::MAX;
    let (mut scope, mut owner) = prepare(&config);
    initialized(&mut scope, &mut owner).await;
    let mut writer = owner.writer(0).unwrap();
    writer.write_all(b"old\n").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    let maximum = owner.usage().storage_bytes;
    config.max_storage_bytes = Some(maximum);
    let handle = owner.budget_handle();
    let mut candidate = handle.prepare_limits(&config).unwrap();
    candidate.commit_deferred();
    candidate.publish_committed();
    drop(candidate);
    owner.shared.entries[0]
        .panic_next_work
        .store(true, std::sync::atomic::Ordering::Release);
    writer.write_all(b"bad\n").unwrap();
    // Unlike the IO-only pressure test, no pending IO source exists before
    // poll_exit. Main consumes the ACTUAL failed native join, then its archive
    // reserve refuses. Drop of that offer permits same-row redelivery by Scope.
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| match owner.poll_workers(&mut scope, cx) {
            Err(source) => Poll::Ready(source),
            Ok(()) => Poll::Pending,
        }),
    )
    .await
    .unwrap();
    assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
    assert!(owner.peek_io_failure().is_none());
    assert!(owner.failures().is_empty());
    let witness = owner.witness(0).unwrap();
    let joined = witness
        .try_joined()
        .expect("real Scope poll_exit consumed this task");
    let original = joined.result().unwrap_err();
    assert!(witness.matches_joined(&joined));
    assert_eq!(witness.task_id(), Some(joined.id()));
    assert_eq!(joined.class(), TaskClass::Critical);
    assert!(!joined.failure_accepted());
    assert!(!witness
        .try_joined()
        .expect("actual original task has joined")
        .registration_released());
    assert!(
        !owner.native_closed(),
        "original FD closure is independent of failed task join"
    );
    assert_eq!(owner.usage().pending_bytes, 4);
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
    // This normal caller changes only the same actual book. No Scope receipt
    // is obtained/ACKed manually and no new task/IO owner is substituted.
    config.max_storage_bytes = Some(maximum.checked_add(4096).unwrap());
    let mut candidate = handle.prepare_limits(&config).unwrap();
    candidate.commit_deferred();
    candidate.publish_committed();
    drop(candidate);
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    assert!(joined.failure_accepted());
    assert!(witness
        .try_joined()
        .expect("actual original task has joined")
        .registration_released());
    assert_eq!(owner.failures().len(), 1);
    assert!(
        matches!(&owner.failures()[0], LoggingFailure::Worker(source)
        if source.joined().id() == joined.id()
            && Arc::ptr_eq(&source.joined().result().unwrap_err(), &original)
            && source.failed_bytes() == Some(b"bad\n".as_slice()))
    );
    assert_eq!(owner.usage().pending_bytes, 4);
    assert_eq!(
        fs::read(&path).unwrap(),
        b"old\n",
        "accepted failed chunk was never replayed"
    );
    drop(joined);
    drop(witness);
    drop(original);
    writer.write_all(b"good").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    assert_eq!(fs::read(&path).unwrap(), b"old\ngood");
    assert_eq!(
        owner.failures().len(),
        1,
        "same original was not minted as another fault"
    );
    retire(&mut scope, &mut owner).await;
}

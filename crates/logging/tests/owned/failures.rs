use super::*;
#[tokio::test]
async fn real_rotation_fault_retains_failed_bytes_and_shrink_recovers_without_new_dial() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fault.log");
    let mut config = config(&path);
    config.max_pending_bytes = Some(8);
    let (mut scope, mut owner) = prepare(&config);
    initialized(&mut scope, &mut owner).await;
    let mut writer = owner.writer(0).unwrap();
    writer.write_all(b"old\n").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    fs::create_dir(path.with_extension("1")).unwrap();
    writer.write_all(b"bad\n").unwrap();
    until(&mut scope, &mut owner, |owner| {
        !owner.failures().is_empty() && owner.drained()
    })
    .await;
    let source = match &owner.failures()[0] {
        LoggingFailure::Io(source) => source.clone(),
        _ => panic!("actual rename fault"),
    };
    assert_eq!(source.failed_bytes(), Some(b"bad\n".as_slice()));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
    assert_eq!(
        owner.usage().pending_bytes,
        4,
        "failed data remains charged"
    );
    assert_eq!(owner.usage().pending_chunks, 1);
    let book = owner.budget_handle();
    let mut small = config.clone();
    small.max_pending_bytes = Some(3);
    assert!(book.prepare_limits(&small).is_err());
    assert_eq!(owner.failures().len(), 1);
    fs::remove_dir(path.with_extension("1")).unwrap();
    writer.write_all(b"good").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    assert_eq!(fs::read(&path).unwrap(), b"good");
    assert_eq!(fs::read(path.with_extension("1")).unwrap(), b"old\n");
    assert_eq!(owner.failures().len(), 1, "no replay of the failed chunk");
    retire(&mut scope, &mut owner).await;
    drop(writer);
    drop(owner); // retired local source copies cannot pin the handed-off data
    assert!(
        book.prepare_limits(&small).is_err(),
        "actual external payload still owns its 4 bytes"
    );
    drop(source);
    let mut candidate = book.prepare_limits(&small).unwrap();
    candidate.commit_deferred();
    candidate.publish_committed();
    assert_eq!(book.usage().pending_bytes, 0);
}
#[tokio::test]
async fn real_worker_panic_preserves_original_task_chunk_and_host_ack() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("panic.log");
    let config = config(&path);
    let (mut scope, mut owner) = prepare(&config);
    initialized(&mut scope, &mut owner).await;
    owner.shared.entries[0]
        .panic_next_work
        .store(true, std::sync::atomic::Ordering::Release);
    let mut writer = owner.writer(0).unwrap();
    writer.write_all(b"bad\n").unwrap();
    owner.drive(&scope.registrar()).unwrap();
    let receipt = tokio::time::timeout(Duration::from_secs(5), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let original = receipt.result().unwrap_err();
    let witness = owner.witness(0).unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    let source = owner.take_worker_failure().unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &source.joined().result().unwrap_err(),
        &original
    ));
    assert_eq!(source.failed_bytes(), Some(b"bad\n".as_slice()));
    assert!(owner.accept_worker_failure(&source)); // actual local typed receiver
    assert!(!witness.registration_released());
    owner.drive(&scope.registrar()).unwrap();
    assert!(owner.witness(0).unwrap().same_registration(&witness));
    assert!(!owner.close_after_workers());
    scope.acknowledge(&receipt).unwrap();
    drop(receipt);
    assert!(witness.registration_released());
    drop(witness);
    owner.drive(&scope.registrar()).unwrap(); // closes old original before reuse
    writer.write_all(b"good").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    assert_eq!(fs::read(&path).unwrap(), b"good");
    assert_eq!(
        owner.usage().pending_bytes,
        4,
        "original failed chunk stays paid"
    );
    retire(&mut scope, &mut owner).await;
    drop(writer);
    drop(owner);
    assert!(Arc::ptr_eq(
        &source.joined().result().unwrap_err(),
        &original
    ));
    drop(source);
}

#[tokio::test]
async fn actual_os_error_and_original_task_panic_are_both_received_before_ack() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("both.log");
    let config = config(&path);
    let (mut scope, mut owner) = prepare(&config);
    initialized(&mut scope, &mut owner).await;
    let mut writer = owner.writer(0).unwrap();
    writer.write_all(b"old\n").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    fs::create_dir(path.with_extension("1")).unwrap();
    owner.shared.entries[0]
        .panic_after_write_failure
        .store(true, std::sync::atomic::Ordering::Release);
    writer.write_all(b"bad\n").unwrap();
    owner.drive(&scope.registrar()).unwrap();
    let receipt = tokio::time::timeout(Duration::from_secs(5), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let original = receipt.result().unwrap_err();
    let witness = owner.witness(0).unwrap();
    owner.collect_failures().unwrap();
    assert_eq!(owner.failures().len(), 2);
    assert!(owner.failures().iter().any(|source| matches!(source, LoggingFailure::Io(source) if source.failed_bytes() == Some(b"bad\n".as_slice()))));
    assert!(owner.failures().iter().any(|source| matches!(source, LoggingFailure::Worker(source) if Arc::ptr_eq(&source.joined().result().unwrap_err(), &original))));
    assert!(receipt.local_failure_accepted());
    assert!(!witness.registration_released());
    owner.drive(&scope.registrar()).unwrap();
    assert_eq!(owner.failures().len(), 2);
    assert!(owner.witness(0).unwrap().same_registration(&witness));
    scope.acknowledge(&receipt).unwrap();
    drop(receipt);
    assert!(witness.registration_released());
    drop(witness);
    fs::remove_dir(path.with_extension("1")).unwrap();
    writer.write_all(b"good").unwrap();
    until(&mut scope, &mut owner, ScopedTracing::drained).await;
    assert_eq!(fs::read(&path).unwrap(), b"good");
    assert_eq!(owner.failures().len(), 2);
    retire(&mut scope, &mut owner).await;
}

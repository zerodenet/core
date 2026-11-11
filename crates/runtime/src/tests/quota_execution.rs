use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{timeout, Duration};

fn owned() -> (
    tempfile::TempDir,
    Arc<ConfigurationSource>,
    OwnedQuotaStorage,
) {
    let directory = tempfile::tempdir().unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let memory = source.quota_memory(&Default::default()).unwrap();
    let storage = source
        .open_owned_quota_storage(directory.path().join("quota.json"), &memory)
        .unwrap();
    (directory, source, storage)
}

fn assert_original_close(storage: &OwnedQuotaStorage) {
    let lease = storage.lease.lock().unwrap();
    assert!(lease
        .original
        .as_ref()
        .unwrap()
        .matches_closed(lease.closed.as_ref().unwrap()));
}

#[cfg(unix)]
#[tokio::test]
async fn quota_handoff_joins_genuine_original_before_scoped_writer_and_cancelled_shutdown_keeps_custody(
) {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let (_directory, source, storage) = owned();
    let roots = crate::HostOriginalBudgets::default();
    let (mut release, original_read) = UnixStream::pair().unwrap();
    let original_read = Mutex::new(original_read);
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let (entered, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let original = storage.downgrade();
    storage
        .prepare_writer(move || {
            let call = callback_calls.fetch_add(1, Ordering::SeqCst);
            // Establish Pending on the actual original I/O before advertising
            // the suspension point. The retained peer cannot release it until
            // the test has checked custody and cancelled the shutdown wait.
            if call == 0 || call == 3 {
                let mut original = original_read.lock().unwrap();
                original.set_nonblocking(true)?;
                assert_eq!(
                    original.read(&mut [0]).unwrap_err().kind(),
                    io::ErrorKind::WouldBlock
                );
                original.set_nonblocking(false)?;
                entered.send(call).unwrap();
                original.read_exact(&mut [0])?;
            } else {
                entered.send(call).unwrap();
            }
            original.upgrade().unwrap().replace(b"persisted")
        })
        .unwrap();
    assert!(
        storage.execution.state.lock().unwrap().tasks.is_none(),
        "Tokio availability does not select hosted mode"
    );
    storage.mark_writer_dirty().unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap(),
        Some(0)
    );
    source.adopt_quota_memory(&roots).unwrap();
    source
        .drive_quota_writer(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(roots.component_tasks.usage().charged, 0);
            assert!(storage
                .execution
                .state
                .lock()
                .unwrap()
                .standard
                .as_ref()
                .unwrap()
                .original
                .is_some());
            assert!(source.confirm_quota_storage_closed().is_err());
            release.write_all(&[1]).unwrap();
            assert_eq!(
                timeout(Duration::from_secs(5), receiver.recv())
                    .await
                    .unwrap(),
                Some(1)
            );
            // The genuine original performs its own final flush (1), then
            // only the confirmed handoff may invoke the scoped producer (2).
            assert_eq!(
                timeout(Duration::from_secs(5), receiver.recv())
                    .await
                    .unwrap(),
                Some(2)
            );
        })
        .await;
    assert!(storage.execution.state.lock().unwrap().standard.is_none());
    let mut shutdown = Box::pin(storage.shutdown_writer_async());
    tokio::select! {
        result = &mut shutdown => panic!("native final write is still pending: {result:?}"),
        call = receiver.recv() => assert_eq!(call, Some(3)),
    }
    assert!(timeout(Duration::from_millis(20), &mut shutdown)
        .await
        .is_err());
    drop(shutdown);
    assert!(!storage.writer_retirement_complete());
    assert_eq!(roots.component_tasks.usage().charged, 1);
    assert!(source.confirm_quota_storage_closed().is_err());
    assert_eq!(
        source
            .open_owned_quota_storage(
                storage.path(),
                &source.quota_memory(&Default::default()).unwrap()
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    release.write_all(&[1]).unwrap();
    timeout(Duration::from_secs(5), storage.shutdown_writer_async())
        .await
        .unwrap()
        .unwrap();
    assert!(storage.writer_retirement_complete());
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert_original_close(&storage);
    source.confirm_original_io_closed().unwrap();
}

#[tokio::test]
async fn quota_scoped_panic_retains_actual_failure_and_ack_until_original_cleanup() {
    let (_directory, source, storage) = owned();
    let roots = crate::HostOriginalBudgets::default();
    source.adopt_quota_memory(&roots).unwrap();
    let native_id = Arc::new(Mutex::new(None));
    let entered = native_id.clone();
    storage
        .prepare_writer(move || {
            *entered.lock().unwrap() = Some(tokio::task::id());
            panic!("actual quota producer panic")
        })
        .unwrap();
    let error = timeout(Duration::from_secs(5), storage.shutdown_writer_async())
        .await
        .unwrap()
        .unwrap_err();
    let original = storage.retained_worker_failure().unwrap();
    let failure = original
        .get_ref()
        .unwrap()
        .downcast_ref::<QuotaWriterFailure>()
        .unwrap();
    assert_eq!(
        failure.class(),
        zero_platform_tokio::contract::lifecycle::TaskClass::Critical
    );
    assert!(std::error::Error::source(failure).is_some());
    let mut cause: &(dyn std::error::Error + 'static) = &error;
    let native = loop {
        if let Some(native) = cause.downcast_ref::<tokio::task::JoinError>() {
            break native;
        }
        cause = cause
            .source()
            .expect("the actual Tokio panic JoinError is retained");
    };
    assert!(native.is_panic());
    assert_eq!(native.id(), native_id.lock().unwrap().unwrap());
    assert!(native.to_string().contains("actual quota producer panic"));
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert!(storage.writer_retirement_complete());
    assert_original_close(&storage);
    assert!(storage.shutdown_writer_async().await.is_err());
    assert!(Arc::ptr_eq(
        &original,
        &storage.retained_worker_failure().unwrap()
    ));
}

#[tokio::test]
async fn quota_scoped_known_close_write_failure_can_retry_without_another_owner() {
    let (_directory, source, storage) = owned();
    let roots = crate::HostOriginalBudgets::default();
    source.adopt_quota_memory(&roots).unwrap();
    std::fs::create_dir(storage.path()).unwrap();
    let original = storage.downgrade();
    storage
        .prepare_writer(move || original.upgrade().unwrap().replace(b"recovered"))
        .unwrap();
    storage.mark_writer_dirty().unwrap();
    source
        .drive_quota_writer(async {
            timeout(Duration::from_secs(5), async {
                while storage.writer_error().is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        })
        .await;
    let fault = storage.writer_error().unwrap();
    assert!(storage.execution.state.lock().unwrap().terminal.is_none());
    std::fs::remove_dir(storage.path()).unwrap();
    storage.mark_writer_dirty().unwrap();
    timeout(Duration::from_secs(5), storage.shutdown_writer_async())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(std::fs::read(storage.path()).unwrap(), b"recovered");
    assert!(storage.writer_error().is_none());
    assert!(std::error::Error::source(fault.as_ref()).is_some());
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert_original_close(&storage);
}

#[test]
fn quota_publication_rejects_a_foreign_counter_even_with_the_same_target_limit() {
    let first = ConfigurationSource::default();
    let second = ConfigurationSource::default();
    let runtime = zero_config::RuntimeOptionsConfig::default();
    let memory = first.quota_memory(&runtime).unwrap();
    second.quota_memory(&runtime).unwrap();
    let target = zero_config::RuntimeOptionsConfig {
        principal_quota_total_max_storage_bytes: Some(8 * 1024 * 1024),
        ..runtime
    };
    let candidate = second.prepare_quota_memory(&target).unwrap().unwrap();
    candidate.seal().unwrap();
    assert_eq!(
        first
            .validate_quota_memory_publication(&target, Some(&candidate))
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(memory.snapshot().limit.bytes(), 16 * 1024 * 1024);
    assert!(first
        .validate_quota_memory_publication(&target, None)
        .is_err());
}

#[tokio::test]
async fn quota_hosted_dirty_burst_is_batched_and_final_flush_bypasses_debounce() {
    let (_directory, source, storage) = owned();
    let roots = crate::HostOriginalBudgets::default();
    source.adopt_quota_memory(&roots).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let writes = calls.clone();
    let original = storage.downgrade();
    storage
        .prepare_writer(move || {
            writes.fetch_add(1, Ordering::SeqCst);
            original.upgrade().unwrap().replace(b"batched")
        })
        .unwrap();
    for _ in 0..100 {
        storage.mark_writer_dirty().unwrap();
    }
    // Inspect the original scheduled deadline rather than depending on how
    // much wall time a loaded runner spends before its first poll.
    let at = storage.execution.state.lock().unwrap().ready_at.unwrap();
    storage.execution.state.lock().unwrap().ready_at =
        Some(std::time::Instant::now() + Duration::from_secs(60));
    source
        .drive_quota_writer(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(roots.component_tasks.usage().charged, 0);
        })
        .await;
    assert!(at <= storage.execution.state.lock().unwrap().ready_at.unwrap());
    timeout(Duration::from_secs(5), storage.shutdown_writer_async())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_original_close(&storage);
}

#[tokio::test]
async fn quota_actual_critical_failure_reaches_the_existing_root_waiter() {
    let (_directory, source, storage) = owned();
    let roots = crate::HostOriginalBudgets::default();
    source.adopt_quota_memory(&roots).unwrap();
    storage
        .prepare_writer(|| panic!("original supervised quota failure"))
        .unwrap();
    // Register the actual root wait before creating the fault, exercising the
    // notification handoff rather than only polling an already failed status.
    let mut failure = Box::pin(source.wait_quota_writer_failure());
    assert!(timeout(Duration::from_millis(10), &mut failure)
        .await
        .is_err());
    storage.mark_writer_dirty().unwrap();
    let original = timeout(Duration::from_secs(5), source.drive_quota_writer(failure))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&original, &storage.writer_error().unwrap()));
    assert!(storage.shutdown_writer_async().await.is_err());
    assert!(storage.writer_retirement_complete());
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert_original_close(&storage);
}

#[test]
fn quota_genuine_dirty_notifications_do_not_shorten_the_existing_write_batch() {
    let (_directory, _source, storage) = owned();
    let started = std::time::Instant::now();
    let (sent, received) = std::sync::mpsc::channel();
    let original = storage.downgrade();
    storage
        .prepare_writer(move || {
            sent.send(started.elapsed()).unwrap();
            original.upgrade().unwrap().replace(b"genuine batch")
        })
        .unwrap();
    storage.mark_writer_dirty().unwrap();
    assert!(received.recv_timeout(Duration::from_millis(10)).is_err());
    for _ in 0..100 {
        storage.mark_writer_dirty().unwrap();
    }
    let elapsed = received.recv_timeout(Duration::from_secs(5)).unwrap();
    // Allow timer granularity while rejecting a dirty notification that turns
    // the existing 50ms batch into an immediate write.
    assert!(
        elapsed >= Duration::from_millis(45),
        "early write at {elapsed:?}"
    );
    storage.shutdown_writer(true).unwrap();
    assert!(storage.writer_retirement_complete());
    assert_original_close(&storage);
}

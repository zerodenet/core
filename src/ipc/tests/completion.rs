//! Multiple actual completion failures keep their original typed sources.
use super::*;
#[derive(Debug)]
struct WriteFault(Arc<()>);
impl std::fmt::Display for WriteFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original control output fault")
    }
}
impl std::error::Error for WriteFault {}
#[tokio::test]
async fn output_failure_does_not_discard_the_original_blocking_worker_panic() {
    let identity = Arc::new(());
    let output = io::Error::new(
        io::ErrorKind::PermissionDenied,
        WriteFault(identity.clone()),
    );
    let worker = tokio::task::spawn_blocking(|| panic!("original blocking control worker"));
    let joined = worker.await.map_err(io::Error::other);
    let failure = finish_work([Err(output), joined]).unwrap_err();
    assert_eq!(
        failure.kind(),
        io::ErrorKind::Other,
        "multiple failures cannot be classified as only a peer disconnect"
    );
    let failures = failure
        .get_ref()
        .unwrap()
        .downcast_ref::<ControlFailures>()
        .unwrap();
    let output = failures.0[0]
        .get_ref()
        .unwrap()
        .downcast_ref::<WriteFault>()
        .unwrap();
    assert!(Arc::ptr_eq(&output.0, &identity));
    let worker = failures.0[1]
        .get_ref()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap();
    assert!(worker.is_panic());
}
#[test]
fn a_single_original_failure_keeps_its_existing_classification_and_identity() {
    let identity = Arc::new(());
    let original = io::Error::new(io::ErrorKind::BrokenPipe, WriteFault(identity.clone()));
    let error = finish_work([Ok(()), Err(original)]).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    let actual = error
        .get_ref()
        .unwrap()
        .downcast_ref::<WriteFault>()
        .unwrap();
    assert!(Arc::ptr_eq(&actual.0, &identity));
}

#[tokio::test]
async fn original_work_panic_and_actual_child_join_fault_are_both_retained_after_cleanup() {
    let identity = Arc::new(());
    let payload = identity.clone();
    async fn original_work(payload: Arc<()>) -> io::Result<()> {
        std::panic::panic_any(payload)
    }
    fn worker_panic() {
        std::panic::panic_any("actual bridge worker panic")
    }
    let work = catch_work(original_work(payload)).await;
    let original = tokio::task::spawn_blocking(worker_panic);
    let joined = original.await.map(|_| ()).map_err(io::Error::other);
    let returned = finish_caught(work, [joined]).unwrap_err();
    let actual = returned
        .get_ref()
        .unwrap()
        .downcast_ref::<PanicCleanup>()
        .unwrap();
    let retained = actual.payload.lock().unwrap();
    assert!(Arc::ptr_eq(
        retained.downcast_ref::<Arc<()>>().unwrap(),
        &identity
    ));
    assert_eq!(actual.cleanup.len(), 1);
    assert!(actual.cleanup[0]
        .get_ref()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap()
        .is_panic());
}

#[cfg(windows)]
#[tokio::test]
async fn windows_server_unwind_keeps_actual_peer_join_error_after_both_raw_tasks_return() {
    let original = Arc::new(());
    let weak = Arc::downgrade(&original);
    let server = tokio::spawn(async move {
        async fn original_work(original: Arc<()>) -> io::Result<()> {
            std::panic::panic_any(original)
        }
        let work = catch_work(original_work(original)).await;
        async fn original_peer() {
            panic!("same actual IPC peer task")
        }
        let peer = tokio::spawn(original_peer());
        let id = peer.id();
        let actual = peer.await.unwrap_err();
        assert!(actual.is_panic() && actual.id() == id);
        // This is the same Windows production terminal branch after original
        // joins. Neither pure error observation issues a native/Scope receipt.
        let result = crate::ipc::windows::finish_server_work(work, vec![io::Error::other(actual)]);
        (id, result)
    });
    let (id, result) = server.await.unwrap();
    let returned = result.unwrap_err();
    let pair = returned
        .get_ref()
        .unwrap()
        .downcast_ref::<PanicCleanup>()
        .unwrap();
    {
        let payload = pair.payload.lock().unwrap();
        let original = payload.downcast_ref::<Arc<()>>().unwrap();
        assert!(Arc::ptr_eq(original, &weak.upgrade().unwrap()));
    }
    assert_eq!(pair.cleanup.len(), 1);
    let actual = pair.cleanup[0]
        .get_ref()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap();
    assert!(actual.is_panic() && actual.id() == id);
    assert!(
        weak.upgrade().is_some(),
        "only the returned pure error now holds the original panic payload"
    );
    drop(returned);
    assert!(weak.upgrade().is_none());
}

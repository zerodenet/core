use super::*;

#[tokio::test]
async fn a_discarded_last_join_waiter_cannot_steal_the_remaining_file_readers_wakeup() {
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
    };
    #[derive(Default)]
    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let directory = Directory::new();
    let path = directory.0.join("wake");
    std::fs::write(&path, b"wake").unwrap();
    let grants = grant(path.clone(), 4);
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut first = Box::pin(grants.read_file(path.clone(), 4));
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    gate.entered().await;
    let mut last = Box::pin(grants.read_file(path, 4));
    assert!(last
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    drop(last);
    let before = wakes.0.load(Ordering::SeqCst);
    gate.release();
    tokio::time::timeout_at(deadline(), async {
        while wakes.0.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout_at(deadline(), first)
            .await
            .unwrap()
            .unwrap(),
        b"wake"
    );
    assert!(grants.shutdown_io(deadline()).await.unwrap().is_complete());
}

#[tokio::test]
async fn cancelling_one_file_waiter_does_not_restart_or_cancel_the_other_actual_reader() {
    let directory = Directory::new();
    let path = directory.0.join("shared");
    std::fs::write(&path, b"shared").unwrap();
    let grants = grant(path.clone(), 6);
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let reading = grants.clone();
    let selected = path.clone();
    let first = tokio::spawn(async move { reading.read_file(selected, 6).await });
    gate.entered().await;
    let mut second = Box::pin(grants.read_file(path, 6));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(second.as_mut(), &mut context).is_pending());
    assert_eq!(grants.state.lock().unwrap().files.records.len(), 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    gate.release();
    assert_eq!(
        tokio::time::timeout_at(deadline(), second)
            .await
            .unwrap()
            .unwrap(),
        b"shared"
    );
    assert_eq!(
        grants
            .shutdown_io(deadline())
            .await
            .unwrap()
            .released_resources
            .len(),
        1
    );
}

#[tokio::test]
async fn actual_blocking_job_panic_retains_its_join_source_and_never_confirms_successful_cleanup() {
    let directory = Directory::new();
    let path = directory.0.join("not-read-after-panic");
    let grants = grant(path.clone(), 4);
    grants.state.lock().unwrap().files.before_read =
        Some(Arc::new(|| panic!("actual blocking file job failed")));
    let _access = grants.activate(deadline()).unwrap();
    let failure = grants.read_file(path, 4).await.unwrap_err();
    assert_eq!(failure.reason, Reason::Internal);
    fn has_join(mut error: &(dyn Error + 'static)) -> bool {
        loop {
            if error.is::<JoinError>() {
                return true;
            }
            let Some(next) = error.source() else {
                return false;
            };
            error = next;
        }
    }
    assert!(has_join(&failure));
    for _ in 0..2 {
        let failure = grants.shutdown_io(deadline()).await.unwrap_err();
        assert_eq!(failure.reason, Reason::Internal);
        assert!(has_join(&failure));
    }
    let state = grants.state.lock().unwrap();
    assert!(
        state.files.records.is_empty(),
        "the failed actual task was joined"
    );
    assert_eq!(
        state.files.receipts.released_resources.len(),
        1,
        "the actual join receipt is still owned despite the reported failure"
    );
}

use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::time::Duration;

#[tokio::test]
async fn accepted_configuration_task_survives_its_waiter_and_keeps_the_actual_serial_guard() {
    let owner = Arc::new(ApplyOwner::default());
    let serial = Arc::new(Mutex::new(()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let waiting = {
        let (owner, serial, entered, release, completed) = (
            owner.clone(),
            serial.clone(),
            entered.clone(),
            release.clone(),
            completed.clone(),
        );
        tokio::spawn(async move {
            owner
                .run(serial, async move {
                    entered.notify_one();
                    release.notified().await;
                    completed.store(true, Ordering::Release);
                    Ok(())
                })
                .await
        })
    };
    entered.notified().await;
    assert!(!waiting.is_finished());
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert!(
        serial.try_lock().is_err(),
        "the accepted task retains the actual apply guard"
    );
    let count = Arc::new(AtomicUsize::new(0));
    let next = {
        let (owner, serial, count) = (owner.clone(), serial.clone(), count.clone());
        tokio::spawn(async move {
            owner
                .run(serial, async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        })
    };
    release.notify_one();
    next.await.unwrap().unwrap();
    assert!(completed.load(Ordering::Acquire));
    assert_eq!(count.load(Ordering::Acquire), 1);
    owner
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupted_shutdown_retains_the_accepted_task_until_its_real_exit() {
    let owner = Arc::new(ApplyOwner::default());
    let serial = Arc::new(Mutex::new(()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let waiting = {
        let (owner, entered, release) = (owner.clone(), entered.clone(), release.clone());
        tokio::spawn(async move {
            owner
                .run(serial, async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
                .await
        })
    };
    entered.notified().await;
    owner.begin_shutdown();
    let writes = AtomicUsize::new(0);
    assert!(owner
        .commit(|| {
            writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .is_err());
    assert_eq!(writes.load(Ordering::Acquire), 0);
    assert!(owner
        .shutdown(Instant::now() + Duration::from_millis(1))
        .await
        .unwrap_err()
        .contains("unconfirmed"));
    assert!(!waiting.is_finished());
    release.notify_one();
    waiting.await.unwrap().unwrap();
    owner
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_scope_task_failure_is_retained_and_cannot_be_erased_by_a_later_apply() {
    async fn failed() -> Result<(), String> {
        panic!("injected actual owned-task panic")
    }
    let owner = Arc::new(ApplyOwner::default());
    let serial = Arc::new(Mutex::new(()));
    assert!(owner.run(serial.clone(), failed()).await.is_err());
    assert!(owner.wait_idle().await.unwrap_err().contains("failed"));
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    assert!(owner
        .run(serial, async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .is_err());
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .is_err());
    let state = owner.owner.lock().await;
    assert!(std::error::Error::source(&state.failure.as_ref().unwrap().error).is_some());
}

#[tokio::test]
async fn cancelling_a_suspended_observer_cannot_consume_the_real_failure_notification() {
    async fn failed(entered: Arc<Notify>, release: Arc<Notify>) -> Result<(), String> {
        entered.notify_one();
        release.notified().await;
        panic!("actual fault retained across cancelled observer")
    }
    let owner = Arc::new(ApplyOwner::default());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let waiting = {
        let (owner, entered, release) = (owner.clone(), entered.clone(), release.clone());
        tokio::spawn(async move {
            owner
                .run(Arc::new(Mutex::new(())), failed(entered, release))
                .await
        })
    };
    entered.notified().await;
    let held_owner = owner.owner.lock().await;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !owner.completed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(waiting.await.unwrap().is_err());
    let mut observing = Box::pin(owner.observe_failure());
    std::future::poll_fn(|cx| {
        assert!(
            observing.as_mut().poll(cx).is_pending(),
            "observer suspends at the actual held owner lock"
        );
        std::task::Poll::Ready(())
    })
    .await;
    drop(observing);
    assert!(
        owner.completed.load(Ordering::Acquire),
        "unjoined completion must remain visible"
    );
    drop(held_owner);
    let failure = tokio::time::timeout(Duration::from_secs(3), owner.observe_failure())
        .await
        .unwrap();
    assert!(failure.contains("failed"));
    let state = owner.owner.lock().await;
    assert!(std::error::Error::source(&state.failure.as_ref().unwrap().error).is_some());
}

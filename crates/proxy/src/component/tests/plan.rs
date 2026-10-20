use super::*;
#[path = "plan/fixtures.rs"]
mod fixtures;
use std::error::Error;
use tokio::time::{Duration, Instant};
use zero_platform_tokio::contract::lifecycle::TaskClass;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn cancelled_plan_shutdown_retains_actual_join_failure_after_report_consumption() {
    use tokio::io::AsyncWriteExt;
    let (blocking, blocked) = fixtures::slot(1, false);
    let (panicking, panic) = fixtures::slot(2, true);
    let plan = Arc::new(ComponentPlan {
        builtin: blocking.clone(),
        outbounds: BTreeMap::new(),
        order: vec![blocking, panicking],
        started: AtomicBool::new(false),
        failures: Mutex::new(BTreeMap::new()),
        shared_identities: Default::default(),
    });
    plan.start(deadline()).await.unwrap();
    // The critical task must actually receive from its owned stream before it
    // panics; its native join ID is captured inside that real task.
    let mut peer = panic.peer.lock().unwrap().take().unwrap();
    peer.write_u8(1).await.unwrap();
    let first = tokio::spawn({
        let plan = plan.clone();
        async move { plan.shutdown(deadline()).await }
    });
    blocked.pending.notified().await;
    // The second owner is suspended in an actual read. The first owner's real
    // exit has already been joined and its one-shot report consumed.
    let original = plan
        .failures
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    assert_eq!(original.0.id, panic.task.lock().unwrap().unwrap());
    assert_eq!(original.0.class, TaskClass::Critical);
    let native_id = panic.native_id.lock().unwrap().unwrap();
    let join = original
        .0
        .error
        .source()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap();
    assert!(join.is_panic());
    assert_eq!(join.id(), native_id);
    let source_address = std::ptr::from_ref(join);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(!blocked.closed.load(Ordering::Acquire));
    drop(blocked.peer.lock().unwrap().take());
    for _ in 0..2 {
        let error = plan.shutdown(deadline()).await.unwrap_err();
        let retained = error
            .source()
            .unwrap()
            .downcast_ref::<Arc<plan::PlanTaskFailure>>()
            .unwrap();
        assert!(Arc::ptr_eq(retained, &original));
        let join = retained
            .0
            .error
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap();
        assert_eq!(std::ptr::from_ref(join), source_address);
        assert_eq!(join.id(), native_id);
    }
    assert!(blocked.closed.load(Ordering::Acquire));
}

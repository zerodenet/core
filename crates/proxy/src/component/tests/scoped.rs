use crate::component::catalog::supervision::testing::TestContext;
#[path = "scoped/fixtures.rs"]
mod fixtures;
use std::sync::atomic::Ordering;
use tokio::{
    io::AsyncReadExt,
    time::{Duration, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::Unfinished,
    scope::{SpawnError, TaskClass},
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn protocol_cleanup_and_declared_driver_exit_are_driven_together() {
    let (slot, state) = fixtures::slot(false, false, false);
    let host = TestContext::new([slot.as_ref()]);
    assert_eq!(state.started.load(Ordering::SeqCst), 0);
    host.drive(slot.start(deadline())).await.unwrap();
    host.drive(slot.start(deadline())).await.unwrap();
    assert_eq!(state.started.load(Ordering::SeqCst), 1);
    let task = state.task.lock().unwrap().unwrap();
    let mut peer = state.peer.lock().unwrap().take().unwrap();
    let report = host.drive(slot.shutdown(deadline())).await.unwrap();
    assert!(report.is_complete(), "{:?}", report.unfinished);
    assert_eq!(report.exited_tasks, vec![task]);
    assert_eq!(report.closed_io, vec!["owned driver stream"]);
    assert!(state.closed.load(Ordering::SeqCst));
    assert_eq!(
        peer.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn bounded_driver_keeps_io_and_join_ownership_after_cleanup_timeout() {
    let (slot, state) = fixtures::slot(true, false, false);
    let host = TestContext::new([slot.as_ref()]);
    host.drive(slot.start(deadline())).await.unwrap();
    let task = state.task.lock().unwrap().unwrap();
    let registrar = state.registrar.lock().unwrap().clone().unwrap();
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let mut peer = state.peer.lock().unwrap().take().unwrap();
    let report = host
        .drive(slot.shutdown(Instant::now() + Duration::from_millis(10)))
        .await
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.unfinished.contains(&Unfinished::Task(task)));
    assert!(!state.closed.load(Ordering::SeqCst));
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
    state.release.notify_one();
    let report = host.drive(slot.shutdown(deadline())).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, vec![task]);
    assert_eq!(
        peer.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn protocol_cleanup_failure_retains_the_confirmed_driver_failure_for_retry() {
    use std::error::Error;
    let (slot, state) = fixtures::slot(false, true, true);
    let host = TestContext::new([slot.as_ref()]);
    host.drive(slot.start(deadline())).await.unwrap();
    let task = state.task.lock().unwrap().unwrap();
    let first = host.drive(slot.shutdown(deadline())).await.unwrap_err();
    assert_eq!(
        first.source().unwrap().to_string(),
        "protocol cleanup confirmation failed once"
    );
    let report = host.drive(slot.shutdown(deadline())).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, vec![task]);
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, task);
    assert_eq!(
        crate::component::tests::source::<std::io::Error>(&report.failed_tasks[0].error)
            .to_string(),
        "scoped driver reported a terminal failure"
    );
    assert!(state.closed.load(Ordering::SeqCst));
}

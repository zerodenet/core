use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
#[tokio::test]
async fn cancelled_join_waiter_keeps_the_same_original_and_typed_panic() {
    let (release, pending) = tokio::sync::oneshot::channel::<()>();
    let owner = ControlTask::new(tokio::spawn(async move {
        pending.await.unwrap();
        panic!("retained original control failure");
        #[allow(unreachable_code)]
        io::Result::Ok(())
    }));
    let mut first = Box::pin(owner.join());
    poll_fn(|cx| {
        assert!(first.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(first);
    assert!(owner.original_retained_for_test().await);
    release.send(()).unwrap();
    let first = owner.join().await.unwrap_err();
    let repeated = owner.join().await.unwrap_err();
    assert!(first
        .to_string()
        .contains("retained original control failure"));
    assert_eq!(first.to_string(), repeated.to_string());
    assert!(!owner.original_retained_for_test().await);
    let source = owner.original_fault_for_test().await.unwrap();
    assert!(source
        .get_ref()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap()
        .is_panic());
}

#[tokio::test]
async fn original_task_reservation_survives_a_concurrent_cancelled_join_waiter() {
    let owner = ControlTask::empty();
    let registration = owner.reserve().unwrap();
    let mut waiting = Box::pin(owner.join());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let original = Arc::new(io::Error::new(
        io::ErrorKind::InvalidData,
        "actual reserved server fault",
    ));
    let fault = original.clone();
    let (release, started) = tokio::sync::oneshot::channel::<()>();
    registration.install(tokio::spawn(async move {
        started.await.unwrap();
        Err(io::Error::new(fault.kind(), fault))
    }));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert!(owner.original_retained_for_test().await);
    release.send(()).unwrap();
    for _ in 0..2 {
        let returned = owner.join().await.unwrap_err();
        let retained = returned
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        let same = retained
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap();
        assert!(Arc::ptr_eq(same, &original));
    }
    assert!(!owner.original_retained_for_test().await);
}
#[tokio::test]
async fn a_stopped_cold_or_cancelled_initializer_cannot_install_a_later_task() {
    let observed = ControlTask::empty();
    observed.join().await.unwrap();
    drop(observed.reserve().unwrap());
    let cold = ControlTask::empty();
    cold.close_admission();
    cold.join().await.unwrap();
    assert!(cold.reserve().is_err());
    let opening = ControlTask::empty();
    let registration = opening.reserve().unwrap();
    let mut waiting = Box::pin(opening.join());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(registration);
    waiting.await.unwrap();
    assert!(opening.reserve().is_err());
    assert!(!opening.original_retained_for_test().await);
}

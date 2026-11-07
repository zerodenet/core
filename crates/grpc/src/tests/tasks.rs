//! The retained original blocking join, rather than waiter lifetime, owns charge release.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
#[tokio::test]
async fn cancelled_subscription_join_waiter_preserves_registration_and_original_charge() {
    let budget = zero_runtime::configuration::ControlBudget::new(
        zero_runtime::configuration::ControlBudgetLimits {
            connections: 1,
            subscriptions: 1,
            queued_event_bytes: 1024,
            pipe_workspace_bytes: 2 * 1024 * 1024,
            messages: Default::default(),
        },
    );
    let charge = budget
        .reserve(zero_runtime::configuration::ControlCharge::Subscription)
        .unwrap();
    let owner = ControlTask::empty();
    let registration = owner.reserve().unwrap();
    let mut waiting = Box::pin(owner.join());
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let (begin, started) = std::sync::mpsc::sync_channel::<()>(1);
    let (release, finishing) = std::sync::mpsc::sync_channel::<()>(1);
    let original = Arc::new(io::Error::new(
        io::ErrorKind::InvalidData,
        "actual subscription original fault",
    ));
    let fault = original.clone();
    let task = tokio::task::spawn_blocking(move || {
        started.recv().unwrap();
        finishing.recv().unwrap();
        Err(io::Error::new(fault.kind(), fault))
    });
    registration.install_charged(task, charge);
    begin.send(()).unwrap();
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert!(owner.original_retained_for_test().await);
    assert_eq!(budget.usage().subscriptions, 1);
    release.send(()).unwrap();
    for _ in 0..2 {
        let error = owner.join().await.unwrap_err();
        let retained = error
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
    assert!(owner.original_fault_for_test().await.is_some());
    budget.close_admission();
    budget.confirm_released().unwrap();
}
#[tokio::test]
async fn cold_shutdown_and_cancelled_preparation_fence_the_same_original_slot() {
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

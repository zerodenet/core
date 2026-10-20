use super::*;
use std::{future::Future, task::Poll, time::Duration};
use tokio::sync::oneshot;

#[tokio::test]
async fn interrupted_cleanup_retains_original_pending_child_and_fences_the_already_held_guard() {
    let owner = RetainedTasks::new("actual pending raw-IP child", 1);
    let mut driver = owner.lock().await.unwrap();
    let (entered, suspended) = oneshot::channel();
    let mut entered = Some(entered);
    driver
        .spawn(async move {
            std::future::poll_fn(|_| {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                }
                Poll::<()>::Pending
            })
            .await;
        })
        .unwrap();
    suspended.await.unwrap();
    let mut closing = Box::pin(owner.close(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(driver.len(), 1);
    assert_eq!(driver.spawn(async {}).unwrap_err().reason, Reason::Revoked);
    drop(driver); // Original handle is still in the persistent slot, not this waiter.
    assert_eq!(owner.inner.tasks.lock().await.len(), 1);
    let report = owner
        .close(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(owner.inner.tasks.lock().await.is_empty());
    assert!(owner.lock().await.is_err());
    assert_eq!(report.released_resources, ["actual pending raw-IP child"]);
    assert!(report.closed_io.is_empty());
}

#[tokio::test]
async fn first_actual_panic_fences_old_guard_and_preserves_original_id_and_source_on_reentry() {
    let owner = RetainedTasks::<()>::new("actual panicking raw-IP child", 2);
    let mut driver = owner.lock().await.unwrap();
    let id = driver
        .spawn(async { panic!("original raw-IP child panic") })
        .unwrap();
    let error = driver.join_next().await.unwrap().unwrap_err();
    let original = owner.remember(error);
    assert_eq!(original.id(), id);
    for _ in 0..100 {
        assert_eq!(driver.spawn(async {}).unwrap_err().reason, Reason::Revoked);
    }
    drop(driver);
    for _ in 0..2 {
        let error = owner
            .close(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap_err();
        let failures = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<RetainedFailures>()
            .unwrap();
        assert_eq!(failures.first.task, id);
        assert_eq!(failures.additional, 0);
        assert!(std::ptr::eq(
            failures.first.source.downcast_ref::<JoinError>().unwrap(),
            original.as_ref()
        ));
    }
}

#[tokio::test]
async fn cleanup_retains_typed_output_error_before_waiting_for_another_actual_child_join() {
    let owner = RetainedTasks::new("actual fallible raw-IP children", 2);
    let mut driver = owner.lock().await.unwrap();
    let (ready, output_ready) = oneshot::channel();
    let id = driver
        .spawn(async move {
            ready.send(()).unwrap();
            Err::<(), _>(zero_engine::EngineError::Io(
                std::io::Error::from_raw_os_error(10013),
            ))
        })
        .unwrap();
    output_ready.await.unwrap();
    let (entered, suspended) = oneshot::channel();
    let mut entered = Some(entered);
    driver
        .spawn(async move {
            std::future::poll_fn(|_| {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                }
                Poll::<Result<(), zero_engine::EngineError>>::Pending
            })
            .await
        })
        .unwrap();
    suspended.await.unwrap();
    drop(driver);
    let mut closing = Box::pin(owner.close(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    let original = owner
        .inner
        .admission
        .lock()
        .unwrap()
        .first
        .as_ref()
        .unwrap()
        .source
        .clone();
    for _ in 0..2 {
        let error = owner
            .close(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap_err();
        let failures = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<RetainedFailures>()
            .unwrap();
        assert_eq!(failures.first.task, id);
        assert!(Arc::ptr_eq(&failures.first.source, &original));
        let zero_engine::EngineError::Io(source) = failures
            .first
            .source
            .downcast_ref::<zero_engine::EngineError>()
            .unwrap()
        else {
            panic!("original typed EngineError")
        };
        assert_eq!(source.raw_os_error(), Some(10013));
    }
    assert!(owner.inner.tasks.lock().await.is_empty());
}

#[tokio::test]
async fn remaining_fatal_outputs_are_counted_in_bounded_place_without_success_history() {
    let owner = RetainedTasks::new("eight actual fallible children", 8);
    let mut driver = owner.lock().await.unwrap();
    let (done, mut completed) = tokio::sync::mpsc::channel(8);
    for n in 0..8 {
        let done = done.clone();
        driver
            .spawn(async move {
                done.send(()).await.unwrap();
                Err::<(), _>(zero_engine::EngineError::AdmissionDenied {
                    reason: format!("actual child {n}"),
                })
            })
            .unwrap();
    }
    for _ in 0..8 {
        completed.recv().await.unwrap();
    }
    owner.finish(&mut driver).await;
    assert!(driver.is_empty());
    assert_eq!(
        driver.spawn(async { Ok(()) }).unwrap_err().reason,
        Reason::Revoked
    );
    drop(driver);
    let admission = owner.inner.admission.lock().unwrap();
    assert!(admission.first.is_some());
    assert_eq!(admission.additional, 7);
}

#[tokio::test]
async fn synchronous_resource_revoke_on_another_thread_fences_the_original_live_guard() {
    let owner = RetainedTasks::<()>::new("same fenced child registry", 2);
    let mut guard = owner.lock().await.unwrap();
    guard.spawn(std::future::pending()).unwrap();
    let revoking = owner.clone();
    std::thread::spawn(move || ResourceLifecycle::revoke(&revoking, CloseReason::Shutdown))
        .join()
        .unwrap();
    assert_eq!(guard.spawn(async {}).unwrap_err().reason, Reason::Revoked);
    assert_eq!(guard.len(), 1);
    drop(guard);
    let report = owner
        .close(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(owner.inner.tasks.lock().await.is_empty());
}

use super::*;
use std::{future::poll_fn, task::Poll};

#[tokio::test]
async fn caller_timeout_keeps_exact_apply_join_without_borrowing_the_next_operation() {
    let owner = Arc::new(ApplyOwner::default());
    let serial = Arc::new(Mutex::new(()));
    let observed = ApplyObservation::new();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let mut waiting = Box::pin(observed.scope(owner.run(serial.clone(), async move {
        entered.send(()).unwrap();
        released.await.unwrap();
        Ok(())
    })));
    // Drive this actual accepted task to its explicit pending cleanup gate.
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    entering.await.unwrap();
    drop(waiting); // Same lost waiter as the caller-only timeout.
    let mut joined = Box::pin(observed.wait_joined());
    poll_fn(|cx| {
        assert!(joined.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    let receipt = joined.await.unwrap();
    assert!(receipt.result().is_ok());
    assert!(receipt.registration_released());

    let (next_entered, next_entering) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let next = {
        let owner = owner.clone();
        tokio::spawn(async move {
            owner
                .run(serial, async move {
                    next_entered.send(()).unwrap();
                    finished.await.unwrap();
                    Ok(())
                })
                .await
        })
    };
    next_entering.await.unwrap();
    // The exact old receipt remains complete while the new operation is live.
    let again = observed.wait_joined().await.unwrap();
    assert_eq!(again.id(), receipt.id());
    assert!(!next.is_finished());
    finish.send(()).unwrap();
    next.await.unwrap().unwrap();
    owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test]
async fn same_actual_panic_arc_survives_ack_and_known_no_apply_paths_are_complete() {
    let none = ApplyObservation::new();
    assert_eq!(
        none.scope(async { Err::<(), _>("parse failure") }).await,
        Err("parse failure")
    );
    assert!(none.wait_joined().await.is_none());
    let owner = Arc::new(ApplyOwner::default());
    let observed = ApplyObservation::new();
    async fn panic_work() -> Result<(), String> {
        panic!("actual observed Apply panic")
    }
    assert!(observed
        .scope(owner.run(Arc::new(Mutex::new(())), panic_work()))
        .await
        .is_err());
    let receipt = observed.wait_joined().await.unwrap();
    assert!(receipt.registration_released());
    let first = receipt.result().unwrap_err();
    let second = observed.wait_joined().await.unwrap().result().unwrap_err();
    assert!(Arc::ptr_eq(&first, &second));
    let owner = owner.owner.lock().await;
    let failure = owner.failure.as_ref().unwrap();
    assert_eq!(failure.id, receipt.id());
    assert_eq!(failure.class, receipt.class());
    let mut source: &(dyn std::error::Error + 'static) = &failure.error;
    let mut same = false;
    loop {
        if source
            .downcast_ref::<zero_platform_tokio::contract::lifecycle::ComponentError>()
            .is_some_and(|source| std::ptr::eq(source, first.as_ref()))
        {
            same = true;
            break;
        }
        let Some(next) = source.source() else { break };
        source = next;
    }
    assert!(
        same,
        "canonical ApplyOwner keeps the exact original task-error Arc"
    );
}

#[tokio::test]
async fn captured_original_blocking_producer_cannot_be_mistaken_for_no_accepted_work() {
    let observed = ApplyObservation::new();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let worker = observed
        .scope(async {
            let original = ApplyObservation::capture().unwrap();
            tokio::task::spawn_blocking(move || {
                original.run_sync(|| {
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                })
            })
        })
        .await;
    entering.await.unwrap();
    let mut joined = Box::pin(observed.wait_joined());
    poll_fn(|cx| {
        assert!(joined.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    worker.await.unwrap();
    assert!(joined.await.is_none());
}

#[tokio::test]
async fn sequential_actual_operations_are_joined_before_the_observation_changes_registration() {
    let first = Arc::new(ApplyOwner::default());
    let second = Arc::new(ApplyOwner::default());
    let observed = ApplyObservation::new();
    let serial = Arc::new(Mutex::new(()));
    let original = observed
        .scope(async {
            first.run(serial.clone(), async { Ok(()) }).await.unwrap();
            let original = {
                let state = observed.0.state.lock().unwrap();
                let Some(Original::Registered(_, witness)) = &state.original else {
                    panic!("actual registration absent")
                };
                witness.clone()
            };
            let proof = original.clone();
            second
                .run(serial, async move {
                    let joined = proof.try_joined().unwrap();
                    assert!(joined.result().is_ok());
                    assert!(joined.registration_released());
                    Ok(())
                })
                .await
                .unwrap();
            original
        })
        .await;
    let receipt = observed.wait_joined().await.unwrap();
    assert_ne!(receipt.id(), original.task_id().unwrap());
    assert!(receipt.result().is_ok());
    assert!(original.try_joined().unwrap().registration_released());
}

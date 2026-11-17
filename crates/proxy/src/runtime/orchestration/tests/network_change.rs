use super::*;
use std::{future::Future, task::Poll};

#[tokio::test]
async fn accepted_apply_keeps_network_replacement_pending_through_ready_and_cancelled_wait() {
    let serial = Arc::new(Mutex::new(()));
    let owner = Arc::new(zero_runtime::ApplyOwner::default());
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let mut changes = egress.subscribe_generation();
    let before = egress.generation();
    let (ready, accepted) = tokio::sync::oneshot::channel();
    let (commit, finish) = tokio::sync::oneshot::channel();
    let applying = tokio::spawn({
        let owner = owner.clone();
        let serial = serial.clone();
        async move {
            owner
                .run(serial, async move {
                    ready.send(()).unwrap();
                    finish.await.unwrap();
                    Ok(())
                })
                .await
                .unwrap();
        }
    });
    accepted.await.unwrap();
    let mut network = Box::pin(wait(&serial, &mut changes, before, None));
    std::future::poll_fn(|cx| {
        assert!(network.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    egress.invalidate_network();
    std::future::poll_fn(|cx| {
        assert!(network.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(network);
    assert!(
        !changes.has_changed().unwrap(),
        "the real watch wake was consumed"
    );
    assert_ne!(
        *changes.borrow(),
        before,
        "its original generation still owes replacement"
    );
    let mut resumed = Box::pin(wait(&serial, &mut changes, before, None));
    std::future::poll_fn(|cx| {
        assert!(resumed.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    commit.send(()).unwrap();
    applying.await.unwrap();
    let original_gate = resumed.await;
    assert!(serial.try_lock().is_err());
    drop(original_gate);
    owner.wait_idle().await.unwrap();
    owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

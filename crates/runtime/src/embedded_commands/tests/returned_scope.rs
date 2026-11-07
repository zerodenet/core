//! Real drained Scope custody must wake its parallel physical close consumer.
use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returning_the_same_drained_scope_wakes_the_original_close_consumer() {
    timeout(Duration::from_secs(5), async {
        let book = book();
        let owner = book.command_owner();
        owner.ensure_now(1).unwrap();
        let original = lock(&owner.state)
            .tasks
            .as_ref()
            .unwrap()
            .retirement_observation();
        owner.begin_shutdown();
        owner.finish_monitor().await.unwrap();
        assert!(original.is_drained());
        let monitor = lock(&owner.state).monitor.clone().unwrap();
        assert!(monitor.try_joined().unwrap().registration_released());
        let (entered, received) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        *lock(&owner.observation_gate) = Some((entered, gate));
        let observing = owner.clone();
        let watcher = tokio::spawn(async move { observing.observe_failure().await });
        received.await.unwrap();
        assert!(lock(&owner.state).scope.is_none());
        let mut closing = Box::pin(owner.finish_monitor());
        std::future::poll_fn(|cx| {
            use std::future::Future;
            assert!(closing.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        closing.await.unwrap();
        assert!(original.is_drained() && owner.retirement_complete());
        assert_eq!(book.usage().pending_commands, 0);
        assert!(
            !watcher.is_finished(),
            "normal real Scope retirement is not a fabricated fault"
        );
        watcher.abort();
        assert!(watcher.await.unwrap_err().is_cancelled());
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

use super::ListenerStop;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};

#[tokio::test]
async fn closed_listener_rejects_late_claim_and_cancelled_wait_retains_consumer() {
    let stop = ListenerStop::default();
    let guard = stop.claim().unwrap();
    stop.begin_shutdown();
    assert!(stop.claim().is_err());
    guard.cancelled().await;
    let mut wait = Box::pin(stop.wait_quiescent());
    poll_fn(|cx| {
        assert!(wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(wait);
    let mut retry = Box::pin(stop.wait_quiescent());
    poll_fn(|cx| {
        assert!(retry.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(guard);
    retry.await;
    assert!(stop.claim().is_err());
}

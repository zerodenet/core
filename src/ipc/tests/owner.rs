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
    assert!(owner.0.lock().await.task.is_some());
    release.send(()).unwrap();
    let first = owner.join().await.unwrap_err();
    let repeated = owner.join().await.unwrap_err();
    assert!(first
        .to_string()
        .contains("retained original control failure"));
    assert_eq!(first.to_string(), repeated.to_string());
    let state = owner.0.lock().await;
    assert!(state.task.is_none());
    let source = state.result.as_ref().unwrap().as_ref().unwrap_err();
    assert!(source
        .get_ref()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap()
        .is_panic());
}

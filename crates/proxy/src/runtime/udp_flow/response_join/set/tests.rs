use super::*;
use std::{
    future::Future,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use tokio::sync::{broadcast, oneshot};
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
async fn pending(future: impl Future) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
#[tokio::test]
async fn actual_receiver_set_survives_cancelled_cleanup_and_lease_drop_until_the_same_join_consumer(
) {
    let (owner, mut lease) = ResponseSetOwner::new(JoinSet::new());
    let record = ResponseTask::new();
    let (_sender, mut receiver) = broadcast::channel::<Packet>(1);
    let (entered, receiving) = oneshot::channel();
    owner.evidence.spawn(&mut lease, &record, async move {
        entered.send(()).unwrap();
        receiver
            .recv()
            .await
            .map_err(|error| std::io::Error::other(error).into())
    });
    receiving.await.unwrap();
    pending(lease.join_next_with_id()).await;
    pending(owner.shutdown(deadline())).await;
    assert!(!owner.is_closed());
    assert!(record.joined().is_none());
    drop(lease); // return original Set; this is not a join receipt
    assert!(!owner.is_closed());
    let closed = owner.shutdown(deadline()).await.unwrap();
    assert!(closed.matches(&owner));
    assert_eq!(closed.joined_tasks(), 1);
    assert!(record.wait_joined().await.failure().is_some());
    let (foreign, _lease) = ResponseSetOwner::new(JoinSet::new());
    assert!(!closed.matches(&foreign));
    assert!(owner.shutdown(deadline()).await.unwrap().matches(&owner));
}
#[tokio::test]
async fn delivered_body_failure_is_the_original_arc_and_does_not_erase_actual_join_closure() {
    let (owner, mut lease) = ResponseSetOwner::new(JoinSet::new());
    let record = ResponseTask::new();
    owner.evidence.spawn(&mut lease, &record, async {
        Err(EngineError::Io(std::io::Error::from_raw_os_error(10013)))
    });
    let delivered = lease
        .join_next_with_id()
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let Some(ResponseFailure::Body(saved)) = owner.first_failure() else {
        panic!("same original body source");
    };
    assert!(Arc::ptr_eq(&saved, &delivered));
    let Some(ResponseFailure::Body(local)) = record.joined().unwrap().failure() else {
        panic!("same local receiver source");
    };
    assert!(Arc::ptr_eq(&local, &saved));
    drop(lease);
    let closed = owner.shutdown(deadline()).await.unwrap();
    assert!(closed.matches(&owner));
    assert_eq!(closed.joined_tasks(), 1);
    let Some(ResponseFailure::Body(retained)) = owner.first_failure() else {
        panic!("source must survive retry");
    };
    assert!(Arc::ptr_eq(&retained, &saved));
}
struct CallerWaker {
    owner: Arc<ResponseSetOwner>,
    dropped: Arc<AtomicBool>,
}
impl Wake for CallerWaker {
    fn wake(self: Arc<Self>) {}
}
impl Drop for CallerWaker {
    fn drop(&mut self) {
        // Native Set wake/drop cannot invoke the original caller under its
        // polling lock or keep the caller's owner alive through a strong cycle.
        assert!(!self.owner.wake.test_in_native());
        self.dropped.store(true, Ordering::Release);
    }
}
#[tokio::test]
async fn cancelled_native_join_wait_removes_the_strong_caller_waker_before_native_set_shutdown() {
    let (owner, mut lease) = ResponseSetOwner::new(JoinSet::new());
    lease.spawn(std::future::pending());
    let dropped = Arc::new(AtomicBool::new(false));
    let callback = Arc::new(CallerWaker {
        owner: owner.clone(),
        dropped: dropped.clone(),
    });
    let weak = Arc::downgrade(&callback);
    let waker = Waker::from(callback.clone());
    let mut cx = Context::from_waker(&waker);
    let mut joining = Box::pin(lease.join_next_with_id());
    assert!(joining.as_mut().poll(&mut cx).is_pending());
    drop(joining);
    drop(cx);
    drop(waker);
    drop(callback);
    assert!(weak.upgrade().is_none());
    assert!(dropped.load(Ordering::Acquire));
    assert!(!owner.is_closed());
    drop(lease);
    assert!(owner.shutdown(deadline()).await.unwrap().matches(&owner));
}

use super::*;
use std::{future::Future, task::Poll};
use tokio::sync::{broadcast, oneshot};

async fn pending(future: impl Future) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
fn packet() -> Packet {
    (
        Address::Ipv4([192, 0, 2, 1]),
        53,
        b"actual response".to_vec(),
        Some(7),
    )
}
#[tokio::test]
async fn real_response_receive_and_cancel_still_require_the_original_join_consumer() {
    let owner = ResponseTasks::default();
    let record = ResponseTask::new();
    let mut tasks = JoinSet::new();
    let (_sender, mut receiver) = broadcast::channel::<Packet>(1);
    let (entered, at_receive) = oneshot::channel();
    owner.spawn(&mut tasks, &record, async move {
        entered.send(()).unwrap();
        receiver
            .recv()
            .await
            .map_err(|error| std::io::Error::other(error).into())
    });
    at_receive.await.unwrap();
    pending(tasks.join_next_with_id()).await; // actual task is suspended at receive
    pending(record.wait_joined()).await; // cancelled waiter keeps original record
    assert!(record.joined().is_none());
    record.cancel();
    assert!(record.joined().is_none());
    let result = tasks.join_next_with_id().await.unwrap();
    assert!(result.as_ref().unwrap_err().is_cancelled());
    assert!(record.joined().is_none()); // native join without actual delivery also differs
    let actual = owner.observe(result).unwrap_err();
    let receipt = record.wait_joined().await;
    assert_eq!(receipt.id(), actual.id());
    let Some(ResponseFailure::Join(saved)) = receipt.failure() else {
        panic!("original join source");
    };
    assert!(Arc::ptr_eq(&saved, &actual));
}
#[tokio::test]
async fn body_ready_cannot_rearm_a_second_receiver_before_actual_join() {
    let owner = ResponseTasks::default();
    let record = ResponseTask::new();
    let mut tasks = JoinSet::new();
    let (ready, body_ready) = oneshot::channel();
    owner.spawn(&mut tasks, &record, async move {
        ready.send(()).unwrap();
        Ok(packet())
    });
    body_ready.await.unwrap();
    assert!(record.joined().is_none());
    owner.spawn(&mut tasks, &record, async {
        panic!("body-ready must not spawn a second response task")
    });
    assert_eq!(tasks.len(), 1);
    let result = tasks.join_next_with_id().await.unwrap();
    assert!(owner.observe(result).unwrap().is_ok());
    let first = record.joined().unwrap();
    assert!(first.failure().is_none());
    owner.spawn(&mut tasks, &record, async { Ok(packet()) });
    assert!(record.joined().is_none());
    assert_eq!(tasks.len(), 1);
    let result = tasks.join_next_with_id().await.unwrap();
    assert!(owner.observe(result).unwrap().is_ok());
    assert_ne!(first.id(), record.joined().unwrap().id());
    assert!(owner.records.lock().unwrap().is_empty()); // no success history
}
#[tokio::test]
async fn original_body_failure_and_os_object_are_shared_with_the_actual_result_receiver() {
    let owner = ResponseTasks::default();
    let record = ResponseTask::new();
    let mut tasks = JoinSet::new();
    owner.spawn(&mut tasks, &record, async {
        Err(EngineError::Io(std::io::Error::from_raw_os_error(10013)))
    });
    let result = tasks.join_next_with_id().await.unwrap();
    let original = owner.observe(result).unwrap().unwrap_err();
    let Some(ResponseFailure::Body(saved)) = record.joined().unwrap().failure() else {
        panic!("actual body failure");
    };
    assert!(Arc::ptr_eq(&saved, &original));
    let EngineError::Io(io) = saved.as_ref() else {
        panic!("original OS error variant");
    };
    assert_eq!(io.raw_os_error(), Some(10013));
}
#[tokio::test]
async fn original_panic_object_is_preserved_and_foreign_or_dropped_consumers_do_not_sign_joined() {
    let owner = ResponseTasks::default();
    let record = ResponseTask::new();
    let mut tasks = JoinSet::new();
    let (started, native_id) = oneshot::channel();
    owner.spawn(&mut tasks, &record, async move {
        started.send(tokio::task::id()).unwrap();
        panic!("original logical receiver panic")
    });
    let native_id = native_id.await.unwrap();
    let result = tasks.join_next_with_id().await.unwrap();
    let original = owner.observe(result).unwrap_err();
    assert!(original.is_panic());
    assert_eq!(original.id(), native_id);
    let Some(ResponseFailure::Join(saved)) = record.joined().unwrap().failure() else {
        panic!("same panic source");
    };
    assert!(Arc::ptr_eq(&saved, &original));
    let unknown = ResponseTask::new();
    owner.spawn(&mut tasks, &unknown, async { Ok(packet()) });
    let result = tasks.join_next_with_id().await.unwrap();
    let other = ResponseTasks::default();
    assert!(other.observe(result).unwrap().is_ok());
    assert!(unknown.joined().is_none());
    pending(unknown.wait_joined()).await;
    let dropped = ResponseTask::new();
    owner.spawn(&mut tasks, &dropped, std::future::pending());
    drop(tasks);
    pending(dropped.wait_joined()).await;
    assert!(dropped.joined().is_none());
}

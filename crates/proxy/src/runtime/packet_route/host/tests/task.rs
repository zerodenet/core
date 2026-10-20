use super::*;
use std::{future::Future, task::Poll};
use tokio::sync::oneshot;

#[tokio::test]
async fn body_ready_is_not_join_and_cancelled_join_waiter_keeps_the_same_original_handle() {
    let (entered, pending) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let mut entered = Some(entered);
    let task = DriverTask::new(async move {
        let mut released = Box::pin(released);
        std::future::poll_fn(|cx| match released.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                }
                Poll::Pending
            }
        })
        .await
        .unwrap();
        DriverResult {
            failure: None,
            native: None,
        }
    });
    pending.await.unwrap();
    let id = task.state.lock().await.task.as_ref().unwrap().id();
    let mut joining = Box::pin(task.join());
    std::future::poll_fn(|cx| {
        assert!(joining.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(joining);
    assert_eq!(task.state.lock().await.task.as_ref().unwrap().id(), id);
    release.send(()).unwrap();
    let result = task
        .join()
        .await
        .unwrap_or_else(|error| panic!("actual driver joined: {error}"));
    assert!(task.state.lock().await.task.is_none());
    assert!(Arc::ptr_eq(
        &result,
        &task
            .join()
            .await
            .unwrap_or_else(|error| panic!("retry: {error}"))
    ));
    assert_eq!(
        task.join_closed().await.unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

#[tokio::test]
async fn driver_original_os_error_is_retained_as_the_same_arc_after_actual_join() {
    let original = Arc::new(io::Error::from_raw_os_error(10013));
    let actual = original.clone();
    let task = DriverTask::new(async move {
        DriverResult {
            failure: Some(actual),
            native: None,
        }
    });
    for _ in 0..2 {
        let error = match task.join().await {
            Ok(_) => panic!("original failure required"),
            Err(error) => error,
        };
        let shared = error.get_ref().unwrap().downcast_ref::<SharedIo>().unwrap();
        assert!(Arc::ptr_eq(&shared.0, &original));
        assert_eq!(shared.0.raw_os_error(), Some(10013));
    }
}

#[tokio::test]
async fn original_driver_panic_retains_the_actual_join_error_and_never_certifies_native_close() {
    let task = DriverTask::new(async { panic!("original host device panic") });
    let first = task.join_closed().await.unwrap_err();
    let first = first
        .get_ref()
        .unwrap()
        .downcast_ref::<SharedJoin>()
        .unwrap()
        .0
        .clone();
    assert!(first.is_panic());
    let retry = task.join_closed().await.unwrap_err();
    let retry = retry
        .get_ref()
        .unwrap()
        .downcast_ref::<SharedJoin>()
        .unwrap();
    assert!(Arc::ptr_eq(&retry.0, &first));
}

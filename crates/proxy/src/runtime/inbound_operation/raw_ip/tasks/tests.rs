use super::*;
use std::{future::Future, task::Poll, time::Duration};
use tokio::sync::oneshot;

#[tokio::test]
async fn interrupted_cleanup_retains_the_original_pending_child_handle_until_real_join() {
    let owner = RetainedTasks::new("actual pending raw-IP child");
    let mut driver = owner.lock().await.unwrap();
    let (entered, suspended) = oneshot::channel();
    let mut entered = Some(entered);
    driver.spawn(async move {
        std::future::poll_fn(|_| {
            if let Some(entered) = entered.take() {
                entered.send(()).unwrap();
            }
            Poll::<()>::Pending
        })
        .await;
    });
    suspended.await.unwrap();
    let mut closing = Box::pin(owner.close(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(driver.len(), 1); // No handle moved into the interrupted waiter.
    drop(driver); // Cancelling the borrowing driver is not a child join.
    assert_eq!(owner.lock().await.unwrap().len(), 1);
    let report = owner
        .close(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(owner.inner.tasks.lock().await.tasks.is_empty());
    assert!(owner.lock().await.is_err());
    assert_eq!(report.released_resources, ["actual pending raw-IP child"]);
    assert!(report.closed_io.is_empty()); // Join evidence makes no native FD claim.
}

#[tokio::test]
async fn child_panic_keeps_the_exact_original_source_across_cleanup_reentry() {
    let owner = RetainedTasks::<()>::new("actual panicking raw-IP child");
    owner
        .lock()
        .await
        .unwrap()
        .spawn(async { panic!("original raw-IP child panic") });
    let error = owner
        .lock()
        .await
        .unwrap()
        .join_next()
        .await
        .unwrap()
        .unwrap_err();
    owner.remember(error);
    let original = owner.inner.faults.lock().unwrap()[0].clone();
    for _ in 0..2 {
        let error = owner
            .close(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap_err();
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<Arc<JoinError>>()
            .unwrap();
        assert!(Arc::ptr_eq(source, &original));
    }
}

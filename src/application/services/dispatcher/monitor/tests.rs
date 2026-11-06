use super::*;
use std::{future::Future, task::Poll};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn cancelled_close_keeps_same_sampler_and_real_pending_join_until_retry() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let owner = StatusMonitor::cold();
        let (mut input, _peer) = tokio::io::duplex(1);
        let (pending, received) = tokio::sync::oneshot::channel();
        owner
            .start(async move {
                let read = input.read_u8();
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                pending.send(()).unwrap();
                let _ = read.await;
            })
            .unwrap();
        received.await.unwrap();
        let witness = owner.worker.get().unwrap().clone();
        assert!(!witness.is_joined());
        assert!(!owner.retirement_complete());
        let mut closing = Box::pin(owner.shutdown());
        std::future::poll_fn(|cx| {
            assert!(closing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(closing);
        assert!(owner.worker.get().unwrap().same_registration(&witness));
        assert!(!owner.retirement_complete());
        owner.shutdown().await.unwrap();
        let joined = witness.try_joined().unwrap();
        assert!(joined.result().is_ok());
        assert!(joined.registration_released());
        assert!(owner.retirement_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn same_scoped_panic_reaches_supervisor_and_retained_error_without_permanent_join_debt() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let owner = StatusMonitor::cold();
        owner
            .start(async { panic!("actual scoped status sampler failure") })
            .unwrap();
        assert!(owner.wait_terminated().await.is_err());
        assert!(
            !owner.retirement_complete(),
            "observing one exit has not sealed its scope"
        );
        let first = owner.shutdown().await.unwrap_err();
        assert!(owner.retirement_complete());
        let next = owner.shutdown().await.unwrap_err();
        assert!(Arc::ptr_eq(&first, &next));
        let joined = owner.worker.get().unwrap().try_joined().unwrap();
        assert!(Arc::ptr_eq(&first, &joined.result().unwrap_err()));
        assert!(joined.registration_released());
    })
    .await
    .unwrap();
}

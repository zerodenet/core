use super::*;
#[tokio::test]
async fn fully_received_missing_packet_is_not_failed_behind_backpressured_http_writers() {
    use std::{future::Future, task::Poll};
    let sessions = super::super::super::sessions::Sessions::default();
    let session = sessions.get_with_limit("received-gap", 2).unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(8));
    // Four higher sequences fill the bounded ingress/reassembly queues. The
    // missing sequence is received too, but its HTTP producer waits behind them.
    for sequence in [1u64, 2, 3, 4, 0] {
        let mut admission = Box::pin(admit(
            session.clone(),
            sequence,
            Bytes::from(sequence.to_be_bytes().to_vec()),
            permits.clone().try_acquire_owned().unwrap(),
            session.reserve_upload(8).unwrap(),
        ));
        std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = admission.as_mut().poll(cx) {
                result.unwrap();
            }
            Poll::Ready(())
        })
        .await;
    }
    for sequence in 0..5u64 {
        let packet = tokio::time::timeout(Duration::from_secs(1), session.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(packet.data.as_ref(), &sequence.to_be_bytes());
    }
}
#[tokio::test]
async fn completed_packet_survives_cancelled_http_waiter_under_backpressure() {
    let sessions = super::super::super::sessions::Sessions::default();
    let session = sessions.get_with_limit("id", 1).unwrap();
    session
        .push(0, Bytes::from_static(b"first"), false)
        .await
        .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let mut admission = Box::pin(admit(
        session.clone(),
        1,
        Bytes::from_static(b"second"),
        permits.clone().acquire_owned().await.unwrap(),
        session.reserve_upload(6).unwrap(),
    ));
    std::future::poll_fn(|cx| {
        use std::{future::Future, task::Poll};
        assert!(admission.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // Dropping the HTTP waiter must neither cancel admission nor release its
    // request permit while the complete packet waits for queue capacity.
    drop(admission);
    tokio::task::yield_now().await;
    assert_eq!(permits.available_permits(), 0);
    assert_eq!(session.next().await.unwrap().unwrap().data, "first");
    let second = tokio::time::timeout(Duration::from_secs(1), session.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.data, "second");
    let permit = tokio::time::timeout(Duration::from_secs(1), permits.acquire())
        .await
        .unwrap()
        .unwrap();
    drop(permit);
}

#[tokio::test]
async fn received_burst_keeps_fifo_position_when_http_waiters_are_dropped() {
    use std::{future::Future, task::Poll};
    let sessions = super::super::super::sessions::Sessions::default();
    let session = sessions.get_with_limit("burst", 8).unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(64));
    // Poll each HTTP handler in receive order without yielding to its detached
    // task. Tokio may schedule those tasks in a different order afterwards.
    for sequence in 0..64 {
        let mut admission = Box::pin(admit(
            session.clone(),
            sequence,
            Bytes::from(sequence.to_be_bytes().to_vec()),
            permits.clone().acquire_owned().await.unwrap(),
            session.reserve_upload(8).unwrap(),
        ));
        std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = admission.as_mut().poll(cx) {
                result.unwrap();
            }
            Poll::Ready(())
        })
        .await;
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        for sequence in 0..64u64 {
            assert_eq!(
                session.next().await.unwrap().unwrap().data.as_ref(),
                &sequence.to_be_bytes()
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn received_packet_reserves_fifo_position_even_when_http_task_budget_is_exhausted() {
    use std::{future::Future, task::Poll};
    let sessions = super::super::super::sessions::Sessions::default();
    let session = sessions.get_with_limit("exhausted", 8).unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let mut admission = Box::pin(admit(
        session.clone(),
        0,
        Bytes::from_static(b"first"),
        permits.clone().try_acquire_owned().unwrap(),
        session.reserve_upload(5).unwrap(),
    ));
    std::future::poll_fn(|cx| {
        while let Poll::Ready(budget) = tokio::task::coop::poll_proceed(cx) {
            budget.made_progress();
        }
        assert!(!tokio::task::coop::has_budget_remaining());
        // No real queue contention exists. Deferring this packet would allow a
        // later HTTP connection to overtake a request that has already arrived.
        match admission.as_mut().poll(cx) {
            Poll::Ready(result) => result.unwrap(),
            Poll::Pending => panic!("complete packet was deferred before FIFO admission"),
        }
        Poll::Ready(())
    })
    .await;
    assert_eq!(session.next().await.unwrap().unwrap().data, "first");
}

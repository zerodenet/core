use super::*;

#[tokio::test(start_paused = true)]
async fn pending_session_expires_without_another_request() {
    let sessions = Sessions::default();
    let session = sessions.get("pending").unwrap();
    session
        .push(0, Bytes::from_static(b"pending"), false)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    assert!(sessions.0.lock().unwrap().is_empty());
    assert!(session.life.error().is_err());
    drop(session);
    assert_eq!(sessions.1.available_permits(), 64 * 1024 * 1024);
}

#[tokio::test]
async fn shared_budget_covers_dequeued_data_until_consumed() {
    let budget = Arc::new(Semaphore::new(4));
    let first = Session::new(budget.clone(), MAX_POSTS);
    let second = Session::new(budget.clone(), MAX_POSTS);
    first
        .push(0, Bytes::from_static(b"1234"), false)
        .await
        .unwrap();
    let packet = first.next().await.unwrap().unwrap();
    assert!(second
        .push(0, Bytes::from_static(b"x"), false)
        .await
        .is_err());
    drop(packet);
    second
        .push(0, Bytes::from_static(b"x"), false)
        .await
        .unwrap();
    drop(second);
    assert_eq!(budget.available_permits(), 4);
}

#[tokio::test]
async fn reassembly_allows_sequence_gaps_and_rejects_duplicates() {
    let session = Session::new(Arc::new(Semaphore::new(MAX_BYTES)), MAX_POSTS);
    session
        .push(1, Bytes::from_static(b"second"), false)
        .await
        .unwrap();
    assert!(session
        .push(1, Bytes::from_static(b"duplicate"), false)
        .await
        .is_err());
    session
        .push(MAX_POSTS as u64, Bytes::new(), false)
        .await
        .unwrap();
    session
        .push(0, Bytes::from_static(b"first"), false)
        .await
        .unwrap();
    assert_eq!(session.next().await.unwrap().unwrap().data, "first");
    assert_eq!(session.next().await.unwrap().unwrap().data, "second");
    assert!(session.push(0, Bytes::new(), false).await.is_err());
    assert!(session.claim_stream().is_err());
    session.life.close();
    assert!(session.next().await.unwrap().is_none());
}

#[tokio::test]
async fn in_order_burst_waits_for_reader_and_cancel_releases_waiting_producer() {
    let session = Arc::new(Session::new(Arc::new(Semaphore::new(MAX_BYTES)), 2));
    session
        .push(0, Bytes::from_static(b"a"), false)
        .await
        .unwrap();
    session
        .push(1, Bytes::from_static(b"b"), false)
        .await
        .unwrap();
    let producer = {
        let session = session.clone();
        tokio::spawn(async move { session.push(2, Bytes::from_static(b"c"), false).await })
    };
    tokio::task::yield_now().await;
    assert!(!producer.is_finished());
    assert_eq!(session.next().await.unwrap().unwrap().data, "a");
    producer.await.unwrap().unwrap();
    let producer = {
        let session = session.clone();
        tokio::spawn(async move { session.push(3, Bytes::from_static(b"d"), false).await })
    };
    tokio::task::yield_now().await;
    assert!(!producer.is_finished());
    session.life.close();
    assert!(producer.await.unwrap().is_err());
}

#[tokio::test(start_paused = true)]
async fn unbounded_reordering_times_out_without_unbounded_queue_growth() {
    let session = Arc::new(Session::new(Arc::new(Semaphore::new(MAX_BYTES)), 2));
    let reader = {
        let session = session.clone();
        tokio::spawn(async move { session.next().await })
    };
    for sequence in 1..=4 {
        session
            .push(sequence, Bytes::from_static(b"x"), false)
            .await
            .unwrap();
    }
    assert!(reader.await.unwrap().is_err());
}

#[tokio::test(start_paused = true)]
async fn missing_upload_can_arrive_after_reassembly_backpressure_without_buffer_growth() {
    let session = Arc::new(Session::new(Arc::new(Semaphore::new(MAX_BYTES)), 2));
    let reader = {
        let session = session.clone();
        tokio::spawn(async move { session.next().await })
    };
    for sequence in 1..=4 {
        session
            .push(sequence, Bytes::from_static(b"later"), false)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!reader.is_finished());
    assert!(session.state.lock().unwrap().packets.len() <= 4);
    session
        .push(0, Bytes::from_static(b"first"), false)
        .await
        .unwrap();
    assert_eq!(reader.await.unwrap().unwrap().unwrap().data, "first");
    for _ in 1..=4 {
        assert_eq!(session.next().await.unwrap().unwrap().data, "later");
    }
}

#[tokio::test]
async fn backpressure_preserves_fifo_admission_for_blocked_http_producers() {
    let session = Arc::new(Session::new(Arc::new(Semaphore::new(MAX_BYTES)), 2));
    session.push(0, Bytes::new(), false).await.unwrap();
    session.push(1, Bytes::new(), false).await.unwrap();
    let mut producers = tokio::task::JoinSet::new();
    for sequence in 2..64 {
        let session = session.clone();
        producers.spawn(async move { session.push(sequence, Bytes::new(), false).await });
        tokio::task::yield_now().await;
    }
    for _ in 0..64 {
        session.next().await.unwrap().unwrap();
    }
    while let Some(producer) = producers.join_next().await {
        producer.unwrap().unwrap();
    }
}

#[tokio::test]
async fn blocked_upload_payload_remains_in_shared_byte_budget_until_cancelled() {
    let budget = Arc::new(Semaphore::new(4));
    let session = Arc::new(Session::new(budget.clone(), 1));
    session
        .push(0, Bytes::from_static(b"a"), false)
        .await
        .unwrap();
    let blocked = {
        let session = session.clone();
        tokio::spawn(async move { session.push(1, Bytes::from_static(b"bcd"), false).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(budget.available_permits(), 0);
    let another = Session::new(budget.clone(), 1);
    assert!(another
        .push(0, Bytes::from_static(b"e"), false)
        .await
        .is_err());
    session.life.close();
    assert!(blocked.await.unwrap().is_err());
    assert_eq!(budget.available_permits(), 3);
    drop(session);
    assert_eq!(budget.available_permits(), 4);
}

use super::*;

#[tokio::test]
async fn http1_receipt_unblocks_connection_reuse_before_missing_packet_arrives() {
    let sessions = super::super::super::sessions::Sessions::default();
    let session = sessions.get_with_limit("reuse-gap", 1).unwrap();
    let reader = {
        let session = session.clone();
        tokio::spawn(async move { session.next().await })
    };
    // Fill the reassembly window, then ingress, while sequence zero has not
    // arrived. All of these bytes already belong to the bounded session.
    for sequence in 1..=4 {
        session
            .push(sequence, Bytes::from_static(b"x"), false)
            .await
            .unwrap();
    }
    let permits = Arc::new(tokio::sync::Semaphore::new(2));
    let profile = Profile::new(&crate::profile::OwnedSplitHttpProfile {
        host: Some("example.com".into()),
        path: "/tunnel".into(),
        mode: "packet-up".into(),
        options: Default::default(),
    });
    let response = tokio::time::timeout(
        Duration::from_millis(200),
        upload(
            profile.packet_request("reuse-gap", 5, b"last").unwrap(),
            profile.clone(),
            session.clone(),
            5,
            permits.clone().try_acquire_owned().unwrap(),
        ),
    )
    .await
    .expect("HTTP/1 client must receive its prior receipt before sending the missing packet")
    .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        permits.available_permits(),
        1,
        "acknowledgement retains admission ownership"
    );
    // The reference HTTP/1 client reads the previous response before writing
    // its next request. Only now can it deliver the missing sequence.
    let response = upload(
        profile.packet_request("reuse-gap", 0, b"first").unwrap(),
        profile,
        session.clone(),
        0,
        permits.clone().try_acquire_owned().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    tokio::time::timeout(Duration::from_secs(1), async {
        assert_eq!(reader.await.unwrap().unwrap().unwrap().data, "first");
        for _ in 1..=4 {
            assert_eq!(session.next().await.unwrap().unwrap().data, "x");
        }
        assert_eq!(session.next().await.unwrap().unwrap().data, "last");
        let _all = permits.acquire_many(2).await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn http1_receipt_keeps_budget_duplicate_checks_and_admission_deadline() {
    let sessions = super::super::super::sessions::Sessions::default();
    let session = sessions.get_with_limit("bounded-receipt", 1).unwrap();
    session
        .push(0, Bytes::from_static(b"first"), false)
        .await
        .unwrap();
    let permits = Arc::new(tokio::sync::Semaphore::new(2));
    let profile = Profile::new(&crate::profile::OwnedSplitHttpProfile {
        host: Some("example.com".into()),
        path: "/tunnel".into(),
        mode: "packet-up".into(),
        options: Default::default(),
    });
    let response = upload(
        profile
            .packet_request("bounded-receipt", 1, b"second")
            .unwrap(),
        profile.clone(),
        session.clone(),
        1,
        permits.clone().try_acquire_owned().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(permits.available_permits(), 1);
    let duplicate = upload(
        profile
            .packet_request("bounded-receipt", 1, b"duplicate")
            .unwrap(),
        profile,
        session.clone(),
        1,
        permits.clone().try_acquire_owned().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(duplicate.status(), 409);
    assert_eq!(permits.available_permits(), 1);
    // Exhaust the shared listener reservation without allocating payloads.
    // The queued first packet and the detached second packet retain 11 bytes.
    let chunk = 16 * 1024 * 1024;
    let _held: Vec<_> = (0..3)
        .map(|_| session.reserve_upload(chunk).unwrap())
        .collect();
    let _tail = session
        .reserve_upload(chunk - b"firstsecond".len())
        .unwrap();
    assert!(session.reserve_upload(1).is_err());
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    assert!(session.life.error().is_err());
    assert_eq!(permits.available_permits(), 2);
    let _released = session.reserve_upload(b"second".len()).unwrap();
    assert!(
        session.reserve_upload(1).is_err(),
        "queued data remains accounted for"
    );
}

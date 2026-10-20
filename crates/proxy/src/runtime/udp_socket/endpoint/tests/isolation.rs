use super::*;

#[tokio::test]
async fn opaque_client_session_isolation_survives_the_real_shared_owner() {
    let fixture = Fixture::new().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let mut owner = fixture.owner(scope, UdpReceiveQueueConfig::default(), None);
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let third = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // Opaque protocol keys include zero and the full u64 range. They are not
    // interpreted as host ComponentId/SessionId/AssociationId allocations.
    let (mut tx1, _rx1, mut ctl1) = fixture.open_isolated(
        &endpoint,
        first.local_addr().unwrap(),
        CancellationToken::new(),
        Some(0),
    );
    let (mut tx2, _rx2, mut ctl2) = fixture.open_isolated(
        &endpoint,
        second.local_addr().unwrap(),
        CancellationToken::new(),
        Some(0),
    );
    let (mut tx3, mut rx3, mut ctl3) = fixture.open_isolated(
        &endpoint,
        third.local_addr().unwrap(),
        CancellationToken::new(),
        Some(u64::MAX),
    );
    let same = send(&mut tx1, &first, b"first scope").await;
    assert_eq!(send(&mut tx2, &second, b"same scope").await, same);
    let isolated = send(&mut tx3, &third, b"different scope").await;
    assert_ne!(isolated, same);
    assert!(ctl1.wait_closed(deadline()).await.unwrap().is_complete());
    assert!(tokio::net::UdpSocket::bind(same).await.is_err());
    assert!(ctl2.wait_closed(deadline()).await.unwrap().is_complete());
    tokio::net::UdpSocket::bind(same).await.unwrap();
    third.send_to(b"still live", isolated).await.unwrap();
    let (bytes, guard) = rx3.recv().await.unwrap().unwrap();
    assert_eq!(bytes, b"still live");
    assert!(guard.is_current());
    assert!(ctl3.wait_closed(deadline()).await.unwrap().is_complete());
    assert!(!guard.is_current());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 2 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(isolated).await.unwrap();
}

use super::super::dispatch::DirectDispatchEndpoint;
use super::*;
fn business(fixture: &Fixture, peer: SocketAddr) -> SessionId {
    let mut business = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        peer.port(),
        Network::Udp,
        ProtocolType::UNKNOWN,
    );
    fixture
        .proxy
        .engine()
        .prepare_session(&mut business, "fixture-udp")
        .unwrap();
    SessionId(business.id)
}
#[tokio::test]
async fn typed_fan_in_uses_same_native_socket_and_preserves_final_delivery_guard() {
    let fixture = Fixture::new().await;
    let mut owner = fixture.owner(
        TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        ),
        UdpReceiveQueueConfig::default(),
        None,
    );
    owner.start(deadline()).await.unwrap();
    let mut bridge = DirectDispatchEndpoint::new(owner.endpoint(), fixture.retirement.clone());
    let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let one = business(&fixture, first.local_addr().unwrap());
    let two = business(&fixture, second.local_addr().unwrap());
    for id in [one, two] {
        bridge
            .admit(
                id,
                super::super::super::tests::policy(&fixture.proxy, Some("first")),
                None,
                CancellationToken::new(),
            )
            .unwrap();
    }
    let literal = Address::Ipv4([127, 0, 0, 1]);
    let mut bytes = [0; 64];
    bridge
        .send_new_packet(one, &literal, &[first.local_addr().unwrap()], b"first")
        .await
        .unwrap();
    let (_, shared) = first.recv_from(&mut bytes).await.unwrap();
    bridge
        .send_new_packet(two, &literal, &[second.local_addr().unwrap()], b"second")
        .await
        .unwrap();
    assert_eq!(second.recv_from(&mut bytes).await.unwrap().1, shared);
    first.send_to(b"already read", shared).await.unwrap();
    let (size, source) = bridge.recv_from_addr(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"already read");
    assert_eq!(source.session_id, Some(one.0));
    assert!(source.is_current());
    bridge.retire_session(one);
    assert!(
        !source.is_current(),
        "same token passed to final delivery revokes before any await"
    );
    second.send_to(b"", shared).await.unwrap();
    let (size, source) = bridge.recv_from_addr(&mut bytes).await.unwrap();
    assert_eq!(size, 0);
    assert_eq!(source.session_id, Some(two.0));
    assert!(source.is_current());
    assert!(bridge
        .finish_session(two, deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(!source.is_current());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.exited_tasks.len(),
        1,
        "fan-in never spawns a per-flow task"
    );
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(shared).await.unwrap();
}

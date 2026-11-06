use super::*;
use crate::component::datagram_retirement::DatagramReturns;
use zero_core::{Address, packet::Datagram, request::Destination};
use zero_platform_tokio::contract::datagram::{recv, send};

#[tokio::test]
async fn accepted_stop_retains_and_retires_late_original_adoption_without_stopping_global_admission()
 {
    let owner = owner(1);
    let returns = Arc::new(DatagramReturns::default());
    let reservation = owner.reserve_with_returns(Some(&returns)).unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let mut channel = socket.into_fixed_datagram_session(
        reservation.association(),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let report = owner
        .wait_returns(
            &returns,
            tokio::time::Instant::now() + std::time::Duration::from_millis(5),
        )
        .await
        .unwrap();
    assert!(
        !report.is_complete(),
        "the actual open reservation remains owned"
    );
    let original = owner.entries().into_iter().next().unwrap().1;
    channel.control = reservation.adopt(channel.control);
    assert!(
        owner
            .wait_returns(&returns, deadline())
            .await
            .unwrap()
            .is_complete()
    );
    assert!(
        matches!(recv(channel.rx.as_mut()).await, Ok(None) | Err(_)),
        "late original controller, not Drop or a port probe, retires its view"
    );
    assert!(
        original.lock().unwrap().closed,
        "same original controller consumed its actual close outcome"
    );
    drop(owner.reserve().unwrap());
}

#[tokio::test]
async fn accepted_returns_share_global_capacity_and_close_only_their_actual_original_views() {
    let owner = owner(2);
    let first = Arc::new(DatagramReturns::default());
    let second = Arc::new(DatagramReturns::default());
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let endpoint = peer.local_addr().unwrap();
    let reservation = owner.reserve_with_returns(Some(&first)).unwrap();
    let id = reservation.association();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let mut a = socket.into_fixed_datagram_session(id, endpoint, CancellationToken::new());
    let original_a = owner.entries().into_iter().next().unwrap().1;
    a.control = reservation.adopt(a.control);
    let reservation = owner.reserve_with_returns(Some(&second)).unwrap();
    let id = reservation.association();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let mut b = socket.into_fixed_datagram_session(id, endpoint, CancellationToken::new());
    b.control = reservation.adopt(b.control);
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    std::future::poll_fn(|cx| {
        assert!(
            a.rx.as_mut().poll_recv(cx).is_pending(),
            "actual native receive is suspended"
        );
        std::task::Poll::Ready(())
    })
    .await;
    let report = owner.wait_returns(&first, deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.released_resources.len(), 1);
    assert!(
        matches!(recv(a.rx.as_mut()).await, Ok(None) | Err(_)),
        "same original transferred RX is retired"
    );
    assert!(
        original_a.lock().unwrap().closed,
        "the original native controller finished; EOF alone is not closure proof"
    );
    let frame = Datagram {
        association: id,
        address: Destination::new(Address::Ipv4([127, 0, 0, 1]), endpoint.port()),
        payload: b"foreign".to_vec().into(),
    };
    send(b.tx.as_mut(), b.control.as_mut(), &frame, &b.limits)
        .await
        .unwrap();
    let mut bytes = [0; 16];
    let (size, _) = tokio::time::timeout_at(deadline(), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        &bytes[..size],
        b"foreign",
        "another actual original remains usable"
    );
    assert_eq!(
        owner
            .reserve_with_returns(Some(&first))
            .err()
            .unwrap()
            .reason,
        Reason::Revoked
    );
    drop(owner.reserve().unwrap());
    assert!(
        owner
            .wait_returns(&second, deadline())
            .await
            .unwrap()
            .is_complete()
    );
    assert!(matches!(recv(b.rx.as_mut()).await, Ok(None) | Err(_)));
    assert!(
        owner
            .wait_returns(&first, deadline())
            .await
            .unwrap()
            .released_resources
            .is_empty(),
        "no duplicate original receipts"
    );
}

#[tokio::test]
async fn accepted_return_failure_preserves_its_original_controller_source_and_retry_custody() {
    use std::error::Error;
    let owner = owner(1);
    let returns = Arc::new(DatagramReturns::default());
    let (control, eof, release, server) = tcp_control(true).await;
    let lease = owner
        .reserve_with_returns(Some(&returns))
        .unwrap()
        .adopt(control);
    let error = owner.wait_returns(&returns, deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::Protocol);
    let mut source = error.source();
    let mut actual = false;
    while let Some(error) = source {
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            assert_eq!(io.kind(), std::io::ErrorKind::InvalidData);
            actual = true;
        }
        source = error.source();
    }
    assert!(actual);
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    eof.notified().await;
    release.notify_one();
    assert!(
        owner
            .wait_returns(&returns, deadline())
            .await
            .unwrap()
            .is_complete()
    );
    drop(lease);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    drop(owner.reserve().unwrap());
}

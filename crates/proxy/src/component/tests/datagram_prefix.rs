use super::guard;
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    task::Poll,
};
use zero_core::{component::AssociationId, packet::Datagram, request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        datagram::{recv, send},
        CancellationToken, DeliveryState, Reason,
    },
    TokioDatagramSocket,
};
fn target(peer: SocketAddr) -> Destination {
    let IpAddr::V4(ip) = peer.ip() else {
        panic!("IPv4 fixture");
    };
    Destination::new(Address::Ipv4(ip.octets()), peer.port())
}
#[tokio::test]
async fn prefix_caller_cancellation_wakes_real_pending_udp_without_claiming_close() {
    let peer = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let logical = CancellationToken::new();
    let caller = CancellationToken::new();
    let raw = socket.into_fixed_datagram_session(
        AssociationId(41),
        peer.local_addr().unwrap(),
        logical.clone(),
    );
    let expected = raw.limits.clone();
    let mut channel = guard(raw, logical.clone(), caller.clone());
    assert_eq!(channel.limits, expected);
    {
        let received = recv(channel.rx.as_mut());
        tokio::pin!(received);
        std::future::poll_fn(|cx| {
            assert!(received.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        caller.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), received)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
    }
    assert!(logical.is_cancelled());
    // Cancellation has not closed the underlying socket or produced a receipt.
    assert!(TokioDatagramSocket::bind_addr(local).await.is_err());
    let frame = Datagram {
        address: target(peer.local_addr().unwrap()),
        association: AssociationId(41),
        payload: b"cancelled".to_vec().into(),
    };
    let failure = send(
        channel.tx.as_mut(),
        channel.control.as_mut(),
        &frame,
        &channel.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.error.reason, Reason::Cancelled);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    let report = channel
        .control
        .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
}
#[tokio::test]
async fn prefix_guard_preserves_actual_datagrams_association_limits_and_source() {
    let peer = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let logical = CancellationToken::new();
    let raw = socket.into_fixed_datagram_session(
        AssociationId(42),
        peer.local_addr().unwrap(),
        logical.clone(),
    );
    let mut channel = guard(raw, logical, CancellationToken::new());
    let address = target(peer.local_addr().unwrap());
    for payload in [b"wire payload".as_slice(), &[]] {
        let frame = Datagram {
            address: address.clone(),
            association: AssociationId(42),
            payload: payload.to_vec().into(),
        };
        assert_eq!(
            send(
                channel.tx.as_mut(),
                channel.control.as_mut(),
                &frame,
                &channel.limits
            )
            .await
            .unwrap()
            .accepted_bytes,
            payload.len()
        );
        let mut bytes = [0; 64];
        let (size, source) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(source, local);
        assert_eq!(&bytes[..size], payload);
        peer.send_to(&bytes[..size], source).await.unwrap();
        let received = recv(channel.rx.as_mut()).await.unwrap().unwrap();
        assert_eq!(received.address, address);
        assert_eq!(received.association, AssociationId(42));
        assert_eq!(received.payload.as_ref(), payload);
    }
    assert!(channel
        .control
        .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap()
        .is_complete());
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
}

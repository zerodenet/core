use std::time::Duration;
use zero_core::{component::AssociationId, packet::Datagram, request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        datagram::{recv, send},
        CancellationToken, CloseReason, DeliveryState, Reason,
    },
    TokioDatagramSocket,
};
fn target(peer: std::net::SocketAddr) -> Destination {
    Destination::new(
        Address::Ipv4(
            peer.ip()
                .to_string()
                .parse::<std::net::Ipv4Addr>()
                .unwrap()
                .octets(),
        ),
        peer.port(),
    )
}
fn frame(address: Destination, payload: &[u8]) -> Datagram {
    Datagram {
        address,
        association: AssociationId(11),
        payload: payload.to_vec().into(),
    }
}

#[tokio::test]
async fn raw_datagram_session_retains_zero_length_boundaries_and_closes_the_original_shared_descriptor(
) {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let address = target(peer.local_addr().unwrap());
    let mut session = socket.into_fixed_datagram_session(
        AssociationId(11),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    for payload in [b"actual UDP payload".as_slice(), &[]] {
        let receipt = send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &frame(address.clone(), payload),
            &session.limits,
        )
        .await
        .unwrap();
        assert_eq!(receipt.accepted_bytes, payload.len());
        assert_eq!(receipt.delivery, DeliveryState::LocallyAccepted);
        let mut bytes = [0; 64];
        let (size, sender) =
            tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(sender, local);
        assert_eq!(&bytes[..size], payload);
        peer.send_to(&bytes[..size], sender).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), recv(session.rx.as_mut()))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(received.association, AssociationId(11));
        assert_eq!(received.address, address);
        assert_eq!(received.payload.as_ref(), payload);
    }
    session.control.cancel(CloseReason::Completed);
    let report = session
        .control
        .wait_closed(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    // Tx and Rx still exist. Closing a cloned descriptor would leave this bind
    // occupied; explicit control closed their one original socket instead.
    let replacement = TokioDatagramSocket::bind_addr(local).await.unwrap();
    assert!(recv(session.rx.as_mut()).await.unwrap().is_none());
    assert!(send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame(address, b"late"),
        &session.limits
    )
    .await
    .is_err());
    replacement.close().unwrap();
}

#[tokio::test]
async fn raw_datagram_session_rejects_foreign_sources_wrong_associations_and_oversized_frames() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let foreign = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let address = target(peer.local_addr().unwrap());
    let mut session = socket.into_fixed_datagram_session(
        AssociationId(11),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    foreign.send_to(b"foreign", local).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), recv(session.rx.as_mut()))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.reason, Reason::Protocol);
    let mut wrong = frame(address.clone(), b"wrong");
    wrong.association = AssociationId(12);
    assert_eq!(
        send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &wrong,
            &session.limits
        )
        .await
        .unwrap_err()
        .delivery,
        DeliveryState::NotSubmitted
    );
    let over = frame(address, &vec![1; session.limits.max_payload + 1]);
    assert_eq!(
        send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &over,
            &session.limits
        )
        .await
        .unwrap_err()
        .error
        .reason,
        Reason::Capacity
    );
    session
        .control
        .wait_closed(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelling_a_pending_raw_receive_wakes_its_unique_receiver_and_keeps_explicit_close() {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let mut session = socket.into_fixed_datagram_session(
        AssociationId(13),
        peer.local_addr().unwrap(),
        cancel.clone(),
    );
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let ready = entered.clone();
    let receiving = tokio::spawn(async move {
        {
            let receiving = recv(session.rx.as_mut());
            tokio::pin!(receiving);
            // Poll the actual socket to Pending before reporting readiness.
            std::future::poll_fn(|cx| {
                use std::{future::Future, task::Poll};
                assert!(receiving.as_mut().poll(cx).is_pending());
                ready.notify_one();
                Poll::Ready(())
            })
            .await;
            assert!(receiving.await.unwrap().is_none());
        }
        session
    });
    entered.notified().await;
    cancel.cancel();
    let mut session = tokio::time::timeout(Duration::from_secs(2), receiving)
        .await
        .unwrap()
        .unwrap();
    let report = session
        .control
        .wait_closed(tokio::time::Instant::now() + Duration::from_secs(2))
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

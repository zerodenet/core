use super::*;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, Instant},
};
use zero_core::{
    component::{AssociationId, ComponentId},
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address,
};
use zero_platform_tokio::contract::{
    admission::SessionOwner,
    datagram::{recv, DatagramRx, DatagramTx},
    lifecycle::{ChannelControl, ResourceLifecycle},
    CancellationToken,
};

async fn session(
    capacity: usize,
    targets: DatagramTargets,
) -> (
    SessionOwner,
    DatagramSession<tx::Tx, rx::Rx, control::Control>,
    tokio::io::DuplexStream,
) {
    let (mut owner, admission) = SessionOwner::new(ComponentId(1), &CancellationToken::new());
    owner
        .start(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    let (io, peer) = tokio::io::duplex(capacity);
    let shared = shared::Shared::codec_fixture(io, admission, None, CancellationToken::new());
    let limits = DatagramLimits {
        max_payload: 65535 - 262,
        targets,
        queued_bytes: 65539,
    };
    (
        owner,
        DatagramSession {
            tx: tx::Tx::new(shared.clone(), AssociationId(1), limits.clone()),
            rx: rx::Rx::new(shared.clone(), AssociationId(1), limits.clone()),
            control: control::Control::new(shared),
            limits,
        },
        peer,
    )
}
fn frame(payload: &[u8]) -> Datagram {
    Datagram {
        association: AssociationId(1),
        address: Destination::new(Address::Ipv4([203, 0, 113, 1]), 53),
        payload: payload.to_vec().into(),
    }
}
#[tokio::test]
async fn cancelled_partial_receive_retains_frame_and_zero_payload() {
    let (_owner, mut session, mut peer) = session(128, DatagramTargets::Multiple).await;
    let expected = frame(b"");
    let wire = crate::udp::MieruUdpFlowCodec
        .encode_packet(&expected.address.address, 53, b"")
        .unwrap();
    peer.write_all(&wire[..2]).await.unwrap();
    let mut receiving = Box::pin(recv(Pin::new(&mut session.rx)));
    poll_fn(|cx| {
        assert!(receiving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(receiving);
    peer.write_all(&wire[2..]).await.unwrap();
    assert_eq!(
        recv(Pin::new(&mut session.rx)).await.unwrap().unwrap(),
        expected
    );
    session.control.cancel(CloseReason::Shutdown);
    for _ in 0..2 {
        poll_fn(|cx| {
            assert!(Pin::new(&mut session.rx).poll_recv(cx).is_ready());
            Poll::Ready(())
        })
        .await;
    }
    drop(session.tx);
    drop(session.rx);
    let report = session
        .control
        .wait_closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
}
#[tokio::test]
async fn a_different_frame_after_partial_send_is_unknown_and_isolates_only_this_session() {
    let (_owner, mut session, mut peer) = session(4, DatagramTargets::Multiple).await;
    let original = frame(b"first");
    poll_fn(|cx| {
        assert!(Pin::new(&mut session.tx)
            .poll_send(cx, &original)
            .is_pending());
        Poll::Ready(())
    })
    .await;
    let mut prefix = [0; 4];
    peer.read_exact(&mut prefix).await.unwrap();
    assert_eq!(prefix[0], 0, "the original wire write actually started");
    let different = frame(b"other");
    let failure = poll_fn(|cx| Pin::new(&mut session.tx).poll_send(cx, &different))
        .await
        .unwrap_err();
    assert_eq!(failure.delivery, DeliveryState::Unknown);
    for _ in 0..2 {
        assert!(recv(Pin::new(&mut session.rx)).await.is_err());
    }
    drop(session.tx);
    drop(session.rx);
    let report = session
        .control
        .wait_closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
}

#[tokio::test]
async fn fixed_domain_send_targets_preserve_resolved_response_sources() {
    let target = Destination::new(Address::Domain("udp.example.test".into()), 53);
    let (_owner, mut session, mut peer) = session(128, DatagramTargets::Fixed(target)).await;
    let expected = frame(b"response");
    let wire = crate::udp::MieruUdpFlowCodec
        .encode_packet(&expected.address.address, 53, &expected.payload)
        .unwrap();
    peer.write_all(&wire).await.unwrap();
    assert_eq!(
        recv(Pin::new(&mut session.rx)).await.unwrap().unwrap(),
        expected
    );
    let wrong_send = poll_fn(|cx| Pin::new(&mut session.tx).poll_send(cx, &expected))
        .await
        .unwrap_err();
    assert_eq!(
        wrong_send.delivery,
        DeliveryState::NotSubmitted,
        "send authorization remains fixed to the requested domain"
    );
    peer.write_all(&wire).await.unwrap();
    assert_eq!(
        recv(Pin::new(&mut session.rx)).await.unwrap().unwrap(),
        expected
    );
    drop(session.tx);
    drop(session.rx);
    assert!(session
        .control
        .wait_closed(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .is_complete());
}

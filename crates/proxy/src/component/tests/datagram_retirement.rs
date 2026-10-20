use super::RetiredDatagrams;
use std::{num::NonZeroUsize, sync::Arc};
use zero_core::component::AssociationId;
use zero_platform_tokio::{
    contract::{CancellationToken, Reason},
    TokioDatagramSocket,
};
#[path = "datagram_retirement/fixture.rs"]
mod fixture;
use fixture::{deadline, tcp_control};
fn owner(capacity: usize) -> Arc<RetiredDatagrams> {
    Arc::new(RetiredDatagrams::new(NonZeroUsize::new(capacity).unwrap()))
}

#[tokio::test]
async fn reservation_bounds_active_and_retiring_and_drop_cannot_claim_native_udp_close() {
    let owner = owner(1);
    let reservation = owner.reserve().unwrap();
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let mut channel = socket.into_fixed_datagram_session(
        AssociationId(31),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    channel.control = reservation.adopt(channel.control);
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    drop(channel.control);
    assert!(TokioDatagramSocket::bind_addr(local).await.is_err());
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    let report = owner.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    // Unique Tx/Rx remain alive. This is the original descriptor's close.
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Revoked);
}
#[tokio::test]
async fn unadopted_open_stays_unfinished_until_its_actual_reservation_is_released() {
    let owner = owner(1);
    {
        let unused = owner.reserve().unwrap();
        drop(unused);
    }
    let opening = owner.reserve().unwrap();
    let report = owner
        .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_millis(5))
        .await
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.closed_io.is_empty());
    drop(opening);
    assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn interrupted_lease_close_and_deadline_retain_the_same_real_tcp_controller() {
    let owner = owner(1);
    let reservation = owner.reserve().unwrap();
    let (control, eof, release, server) = tcp_control(false).await;
    let mut lease = reservation.adopt(control);
    {
        let closing = lease.wait_closed(deadline());
        tokio::pin!(closing);
        tokio::select! {result=&mut closing=>panic!("peer EOF is still gated: {result:?}"),_=eof.notified()=>{}}
    }
    drop(lease);
    let retained = owner.clone();
    drop(owner);
    let partial = retained
        .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_millis(5))
        .await
        .unwrap();
    assert!(!partial.is_complete());
    assert!(partial.closed_io.is_empty());
    release.notify_one();
    let complete = retained.wait_closed(deadline()).await.unwrap();
    assert!(complete.is_complete());
    assert_eq!(complete.closed_io.len(), 1);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    assert!(retained
        .wait_closed(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
}
#[tokio::test]
async fn actual_control_failure_keeps_its_source_controller_and_retry_ownership() {
    use std::error::Error;
    let owner = owner(1);
    let (control, eof, release, server) = tcp_control(true).await;
    let lease = owner.reserve().unwrap().adopt(control);
    drop(lease);
    let error = owner.wait_closed(deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::Protocol);
    let mut source = error.source();
    let mut original = false;
    while let Some(value) = source {
        if let Some(io) = value.downcast_ref::<std::io::Error>() {
            assert_eq!(io.kind(), std::io::ErrorKind::InvalidData);
            original = true;
        }
        source = value.source();
    }
    assert!(original);
    eof.notified().await;
    release.notify_one();
    let complete = owner.wait_closed(deadline()).await.unwrap();
    assert!(complete.is_complete());
    assert_eq!(complete.closed_io.len(), 1);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn a_started_open_can_adopt_after_stop_without_losing_its_pre_reserved_controller() {
    let owner = owner(1);
    let reservation = owner.reserve().unwrap();
    assert!(!owner
        .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_millis(5))
        .await
        .unwrap()
        .is_complete());
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let channel = socket.into_fixed_datagram_session(
        AssociationId(32),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let lease = reservation.adopt(channel.control);
    drop(lease);
    let report = owner.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Revoked);
}

#[tokio::test]
async fn normal_udp_driver_recycles_capacity_one_for_two_real_socket_batches_without_global_stop() {
    use std::{future::Future, task::Poll};
    let owner = owner(1);
    for association in [AssociationId(51), AssociationId(52)] {
        let reservation = owner.reserve().unwrap();
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
        let local = socket.local_addr().unwrap();
        let channel = socket.into_fixed_datagram_session(
            association,
            peer.local_addr().unwrap(),
            CancellationToken::new(),
        );
        let lease = reservation.adopt(channel.control);
        let progressing = owner.progress_retired(deadline());
        tokio::pin!(progressing);
        std::future::poll_fn(|cx| {
            assert!(progressing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        // No remote UDP response exists; only this retirement notification can
        // wake the already registered normal-run progress waiter.
        drop(lease);
        let report = tokio::time::timeout_at(deadline(), progressing)
            .await
            .unwrap()
            .unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        TokioDatagramSocket::bind_addr(local)
            .await
            .unwrap()
            .close()
            .unwrap();
    }
    drop(owner.reserve().unwrap());
}

#[tokio::test]
async fn a_real_pending_or_failed_tcp_close_does_not_pin_another_closed_udp_reservation() {
    let owner = owner(2);
    let (control, eof, release, server) = tcp_control(true).await;
    drop(owner.reserve().unwrap().adopt(control));
    let progressing = owner.progress_retired(deadline());
    tokio::pin!(progressing);
    std::future::poll_fn(|cx| {
        use std::{future::Future, task::Poll};
        assert!(progressing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::timeout_at(deadline(), eof.notified())
        .await
        .unwrap();
    let reservation = owner.reserve().unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = TokioDatagramSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let channel = socket.into_fixed_datagram_session(
        AssociationId(53),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    drop(reservation.adopt(channel.control));
    let report = progressing.await.unwrap();
    assert_eq!(report.closed_io.len(), 1);
    assert!(!report.is_complete());
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
    // UDP's confirmed receipt releases its slot even though the real TCP peer
    // has not allowed EOF, and its original protocol failure remains owned.
    drop(owner.reserve().unwrap());
    let error = owner.progress_retired(deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::Protocol);
    release.notify_one();
    let report = owner.progress_retired(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
}

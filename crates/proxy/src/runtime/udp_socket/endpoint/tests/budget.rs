use super::*;
use std::{
    future::{poll_fn, Future},
    mem::size_of,
    sync::atomic::Ordering,
    task::Poll,
};

#[tokio::test]
async fn actual_closed_native_slots_reuse_without_a_lifetime_attempt_limit() {
    let fixture = Fixture::new().await;
    let mut grants = limits();
    grants.native_sockets = NonZeroUsize::new(1).unwrap();
    grants.pending_native_opens = NonZeroUsize::new(1).unwrap();
    let mut owner = fixture.owner_with_limits(
        TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        ),
        UdpReceiveQueueConfig::default(),
        None,
        grants,
    );
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let (mut tx1, _rx1, mut ctl1) = fixture.open(&endpoint, target, CancellationToken::new());
    let original = send(&mut tx1, &peer, b"one original").await;
    let (mut tx2, _rx2, mut ctl2) = fixture.open(&endpoint, target, CancellationToken::new());
    assert!(tx2
        .send_new_packet(
            &Address::Ipv4([127, 0, 0, 1]),
            &[target],
            b"collision needs another original"
        )
        .await
        .is_err());
    assert!(ctl2.wait_closed(deadline()).await.unwrap().is_complete());
    assert_eq!(send(&mut tx1, &peer, b"healthy original").await, original);
    assert!(ctl1.wait_closed(deadline()).await.unwrap().is_complete());
    tokio::net::UdpSocket::bind(original).await.unwrap();
    for _ in 0..128 {
        let (mut tx, _rx, mut control) = fixture.open(&endpoint, target, CancellationToken::new());
        let local = send(&mut tx, &peer, b"capacity reused after actual close").await;
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        tokio::net::UdpSocket::bind(local).await.unwrap();
    }
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 129 original Direct UDP sockets"]
    );
}

#[tokio::test]
async fn command_copy_budget_covers_domain_candidates_payload_and_cancelled_owned_queue() {
    let fixture = Fixture::new().await;
    let mut grants = limits();
    grants.logical = NonZeroUsize::new(2).unwrap();
    grants.candidates_per_job = NonZeroUsize::new(1).unwrap();
    grants.pending_bytes = NonZeroUsize::new(size_of::<SocketAddr>() + 2).unwrap();
    let mut owner = fixture.owner_with_limits(
        TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        ),
        UdpReceiveQueueConfig::default(),
        None,
        grants,
    );
    let shared = owner.shared.clone();
    let io_guard = shared.sockets.lock().await;
    owner.start(deadline()).await.unwrap();
    let endpoint = owner.endpoint();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let (mut tx, _rx, mut control) = fixture.open(&endpoint, target, CancellationToken::new());
    assert!(tx
        .send_new_packet(&Address::Domain("domain.example".into()), &[target], b"ok")
        .await
        .is_err());
    assert!(tx
        .send_new_packet(&Address::Ipv4([127, 0, 0, 1]), &[target, target], b"")
        .await
        .is_err());
    assert_eq!(shared.pending_bytes.load(Ordering::Acquire), 0);
    let literal = Address::Ipv4([127, 0, 0, 1]);
    let candidates = [target];
    let mut pending = Box::pin(tx.send_new_packet(&literal, &candidates, b"ok"));
    poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        shared.pending_bytes.load(Ordering::Acquire),
        grants.pending_bytes.get()
    );
    drop(pending);
    let (mut other, _other_rx, mut other_control) =
        fixture.open(&endpoint, target, CancellationToken::new());
    assert!(other
        .send_new_packet(&literal, &candidates, b"")
        .await
        .is_err());
    assert!(
        fixture
            .try_open(&endpoint, target, CancellationToken::new(), None)
            .is_err(),
        "active+retiring logical slots remain charged"
    );
    assert!(
        io_guard.sockets.is_empty(),
        "no native fd opens behind the blocked actual state owner"
    );
    drop(io_guard);
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
    assert_eq!(shared.pending_bytes.load(Ordering::Acquire), 0);
    assert!(other_control
        .wait_closed(deadline())
        .await
        .unwrap()
        .is_complete());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
}

#[tokio::test]
async fn same_actual_logical_flow_rotates_network_epochs_with_native_capacity_one() {
    let fixture = Fixture::new().await;
    let mut grants = limits();
    grants.native_sockets = NonZeroUsize::new(1).unwrap();
    grants.pending_native_opens = NonZeroUsize::new(1).unwrap();
    let mut owner = fixture.owner_with_limits(
        TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        ),
        UdpReceiveQueueConfig::default(),
        None,
        grants,
    );
    owner.start(deadline()).await.unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (mut tx, mut rx, mut control) = fixture.open(
        &owner.endpoint(),
        peer.local_addr().unwrap(),
        CancellationToken::new(),
    );
    let mut previous = None;
    for _ in 0..32 {
        if let Some(source) = previous.take() {
            fixture.proxy.egress_interface.invalidate_network();
            assert!(!DirectUdpResponseSource::is_current(&source));
        }
        let original = send(&mut tx, &peer, b"same flow new epoch").await;
        peer.send_to(b"current", original).await.unwrap();
        let (bytes, source) = rx.recv().await.unwrap().unwrap();
        assert_eq!(bytes, b"current");
        assert!(source.is_current());
        previous = Some(source);
    }
    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
    assert!(!previous.unwrap().is_current());
    let report = owner.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 32 original Direct UDP sockets"]
    );
}

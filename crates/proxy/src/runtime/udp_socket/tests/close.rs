use super::*;
use std::time::Duration;
#[tokio::test]
async fn explicit_retirement_keeps_shared_port_tombstones_and_other_peer_alive() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut bytes = [0; 64];
    sockets
        .send_to_addr(b"first", first.local_addr().unwrap(), 1, &policy)
        .await
        .unwrap();
    let (_, shared) = first.recv_from(&mut bytes).await.unwrap();
    sockets
        .send_to_addr(b"second", second.local_addr().unwrap(), 2, &policy)
        .await
        .unwrap();
    assert_eq!(second.recv_from(&mut bytes).await.unwrap().1, shared);
    first.send_to(b"old reply", shared).await.unwrap();
    let (_, source) = sockets.recv_from_addr(&mut bytes).await.unwrap();
    assert!(source.is_current());
    sockets.retire_session(1);
    assert!(!source.is_current());
    assert_eq!(sockets.sockets.len(), 1);
    assert!(sockets.take_native_close_report().closed_io.is_empty());
    assert!(tokio::net::UdpSocket::bind(shared).await.is_err());
    // The retired tuple cannot be reassigned while its old shared socket lives.
    sockets
        .send_to_addr(b"new first", first.local_addr().unwrap(), 3, &policy)
        .await
        .unwrap();
    let (_, isolated) = first.recv_from(&mut bytes).await.unwrap();
    assert_ne!(isolated, shared);
    second.send_to(b"still second", shared).await.unwrap();
    let (size, current) =
        tokio::time::timeout(Duration::from_secs(2), sockets.recv_from_addr(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&bytes[..size], b"still second");
    assert_eq!(current.session_id, Some(2));
    sockets.retire_session(2);
    assert!(!current.is_current());
    assert_eq!(sockets.sockets.len(), 1);
    let report = sockets.take_native_close_report();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(shared).await.unwrap();
    assert!(sockets.native_close_failure().is_none());
    sockets.close_all_native();
    assert!(sockets.take_native_close_report().is_complete());
    tokio::net::UdpSocket::bind(isolated).await.unwrap();
}
#[tokio::test]
async fn native_close_ledger_aggregates_real_success_without_clearing_unreported_receipts_on_reuse()
{
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut bytes = [0; 32];
    for session in 1..=8 {
        sockets
            .send_to_addr(
                b"actual socket",
                peer.local_addr().unwrap(),
                session,
                &policy,
            )
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut bytes).await.unwrap();
        sockets.retire_session(session);
        tokio::net::UdpSocket::bind(local).await.unwrap();
    }
    let report = sockets.take_native_close_report();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 8 original Direct UDP sockets"]
    );
    assert!(sockets.take_native_close_report().closed_io.is_empty());
    assert!(sockets.native_close_failure().is_none());
}

#[tokio::test]
async fn socket_identity_exhaustion_rejects_new_collision_before_bind_and_keeps_live_peer() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mut bytes = [0; 16];
    sockets
        .send_to_addr(b"first", target, 1, &policy)
        .await
        .unwrap();
    let (_, local) = peer.recv_from(&mut bytes).await.unwrap();
    sockets.next_socket_id = u64::MAX;
    // Same-peer ownership collision really needs another native socket. The
    // checked ID rejection happens before bind, so there is no untracked fd.
    assert!(sockets
        .send_to_addr(b"collision", target, 2, &policy)
        .await
        .is_err());
    assert_eq!(sockets.sockets.len(), 1);
    assert!(sockets.native_close_failure().is_none());
    sockets
        .send_to_addr(b"healthy", target, 1, &policy)
        .await
        .unwrap();
    let (size, sender) = peer.recv_from(&mut bytes).await.unwrap();
    assert_eq!(sender, local);
    assert_eq!(&bytes[..size], b"healthy");
    sockets.close_all_native();
    let report = sockets.take_native_close_report();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(local).await.unwrap();
}

#[tokio::test]
async fn deferred_native_retirement_revokes_guards_but_keeps_original_fds_and_budget() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    sockets.defer_native_close = true;
    sockets.native_owner = Some(
        zero_platform_tokio::controlled_datagram::ControlledDatagramOwner::new(
            std::num::NonZeroUsize::new(1).unwrap(),
        ),
    );
    let policy = policy(&proxy, Some("first"));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mut bytes = [0; 32];
    sockets
        .send_to_addr(b"actual", target, 1, &policy)
        .await
        .unwrap();
    let (_, local) = peer.recv_from(&mut bytes).await.unwrap();
    peer.send_to(b"read guard", local).await.unwrap();
    let (_, guard) = sockets.recv_from_addr(&mut bytes).await.unwrap();
    assert!(guard.is_current());
    sockets.retire_session(1);
    assert!(!guard.is_current());
    assert_eq!(sockets.sockets.len(), 1);
    assert!(sockets.sockets[0].retired);
    assert!(sockets.take_native_close_report().closed_io.is_empty());
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    // Logical retirement alone leaves the old descriptor charged.
    assert!(sockets
        .send_to_addr(b"forbidden reuse", target, 2, &policy)
        .await
        .is_err());
    assert!(sockets.take_native_close_report().closed_io.is_empty());
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    // Normal topology rotation is performed by this sole mutable I/O owner,
    // after its old poll ended. Only the actual syscall receipt frees capacity.
    proxy.egress_interface.invalidate_network();
    sockets.refresh_if_stale();
    assert!(sockets.sockets.is_empty());
    let rotated = sockets.take_native_close_report();
    assert_eq!(
        rotated.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(local).await.unwrap();
    sockets
        .send_to_addr(b"new epoch", target, 2, &policy)
        .await
        .unwrap();
    let (_, current) = peer.recv_from(&mut bytes).await.unwrap();
    assert!(!guard.is_current());
    // Final lifecycle closure still needs its separate actual task proof.
    // This unit checks only original FD receipts, never constructs TaskExit.
    sockets.close_all_native();
    let report = sockets.take_native_close_report();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io,
        vec!["explicitly closed 1 original Direct UDP sockets"]
    );
    tokio::net::UdpSocket::bind(current).await.unwrap();
}

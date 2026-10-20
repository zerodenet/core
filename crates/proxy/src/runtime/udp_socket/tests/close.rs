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

use super::*;
use crate::EgressInterfaceControl;
use zero_traits::DialPolicy;

#[tokio::test]
async fn exact_original_control_is_unique_foreign_isolated_and_drop_never_closes() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let selected = EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let mut other = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let socket = owner
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap();
    let foreign = other
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap();
    assert_eq!(socket.reservation(), foreign.reservation());
    assert!(owner.take_control(&foreign).is_none());
    let control = owner.take_control(&socket).unwrap();
    assert!(owner.take_control(&socket).is_none());
    let foreign_control = other.take_control(&foreign).unwrap();
    let identity = control.identity();
    assert!(identity.matches_socket(&socket));
    assert!(identity.matches_control(&control));
    assert!(!identity.matches_socket(&foreign));
    assert!(!identity.matches_control(&foreign_control));
    let local = socket.local_addr().unwrap();
    socket.send_to_addr(b"same", peer).await.unwrap();
    let mut bytes = [0; 4];
    let (size, source) = target.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"same");
    assert_eq!(source.port(), local.port());
    assert!(source.ip().is_loopback());
    control.revoke();
    assert!(socket.send_to_addr(b"revoked", peer).await.is_err());
    assert!(!control.observe().native_closed);
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    let receipt = control.close().unwrap();
    assert!(receipt.matches(&control));
    assert!(!receipt.matches(&foreign_control));
    assert!(control.observe().native_closed);
    assert_eq!(owner.occupied(), 0);
    let _rebound = tokio::net::UdpSocket::bind(local).await.unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    let local = foreign.local_addr().unwrap();
    drop(foreign_control);
    assert_eq!(other.occupied(), 1);
    assert!(foreign.send_to_addr(b"dropped", peer).await.is_err());
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    assert_eq!(other.close_all().confirmed_closed, 1);
    let _rebound_foreign = tokio::net::UdpSocket::bind(local).await.unwrap();
}

#[tokio::test]
async fn original_controls_reuse_one_fd_slot_for_sixty_five_actual_acquisitions() {
    let peer = "127.0.0.1:9".parse().unwrap();
    let selected = EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    for _ in 0..65 {
        let socket = owner
            .acquire(peer, None, &DialPolicy::default(), &selected)
            .unwrap();
        let control = owner.take_control(&socket).unwrap();
        let local = socket.local_addr().unwrap();
        assert!(control.try_close().unwrap().unwrap().matches(&control));
        assert_eq!(owner.occupied(), 0);
        assert!(socket.send_to_addr(b"closed", peer).await.is_err());
        let _replacement = tokio::net::UdpSocket::bind(local).await.unwrap();
    }
    assert_eq!(owner.take_report().confirmed_closed, 65);
}

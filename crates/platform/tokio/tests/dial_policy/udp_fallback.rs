use super::*;

#[tokio::test]
async fn exact_udp_listener_collision_returns_its_original_close_receipt_without_port_fallback() {
    use std::num::NonZeroUsize;
    use zero_platform_tokio::controlled_datagram::{AcquisitionOutcome, ControlledDatagramOwner};
    let held = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = held.local_addr().unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let failure = match owner.bind_listener(address, NonZeroUsize::new(1).unwrap()) {
        Ok(_) => panic!("an exact listener must not fall back to a different port"),
        Err(failure) => failure,
    };
    let AcquisitionOutcome::Closed(receipt) = failure.outcome else {
        panic!("the failed original must have a native close receipt");
    };
    assert_eq!(Some(receipt.reservation()), failure.reservation);
    assert!(failure.to_string().contains("bind_source"));
    assert_eq!(owner.occupied(), 0);
    let report = owner.take_report();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 1);
    assert_eq!(report.known_no_native, 0);
    assert_eq!(held.local_addr().unwrap(), address);
}

#[tokio::test]
async fn controlled_udp_port_fallback_preserves_source_and_original_close_accounting() {
    use std::{num::NonZeroUsize, time::Duration};
    use zero_platform_tokio::controlled_datagram::ControlledDatagramOwner;
    let held = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let preferred = held.local_addr().unwrap();
    let peer_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = peer_socket.local_addr().unwrap();
    let policy = source_policy("127.0.0.1");
    let selection = EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &policy)
        .unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let socket = owner
        .acquire(peer, Some(preferred.port()), &policy, &selection)
        .unwrap();
    let control = owner.take_control(&socket).unwrap();
    assert_eq!(socket.local_addr().unwrap().ip(), preferred.ip());
    assert_ne!(socket.local_addr().unwrap().port(), preferred.port());
    assert_eq!(owner.occupied(), 1);
    socket.send_to_addr(b"heartbeat", peer).await.unwrap();
    let mut bytes = [0; 16];
    let (size, sender) =
        tokio::time::timeout(Duration::from_secs(3), peer_socket.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&bytes[..size], b"heartbeat");
    assert_eq!(sender, socket.local_addr().unwrap());
    peer_socket.send_to(b"reply", sender).await.unwrap();
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(3), socket.recv_from_addr(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&bytes[..size], b"reply");
    assert_eq!(source, peer);
    let receipt = control.close().unwrap();
    assert!(receipt.matches(&control));
    assert!(control.observe().native_closed);
    assert!(socket.send_to_addr(b"retired", peer).await.is_err());
    assert_eq!(owner.occupied(), 0);
    let report = owner.take_report();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 1);
    assert_eq!(report.known_no_native, 0);
    assert_eq!(held.local_addr().unwrap(), preferred);
}

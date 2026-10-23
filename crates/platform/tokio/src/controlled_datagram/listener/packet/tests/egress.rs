use super::*;
use crate::EgressInterface;

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn host_peer() -> tokio::net::UdpSocket {
    let probe = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    probe.connect("192.0.2.1:9").unwrap();
    let address = std::env::var("ZERO_TEST_HOST_IPV4")
        .map(|value| value.parse::<std::net::IpAddr>().unwrap())
        .unwrap_or_else(|_| probe.local_addr().unwrap().ip());
    assert!(!address.is_loopback() && !address.is_unspecified());
    tokio::net::UdpSocket::bind((address, 0)).await.unwrap()
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn receive(peer: &tokio::net::UdpSocket, expected: &[u8], port: u16) {
    let mut buffer = [0; 32];
    let (n, source) = tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..n], expected);
    assert_eq!(source.port(), port);
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn controlled_packet_rechecks_changed_generation_and_rejects_unowned_probe() {
    let peer = host_peer().await;
    let target = peer.local_addr().unwrap();
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner
        .bind_listener("0.0.0.0:0".parse().unwrap(), bound(1))
        .unwrap();
    let port = rx.local_addr().unwrap().port();
    let control = EgressInterfaceControl::default();
    control.replace(Some(actual_interface(target.ip())));
    let (_, mut sender) =
        PacketSocketReceiver::from_granted(GrantedDatagramListener { rx, tx }, control.clone());
    sender.send_to(b"before", target).await.unwrap();
    receive(&peer, b"before", port).await;
    control.replace_tunnel_addresses(["10.0.0.1".parse().unwrap()]);
    control.mark_unavailable_for(false, "physical_link_lost");
    assert_eq!(
        sender
            .send_to(b"unavailable", target)
            .await
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::Unsupported
    );
    let mut bytes = [0; 32];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    control.clear();
    control.replace(Some(actual_interface(target.ip())));
    sender.send_to(b"after", target).await.unwrap();
    receive(&peer, b"after", port).await;
    // Recovery to ordinary host routing needs no hidden route-probe socket.
    control.clear();
    sender.send_to(b"kernel-route", target).await.unwrap();
    receive(&peer, b"kernel-route", port).await;
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn actual_ancillary_interface_error_keeps_native_source_without_unmarked_retry() {
    let peer = host_peer().await;
    let target = peer.local_addr().unwrap();
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner
        .bind_listener("0.0.0.0:0".parse().unwrap(), bound(1))
        .unwrap();
    let port = rx.local_addr().unwrap().port();
    let control = EgressInterfaceControl::default();
    control.replace(Some(
        EgressInterface::new("zero-missing0", u32::MAX).unwrap(),
    ));
    let (_, mut sender) =
        PacketSocketReceiver::from_granted(GrantedDatagramListener { rx, tx }, control.clone());
    let error = sender.send_to(b"must-not-leak", target).await.unwrap_err();
    assert!(
        error.raw_os_error().is_some(),
        "actual sendmsg source: {error:?}"
    );
    assert_ne!(error.kind(), std_io::ErrorKind::WouldBlock);
    let mut bytes = [0; 32];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    control.clear();
    control.replace(Some(actual_interface(target.ip())));
    sender.send_to(b"recovered", target).await.unwrap();
    receive(&peer, b"recovered", port).await;
    assert!(native.packet_failure().is_none());
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn actual_interface(address: std::net::IpAddr) -> EgressInterface {
    use std::ffi::CStr;
    let mut head = std::ptr::null_mut();
    // The OS owns this null-terminated list until freeifaddrs. Read only the
    // AF_INET variant; names and sockaddr values remain valid during the loop.
    assert_eq!(unsafe { libc::getifaddrs(&mut head) }, 0);
    let mut cursor = head;
    let mut found = None;
    while !cursor.is_null() {
        let entry = unsafe { &*cursor };
        if !entry.ifa_addr.is_null()
            && unsafe { (*entry.ifa_addr).sa_family as i32 } == libc::AF_INET
        {
            let ipv4 = unsafe { &*entry.ifa_addr.cast::<libc::sockaddr_in>() };
            let ip = std::net::Ipv4Addr::from(ipv4.sin_addr.s_addr.to_ne_bytes());
            if std::net::IpAddr::V4(ip) == address {
                let name = unsafe { CStr::from_ptr(entry.ifa_name) }
                    .to_string_lossy()
                    .into_owned();
                let index = unsafe { libc::if_nametoindex(entry.ifa_name) };
                found = Some((name, index));
                break;
            }
        }
        cursor = entry.ifa_next;
    }
    unsafe { libc::freeifaddrs(head) };
    let (name, index) = found.expect("host source address has a real interface");
    EgressInterface::new(name, index).unwrap()
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn controlled_ancillary_hint_retains_specific_receive_address_and_original_fd() {
    let peer = host_peer().await;
    let target = peer.local_addr().unwrap();
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner
        .bind_listener(SocketAddr::new(target.ip(), 0), bound(1))
        .unwrap();
    let original = rx.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    control.replace(Some(actual_interface(target.ip())));
    let (mut receiver, mut sender) =
        PacketSocketReceiver::from_granted(GrantedDatagramListener { rx, tx }, control.clone());
    for bytes in [b"hint".as_slice(), b"hint-again"] {
        control.replace(Some(actual_interface(target.ip())));
        sender.send_to(bytes, target).await.unwrap();
        receive(&peer, bytes, original.port()).await;
        assert_eq!(receiver.local_addr().unwrap(), original);
        peer.send_to(b"reply", original).await.unwrap();
        let mut buffer = [0; 8];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap(),
            (5, target)
        );
        assert_eq!(&buffer[..5], b"reply");
    }
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
}

#[tokio::test]
async fn required_native_probe_is_unsupported_without_new_native_attempt() {
    // This exact case creates only the declared listener. It needs no native
    // peer/probe fixture. A serial syscall trace can check socket/bind/close
    // separately from the owner's own exact ledger below.
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner
        .bind_listener("127.0.0.1:0".parse().unwrap(), bound(1))
        .unwrap();
    let target: SocketAddr = "192.0.2.1:9".parse().unwrap();
    let control = EgressInterfaceControl::default();
    // Select synchronously before even waiting for native send readiness.
    assert_eq!(
        control
            .select_packet_without_native_probe(target)
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::Unsupported
    );
    let (_, mut sender) =
        PacketSocketReceiver::from_granted(GrantedDatagramListener { rx, tx }, control.clone());
    assert_eq!(
        sender.send_to(b"denied", target).await.unwrap_err().kind(),
        std_io::ErrorKind::Unsupported
    );
    control.replace(Some(EgressInterface::new("unprobed-physical", 1).unwrap()));
    control.replace_tunnel_addresses(["10.0.0.1".parse().unwrap()]);
    assert_eq!(
        sender
            .send_to(b"also-denied", target)
            .await
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::Unsupported
    );
    assert_eq!(owner.occupied(), 1);
    assert_eq!(
        owner.next, 1,
        "no additional original acquisition attempted"
    );
    assert!(!native.shared.socket.entry.retired.is_cancelled());
    assert_eq!(owner.take_report().confirmed_closed, 0);
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn required_mark_without_platform_guard_receipt_is_explicitly_unsupported() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner
        .bind_listener("127.0.0.1:0".parse().unwrap(), bound(1))
        .unwrap();
    let control = EgressInterfaceControl::default();
    control.replace(Some(
        EgressInterface::new("retained-firewall-identity", 1)
            .unwrap()
            .with_socket_mark(1)
            .unwrap(),
    ));
    let (_, mut sender) =
        PacketSocketReceiver::from_granted(GrantedDatagramListener { rx, tx }, control);
    assert_eq!(
        sender
            .send_to(b"no-guard-receipt", "192.0.2.1:9".parse().unwrap())
            .await
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::Unsupported
    );
    assert!(native.packet_failure().is_none());
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}

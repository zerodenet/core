use super::*;
use std::{io, num::NonZeroUsize, os::windows::io::AsRawSocket, time::Duration};
use windows_sys::Win32::Networking::WinSock::{
    setsockopt, WSAGetLastError, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, WSAEACCES, WSAEADDRINUSE,
};
use zero_platform_tokio::controlled_datagram::ControlledDatagramOwner;

fn exclusive_original(address: &str) -> std::net::UdpSocket {
    let address: SocketAddr = address.parse().unwrap();
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .unwrap();
    if address.is_ipv6() {
        socket.set_only_v6(true).unwrap();
    }
    let exclusive = 1i32;
    // SAFETY: this live socket and initialized i32 remain valid through the
    // synchronous Winsock call; capture its actual failure immediately.
    let result = unsafe {
        setsockopt(
            socket.as_raw_socket() as _,
            SOL_SOCKET,
            SO_EXCLUSIVEADDRUSE,
            (&exclusive as *const i32).cast(),
            std::mem::size_of_val(&exclusive) as i32,
        )
    };
    assert_eq!(
        result,
        0,
        "{}",
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    );
    socket.bind(&address.into()).unwrap();
    socket.into()
}

fn assert_native_exclusive_denial(held: &std::net::UdpSocket) {
    let address = held.local_addr().unwrap();
    let contender = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .unwrap();
    let error = contender.bind(&address.into()).unwrap_err();
    assert!(
        matches!(error.raw_os_error(), Some(WSAEACCES | WSAEADDRINUSE)),
        "the actual exclusive original must deny a second bind: {error}"
    );
    assert_eq!(held.local_addr().unwrap(), address);
}

fn pinned_policy(peer: SocketAddr) -> (DialPolicy, zero_platform_tokio::EgressSelection) {
    let control = EgressInterfaceControl::default();
    // Only an in-memory topology is installed, never a host route or adapter.
    control.replace_tunnel_addresses(["192.0.2.1".parse().unwrap()]);
    let mut policy = DialPolicy {
        address_family: if peer.is_ipv4() {
            AddressFamily::OnlyIpv4
        } else {
            AddressFamily::OnlyIpv6
        },
        ..source_policy(&peer.ip().to_string())
    };
    let source = control.select_for_peer_with_policy(peer, &policy).unwrap();
    policy.interface = Some(source.interface().unwrap().name().to_owned());
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    (policy, selected)
}

#[tokio::test]
async fn windows_exclusive_port_falls_back_and_preserves_source_family_and_interface() {
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let held = exclusive_original(address);
        assert_native_exclusive_denial(&held);
        let occupied = held.local_addr().unwrap();
        let receiver = UdpSocket::bind(address).await.unwrap();
        let peer = receiver.local_addr().unwrap();
        let (policy, selected) = pinned_policy(peer);
        let socket = TokioDatagramSocket::bind_for_peer_with_policy_preserving_port(
            peer,
            Some(occupied.port()),
            &policy,
            &selected,
        )
        .await
        .unwrap();
        assert_eq!(socket.local_addr().unwrap().ip(), occupied.ip());
        assert_ne!(socket.local_addr().unwrap().port(), occupied.port());
        assert_eq!(
            socket.egress_interface().unwrap(),
            selected.interface().unwrap()
        );
        socket.send_to_addr(b"heartbeat", peer).await.unwrap();
        let mut bytes = [0; 32];
        let (size, sender) =
            tokio::time::timeout(Duration::from_secs(3), receiver.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"heartbeat");
        assert_eq!(sender, socket.local_addr().unwrap());
        receiver.send_to(b"reply", sender).await.unwrap();
        let (size, from) =
            tokio::time::timeout(Duration::from_secs(3), socket.recv_from_addr(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"reply");
        assert_eq!(from, peer);
        if peer.is_ipv6() {
            assert!(socket
                .send_to_addr(b"wrong-family", "127.0.0.1:9".parse().unwrap())
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn windows_controlled_exclusive_fallback_retains_one_original_and_consumes_its_close_receipt()
{
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let held = exclusive_original(address);
        assert_native_exclusive_denial(&held);
        let occupied = held.local_addr().unwrap();
        let receiver = UdpSocket::bind(address).await.unwrap();
        let peer = receiver.local_addr().unwrap();
        let (policy, selected) = pinned_policy(peer);
        let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
        let socket = owner
            .acquire(peer, Some(occupied.port()), &policy, &selected)
            .unwrap();
        let control = owner.take_control(&socket).unwrap();
        assert_eq!(socket.local_addr().unwrap().ip(), occupied.ip());
        assert_ne!(socket.local_addr().unwrap().port(), occupied.port());
        assert_eq!(
            socket.egress_interface().unwrap(),
            selected.interface().unwrap()
        );
        assert_eq!(owner.occupied(), 1);
        socket.send_to_addr(b"heartbeat", peer).await.unwrap();
        let mut bytes = [0; 32];
        let (size, sender) =
            tokio::time::timeout(Duration::from_secs(3), receiver.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"heartbeat");
        assert_eq!(sender, socket.local_addr().unwrap());
        receiver.send_to(b"reply", sender).await.unwrap();
        let (size, from) =
            tokio::time::timeout(Duration::from_secs(3), socket.recv_from_addr(&mut bytes))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&bytes[..size], b"reply");
        assert_eq!(from, peer);
        let receipt = control.close().unwrap();
        assert!(receipt.matches(&control));
        assert!(control.observe().native_closed);
        assert!(socket.send_to_addr(b"retired", peer).await.is_err());
        assert_eq!(owner.occupied(), 0);
        let report = owner.take_report();
        assert!(report.is_complete());
        assert_eq!(report.confirmed_closed, 1);
        assert_eq!(report.known_no_native, 0);
        assert_eq!(held.local_addr().unwrap(), occupied);
    }
}

#[tokio::test]
async fn windows_wildcard_preferred_port_falls_back_when_loopback_port_is_occupied_by_default_socket(
) {
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let occupied = client.local_addr().unwrap();
    let echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = echo.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    let policy = DialPolicy::default();
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();

    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let outbound = owner
        .acquire(peer, Some(occupied.port()), &policy, &selected)
        .unwrap();
    let native_control = owner.take_control(&outbound).unwrap();
    assert_ne!(
        outbound.local_addr().unwrap().port(),
        occupied.port(),
        "wildcard preferred-port bind must not overlap an active loopback socket"
    );

    outbound.send_to_addr(b"probe", peer).await.unwrap();
    let mut buf = [0u8; 16];
    let (read, sender) = tokio::time::timeout(Duration::from_secs(3), echo.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..read], b"probe");
    echo.send_to(b"echoed", sender).await.unwrap();

    let (read, from) =
        tokio::time::timeout(Duration::from_secs(3), outbound.recv_from_addr(&mut buf))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&buf[..read], b"echoed");
    assert_eq!(from, peer);

    let receipt = native_control.close().unwrap();
    assert!(receipt.matches(&native_control));
}

#[tokio::test]
async fn windows_udp_sockets_ignore_icmp_port_unreachable_connreset_on_subsequent_receives() {
    let closed_peer = {
        let ephemeral = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        ephemeral.local_addr().unwrap()
    };
    let live_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let live_addr = live_peer.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    let policy = DialPolicy::default();
    let selected = control
        .select_for_peer_with_policy(live_addr, &policy)
        .unwrap();

    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let listener = owner.acquire(live_addr, None, &policy, &selected).unwrap();
    let native_control = owner.take_control(&listener).unwrap();
    let listener_addr = SocketAddr::new(
        "127.0.0.1".parse().unwrap(),
        listener.local_addr().unwrap().port(),
    );

    // Trigger loopback ICMP Port Unreachable (WSAECONNRESET without SIO_UDP_CONNRESET=FALSE).
    listener
        .send_to_addr(b"stale-ack", closed_peer)
        .await
        .unwrap();
    live_peer.send_to(b"healthy", listener_addr).await.unwrap();

    let mut buf = [0u8; 16];
    let (read, from) =
        tokio::time::timeout(Duration::from_secs(3), listener.recv_from_addr(&mut buf))
            .await
            .unwrap()
            .expect("ICMP Port Unreachable must not poison subsequent UDP receives");
    assert_eq!(&buf[..read], b"healthy");
    assert_eq!(from, live_addr);

    let receipt = native_control.close().unwrap();
    assert!(receipt.matches(&native_control));
}

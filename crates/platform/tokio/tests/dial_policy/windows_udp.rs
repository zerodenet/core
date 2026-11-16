use super::*;
use std::{io, num::NonZeroUsize, os::windows::io::AsRawSocket, time::Duration};
use windows_sys::Win32::Networking::WinSock::{
    setsockopt, WSAGetLastError, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
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
    assert_eq!(error.raw_os_error(), Some(10013));
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

use super::*;
#[tokio::test]
async fn tcp_and_udp_bind_real_ipv4_loopback_source() {
    let policy = source_policy("127.0.0.1");
    let control = EgressInterfaceControl::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    assert_eq!(
        selected.dial_source_address(),
        Some("127.0.0.1:0".parse().unwrap())
    );
    let client = TokioSocket::connect_addr_with_policy_observed(peer, &policy, &selected)
        .await
        .unwrap();
    let (_, accepted) = listener.accept().await.unwrap();
    assert_eq!(accepted.ip(), policy.source_ip.unwrap());
    assert_eq!(client.local_addr().unwrap().ip(), accepted.ip());
    let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = receiver.local_addr().unwrap();
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    let socket = TokioDatagramSocket::bind_for_peer_with_policy(peer, &policy, &selected)
        .await
        .unwrap();
    socket.send_to_addr(b"hello", peer).await.unwrap();
    let mut bytes = [0; 5];
    let (size, sender) = receiver.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"hello");
    assert_eq!(sender.ip(), policy.source_ip.unwrap());
    assert_eq!(socket.local_addr().unwrap(), sender);
}
#[tokio::test]
async fn mapped_ipv4_source_and_peer_use_native_ipv4_sockets() {
    let policy = source_policy("::ffff:127.0.0.1");
    let control = EgressInterfaceControl::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = SocketAddr::new("::ffff:127.0.0.1".parse().unwrap(), port);
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    let socket = TokioSocket::connect_addr_with_policy(peer, &policy, &selected)
        .await
        .unwrap();
    assert!(socket.local_addr().unwrap().is_ipv4());
    assert!(socket.peer_addr().unwrap().is_ipv4());
}
#[tokio::test]
async fn udp_port_collision_retains_source_constraint_on_ephemeral_retry() {
    let held = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let held_address = held.local_addr().unwrap();
    let policy = source_policy("127.0.0.1");
    let control = EgressInterfaceControl::default();
    let selected = control
        .select_for_peer_with_policy(held_address, &policy)
        .unwrap();
    let socket = TokioDatagramSocket::bind_for_peer_with_policy_preserving_port(
        held_address,
        Some(held_address.port()),
        &policy,
        &selected,
    )
    .await
    .unwrap();
    assert_eq!(socket.local_addr().unwrap().ip(), held_address.ip());
    assert_ne!(socket.local_addr().unwrap().port(), held_address.port());
}
#[tokio::test]
async fn ipv6_source_and_ipv6_only_udp_never_send_mapped_ipv4() {
    let Ok(receiver) = UdpSocket::bind("[::1]:0").await else {
        return;
    };
    let peer = receiver.local_addr().unwrap();
    let policy = source_policy("::1");
    let control = EgressInterfaceControl::default();
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    let socket = TokioDatagramSocket::bind_for_peer_with_policy(peer, &policy, &selected)
        .await
        .unwrap();
    socket.send_to_addr(b"6", peer).await.unwrap();
    let mut bytes = [0; 1];
    let (_, sender) = receiver.recv_from(&mut bytes).await.unwrap();
    assert_eq!(sender.ip(), "::1".parse::<IpAddr>().unwrap());
    let mapped = SocketAddr::new("::ffff:127.0.0.1".parse().unwrap(), peer.port());
    assert!(socket.send_to_addr(b"4", mapped).await.is_err());
}
#[tokio::test]
async fn wrong_selection_cannot_silently_drop_explicit_policy() {
    let control = EgressInterfaceControl::default();
    let peer = "127.0.0.1:80".parse().unwrap();
    let selection = control.select_for_peer(peer);
    let error = TokioSocket::connect_addr_with_policy_observed(
        peer,
        &source_policy("127.0.0.1"),
        &selection,
    )
    .await
    .unwrap_err();
    assert_eq!(error.stage(), "validate_dial_policy");
    assert_eq!(error.error().kind(), std::io::ErrorKind::InvalidInput);
    assert!(error
        .error()
        .to_string()
        .contains("was not prepared for this dial policy"));
}
#[tokio::test]
async fn tcp_binds_real_ipv6_loopback_source_when_available() {
    let Ok(listener) = TcpListener::bind("[::1]:0").await else {
        return;
    };
    let peer = listener.local_addr().unwrap();
    let policy = source_policy("::1");
    let control = EgressInterfaceControl::default();
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    let socket = TokioSocket::connect_addr_with_policy(peer, &policy, &selected)
        .await
        .unwrap();
    let (_, accepted) = listener.accept().await.unwrap();
    assert_eq!(accepted.ip(), policy.source_ip.unwrap());
    assert_eq!(socket.local_addr().unwrap().ip(), accepted.ip());
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn explicit_loopback_binding_is_applied_or_fails_without_unbound_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let policy = loopback_policy();
    let control = EgressInterfaceControl::default();
    let selected = control.select_for_peer_with_policy(peer, &policy).unwrap();
    match TokioSocket::connect_addr_with_policy_observed(peer, &policy, &selected).await {
        Ok(socket) => {
            assert_eq!(socket.egress_interface(), selected.interface());
            #[cfg(target_os = "linux")]
            {
                use std::os::fd::AsRawFd;
                let inner = socket.into_inner();
                let mut name = [0u8; libc::IF_NAMESIZE];
                let mut length = name.len() as libc::socklen_t;
                // SAFETY: descriptor is live and buffer has the supplied capacity.
                let result = unsafe {
                    libc::getsockopt(
                        inner.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_BINDTODEVICE,
                        name.as_mut_ptr().cast(),
                        &mut length,
                    )
                };
                assert_eq!(result, 0);
                let length = name
                    .iter()
                    .position(|byte| *byte == 0)
                    .unwrap_or(name.len());
                assert_eq!(&name[..length], b"lo");
            }
        }
        Err(error) => {
            assert_eq!(error.stage(), "bind_interface");
            assert_eq!(error.error().kind(), std::io::ErrorKind::PermissionDenied);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(30), listener.accept())
                    .await
                    .is_err()
            );
        }
    }
}

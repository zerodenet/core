use super::*;

#[test]
fn preferred_port_conflicts_allow_fallback_but_exact_and_ephemeral_binds_do_not() {
    let collision = io::Error::from(io::ErrorKind::AddrInUse);
    assert!(can_retry_port(Some(32123), &collision));
    assert!(!can_retry_port(None, &collision));
    assert!(!can_retry_port(Some(0), &collision));
    let windows_denied = io::Error::from_raw_os_error(10013);
    assert_eq!(can_retry_port(Some(32123), &windows_denied), cfg!(windows));
    assert!(!can_retry_port(None, &windows_denied));
    assert!(!can_retry_port(Some(0), &windows_denied));
}

#[test]
fn windows_port_conflict_requires_the_original_wsaeacces_code() {
    // Classification only: these values do not simulate native bind success.
    assert!(windows_port_conflict(&io::Error::from_raw_os_error(10013)));
    for code in [13, 10022, 10049, 10055] {
        assert!(!windows_port_conflict(&io::Error::from_raw_os_error(code)));
    }
    assert!(!windows_port_conflict(&io::Error::from(
        io::ErrorKind::PermissionDenied
    )));
}

#[test]
fn non_port_bind_errors_keep_their_failure_without_relaxing_source_constraints() {
    for kind in [
        io::ErrorKind::PermissionDenied,
        io::ErrorKind::AddrNotAvailable,
        io::ErrorKind::InvalidInput,
        io::ErrorKind::OutOfMemory,
    ] {
        assert!(!can_retry_port(Some(32123), &io::Error::from(kind)));
    }
}

#[test]
fn source_port_collision_rebinds_the_same_native_socket_and_keeps_its_ipv4_or_ipv6_address() {
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let held = std::net::UdpSocket::bind(address).unwrap();
        let source = held.local_addr().unwrap();
        let socket = socket2::Socket::new(
            socket2::Domain::for_address(source),
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .unwrap();
        if source.is_ipv6() {
            socket.set_only_v6(true).unwrap();
        }
        let requested = bind_source(
            &socket2::SockRef::from(&socket),
            source,
            Some(source.port()),
        )
        .unwrap();
        assert_eq!(requested.ip(), source.ip());
        assert_eq!(requested.port(), 0);
        let actual = socket.local_addr().unwrap().as_socket().unwrap();
        assert_eq!(actual.ip(), source.ip());
        assert_ne!(actual.port(), 0);
        assert_ne!(actual.port(), source.port());
        if source.is_ipv6() {
            assert!(socket.only_v6().unwrap());
        }
        // The held original still owns the preferred port throughout fallback.
        assert_eq!(held.local_addr().unwrap(), source);
    }
}

#[test]
fn exact_bind_preserves_the_original_native_collision_error_and_leaves_socket_unbound() {
    let held = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let source = held.local_addr().unwrap();
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .unwrap();
    let error = bind_source(&socket2::SockRef::from(&socket), source, None).unwrap_err();
    assert!(error.raw_os_error().is_some());
    assert!(error.get_ref().is_none());
    assert_eq!(socket.local_addr().unwrap().as_socket().unwrap().port(), 0);
    assert_eq!(held.local_addr().unwrap(), source);
}

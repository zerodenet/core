use super::*;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn repeated_probe_reuses_storage_without_retaining_source_or_peer() {
    let mut probes = ProbeSockets::default();
    let peer = "127.0.0.1:9".parse().unwrap();
    assert_eq!(probes.source_for(peer).unwrap(), peer.ip());
    let fd = probes.ipv4.as_ref().unwrap().as_raw_fd();
    for port in 10..42 {
        assert_eq!(
            probes.source_for(SocketAddr::new(peer.ip(), port)).unwrap(),
            peer.ip()
        );
        let socket = probes.ipv4.as_ref().unwrap();
        assert_eq!(socket.as_raw_fd(), fd);
        assert!(socket.local_addr().unwrap().ip().is_unspecified());
        assert_eq!(
            socket.peer_addr().unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    }
    assert!(probes.ipv6.is_none());
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn alternating_destinations_match_fresh_probes_without_a_generation_change() {
    let mut probes = ProbeSockets::default();
    for _ in 0..12 {
        for peer in ["127.0.0.1:9", "192.0.2.1:9", "127.0.0.2:19"] {
            let peer = peer.parse().unwrap();
            assert_eq!(
                probes.source_for(peer).unwrap(),
                fresh_source(peer).unwrap()
            );
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn families_have_independent_probes_and_bad_scope_does_not_poison_next_query() {
    let mut probes = ProbeSockets::default();
    let ipv4 = "127.0.0.1:9".parse().unwrap();
    let ipv6 = "[::1]:9".parse().unwrap();
    assert_eq!(probes.source_for(ipv4).unwrap(), ipv4.ip());
    assert_eq!(probes.source_for(ipv6).unwrap(), ipv6.ip());
    let ipv4_fd = probes.ipv4.as_ref().unwrap().as_raw_fd();
    let ipv6_fd = probes.ipv6.as_ref().unwrap().as_raw_fd();
    assert_ne!(ipv4_fd, ipv6_fd);
    let invalid = SocketAddr::V6(std::net::SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        9,
        0,
        u32::MAX,
    ));
    assert!(probes.source_for(invalid).is_err());
    assert!(probes.ipv6.is_none());
    assert_eq!(probes.ipv4.as_ref().unwrap().as_raw_fd(), ipv4_fd);
    assert_eq!(probes.source_for(ipv6).unwrap(), ipv6.ip());
    let socket = probes.ipv6.as_ref().unwrap();
    assert!(socket.local_addr().unwrap().ip().is_unspecified());
    assert_eq!(
        socket.peer_addr().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn reset_failure_keeps_current_query_but_discards_descriptor_for_next_call() {
    let mut probes = ProbeSockets::default();
    let peer = "127.0.0.1:9".parse().unwrap();
    let result = probes.source_with_reset(peer, |_, _| Err(io::ErrorKind::Unsupported.into()));
    assert_eq!(result.unwrap(), peer.ip());
    assert!(probes.ipv4.is_none());
    assert_eq!(probes.source_for(peer).unwrap(), peer.ip());
    assert!(probes.ipv4.is_some());
}

#[test]
fn route_probe_never_emits_datagrams() {
    let target = UdpSocket::bind("127.0.0.1:0").unwrap();
    target.set_nonblocking(true).unwrap();
    for _ in 0..8 {
        assert_eq!(
            route_source_for(target.local_addr().unwrap()).unwrap(),
            target.local_addr().unwrap().ip()
        );
    }
    assert_eq!(
        target.recv_from(&mut [0; 16]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn mapped_ipv4_probe_keeps_fresh_socket_semantics() {
    let peer = "[::ffff:127.0.0.1]:9".parse().unwrap();
    // Some platforms use an IPv6-only wildcard socket by default. Preserve
    // that platform's original success/error semantics for mapped addresses.
    match (route_source_for(peer), fresh_source(peer)) {
        (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
        (Err(actual), Err(expected)) => assert_eq!(actual.kind(), expected.kind()),
        result => panic!("mapped probe changed one-shot semantics: {result:?}"),
    }
    assert_eq!(
        route_source_for("[::1]:9".parse().unwrap()).unwrap(),
        "::1".parse::<IpAddr>().unwrap()
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
#[ignore = "bounded route-only comparison; timing is evidence, not a CI threshold"]
fn compare_fresh_and_reused_route_probe_cost() {
    let peers = [
        "192.0.2.1:9".parse().unwrap(),
        "127.0.0.1:9".parse().unwrap(),
    ];
    let iterations = 4000;
    let mut probes = ProbeSockets::default();
    for peer in peers {
        probes.source_for(peer).unwrap();
        fresh_source(peer).unwrap();
    }
    let mut fresh = Vec::new();
    let mut reused = Vec::new();
    for round in 0..5 {
        // Alternate order to avoid giving either path the first warm interval.
        for mode in if round % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let start = std::time::Instant::now();
            for index in 0..iterations {
                let source = if mode {
                    probes.source_for(peers[index % 2])
                } else {
                    fresh_source(peers[index % 2])
                }
                .unwrap();
                std::hint::black_box(source);
            }
            let ns = start.elapsed().as_nanos() / iterations as u128;
            if mode {
                reused.push(ns);
            } else {
                fresh.push(ns);
            }
        }
    }
    fresh.sort_unstable();
    reused.sort_unstable();
    eprintln!("route_probe_compare iterations={iterations} rounds=5 fresh_ns={fresh:?} reused_ns={reused:?} median_reduction_percent={:.2}",
        100.0 * (1.0 - reused[2] as f64 / fresh[2] as f64));
}

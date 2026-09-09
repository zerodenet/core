use std::net::SocketAddr;

use zero_core::Address;

use super::{collect_family_bind_outcomes, select_stable_udp_target, DirectUdpSocketBinding};

#[test]
fn udp_target_selection_is_stable_across_same_family_answer_reordering() {
    let target = Address::Domain("Example.COM".to_owned());
    let first: Vec<SocketAddr> = [
        "192.0.2.10:443",
        "192.0.2.11:443",
        "192.0.2.12:443",
        "[2001:db8::10]:443",
    ]
    .into_iter()
    .map(|value| value.parse().unwrap())
    .collect();
    let reordered = vec![first[2], first[0], first[1], first[3]];

    let selected = select_stable_udp_target(&target, &first, true, true).unwrap();
    assert!(selected.is_ipv4());
    assert_eq!(
        select_stable_udp_target(&target, &reordered, true, true),
        Some(selected)
    );
}

#[test]
fn udp_target_selection_falls_back_to_an_available_family() {
    let target = Address::Domain("single-family.example".to_owned());
    let ipv4 = "192.0.2.10:443".parse().unwrap();
    let ipv6 = "[2001:db8::10]:443".parse().unwrap();
    let candidates = [ipv4, ipv6];

    assert_eq!(
        select_stable_udp_target(&target, &candidates, false, true),
        Some(ipv6)
    );
    assert_eq!(
        select_stable_udp_target(&target, &candidates, true, false),
        Some(ipv4)
    );
    assert_eq!(
        select_stable_udp_target(&target, &candidates, false, false),
        None
    );
}

#[test]
fn direct_udp_family_binding_accepts_either_family_and_retains_both_failures() {
    let ipv6_only = collect_family_bind_outcomes::<_, &str>(Err("IPv4 unavailable"), Ok(6));
    assert_eq!(ipv6_only.available, vec![(true, 6)]);
    assert_eq!(ipv6_only.failures, vec![(false, "IPv4 unavailable")]);

    let ipv4_only = collect_family_bind_outcomes::<_, &str>(Ok(4), Err("IPv6 unavailable"));
    assert_eq!(ipv4_only.available, vec![(false, 4)]);
    assert_eq!(ipv4_only.failures, vec![(true, "IPv6 unavailable")]);

    let unavailable =
        collect_family_bind_outcomes::<u8, _>(Err("IPv4 unavailable"), Err("IPv6 unavailable"));
    assert!(unavailable.available.is_empty());
    assert_eq!(
        unavailable.failures,
        vec![(false, "IPv4 unavailable"), (true, "IPv6 unavailable")]
    );
}

#[tokio::test]
async fn direct_udp_socket_set_builds_with_ipv6_only_and_reports_dual_failure() {
    let sockets = super::DirectUdpSockets::bind_with(7, None, |peer, _| async move {
        if peer.is_ipv4() {
            Err(zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "IPv4 egress unavailable",
            )))
        } else {
            zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(peer, None)
                .await
                .map_err(zero_engine::EngineError::Io)
        }
    })
    .await
    .expect("an IPv6 socket is sufficient for a direct UDP socket set");

    assert_eq!(sockets.generation(), 7);
    assert_eq!(sockets.sockets.len(), 1);
    assert!(sockets.sockets[0].binding.ipv6);

    let error = match super::DirectUdpSockets::bind_with(8, None, |peer, _| async move {
        let family = if peer.is_ipv6() { "IPv6" } else { "IPv4" };
        Err(zero_engine::EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!("{family} egress unavailable"),
        )))
    })
    .await
    {
        Ok(_) => panic!("both unavailable families must fail deterministically"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("IPv4 egress unavailable"), "{message}");
    assert!(message.contains("IPv6 egress unavailable"), "{message}");
}

#[test]
fn direct_udp_binding_is_scoped_by_family_and_egress() {
    let physical = zero_platform_tokio::EgressInterface::new("physical0", 7).unwrap();
    let physical_v4 = DirectUdpSocketBinding {
        ipv6: false,
        egress: Some(physical.clone()),
    };
    let system_route_v4 = DirectUdpSocketBinding {
        ipv6: false,
        egress: None,
    };
    let physical_v6 = DirectUdpSocketBinding {
        ipv6: true,
        egress: Some(physical),
    };

    assert_ne!(physical_v4, system_route_v4);
    assert_ne!(physical_v4, physical_v6);
}

#[tokio::test]
async fn scoped_direct_replies_retain_session_identity_and_retirement_closes_only_its_socket() {
    use super::{DirectUdpSocket, DirectUdpSockets};
    let mut sockets = DirectUdpSockets::bind_with(1, None, |peer, _| async move {
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(peer, None)
            .await
            .map_err(zero_engine::EngineError::Io)
    })
    .await
    .unwrap();
    let shared_count = sockets.sockets.len();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut local = Vec::new();
    for id in [41, 42] {
        sockets.isolate_session(id);
        let socket = zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(
            peer.local_addr().unwrap(),
            None,
        )
        .await
        .unwrap();
        local.push(socket.local_addr().unwrap().port());
        let mut entry = DirectUdpSocket::new(socket, false);
        entry.session_id = Some(id);
        sockets.sockets.push(entry);
    }
    let mut buf = [0; 16];
    for (index, id) in [(1, 42), (0, 41)] {
        peer.send_to(&[id as u8], (std::net::Ipv4Addr::LOCALHOST, local[index]))
            .await
            .unwrap();
        let (len, source) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sockets.recv_from_addr(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(source.session_id, Some(id));
        assert_eq!(source.sender, peer.local_addr().unwrap());
        assert_eq!(&buf[..len], &[id as u8]);
    }
    sockets.retire_session(41);
    assert_eq!(sockets.sockets.len(), shared_count + 1);
    assert!(!sockets.isolated_sessions.contains(&41));
    assert!(sockets.isolated_sessions.contains(&42));
    peer.send_to(b"alive", (std::net::Ipv4Addr::LOCALHOST, local[1]))
        .await
        .unwrap();
    let (len, source) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sockets.recv_from_addr(&mut buf),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(source.session_id, Some(42));
    assert_eq!(&buf[..len], b"alive");
    sockets.retire_session(42);
    assert_eq!(sockets.sockets.len(), shared_count);
}

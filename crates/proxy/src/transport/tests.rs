use zero_core::{Address, Network, ProtocolType, Session, TargetHostSource};

use super::{
    direct::candidates::{append_unique_resolved_candidates, MAX_RECOVERED_DIRECT_CANDIDATES},
    direct_dial::{
        dial_tcp_candidates, interleave_address_families, MAX_RECORDED_CONNECTION_ATTEMPTS,
    },
    DirectConnector,
};

fn tls_sni_session(original: Address) -> Session {
    let mut session = Session::new(
        1,
        Address::Domain("exmail.qq.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    session.original_target = Some(original.clone());
    session.direct_target = Some(original);
    session.target_host_source = Some(TargetHostSource::TlsSni);
    session.sni = Some("exmail.qq.com".to_owned());
    session
}

#[test]
fn direct_tls_sni_keeps_the_original_ipv6_destination() {
    let original = Address::Ipv6([
        0x24, 0x0e, 0x09, 0x7c, 0x00, 0x2f, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0x5f,
    ]);
    let session = tls_sni_session(original.clone());

    assert_eq!(session.effective_direct_target(), &original);
}

#[test]
fn direct_fake_ip_resolves_the_recovered_domain() {
    let mut session = tls_sni_session(Address::Ipv4([198, 18, 0, 1]));
    session.target_host_source = Some(TargetHostSource::FakeIp);
    session.direct_target = None;

    assert_eq!(session.effective_direct_target(), &session.target);
}

#[test]
fn direct_url_rewrite_resolves_the_rewritten_domain() {
    let mut session = tls_sni_session(Address::Ipv4([183, 2, 144, 108]));
    session.target = Address::Domain("rewritten.example".to_owned());
    session.direct_target = None;

    assert_eq!(session.effective_direct_target(), &session.target);
}

#[test]
fn tcp_dial_candidates_interleave_families_and_remove_duplicates() {
    let v4_a = "192.0.2.1:443".parse().unwrap();
    let v4_b = "192.0.2.2:443".parse().unwrap();
    let v6_a = "[2001:db8::1]:443".parse().unwrap();
    let v6_b = "[2001:db8::2]:443".parse().unwrap();

    assert_eq!(
        interleave_address_families(vec![v4_a, v4_b, v4_a, v6_a, v6_b, v6_a]),
        vec![v4_a, v6_a, v4_b, v6_b]
    );
    assert_eq!(
        interleave_address_families(vec![v6_a, v6_b, v4_a, v4_b]),
        vec![v6_a, v4_a, v6_b, v4_b]
    );
}

#[test]
fn recovered_candidates_preserve_order_and_remove_dns_duplicates() {
    let original = "192.0.2.1:443".parse().unwrap();
    let mut candidates = vec![original];

    append_unique_resolved_candidates(
        &mut candidates,
        vec![
            zero_traits::IpAddress::V4([192, 0, 2, 1]),
            zero_traits::IpAddress::V4([192, 0, 2, 2]),
            zero_traits::IpAddress::V4([192, 0, 2, 2]),
            zero_traits::IpAddress::V6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        ],
        443,
    );

    assert_eq!(
        candidates,
        vec![
            original,
            "192.0.2.2:443".parse().unwrap(),
            "[::1]:443".parse().unwrap()
        ]
    );
}

#[test]
fn recovered_candidate_enrichment_is_bounded() {
    let original = "192.0.2.1:443".parse().unwrap();
    let mut candidates = vec![original];
    let resolved = (2..=32)
        .map(|last| zero_traits::IpAddress::V4([192, 0, 2, last]))
        .collect();

    append_unique_resolved_candidates(&mut candidates, resolved, 443);

    assert_eq!(candidates.len(), MAX_RECOVERED_DIRECT_CANDIDATES);
    assert_eq!(candidates[0], original);
    assert_eq!(candidates[7], "192.0.2.8:443".parse().unwrap());
}

#[tokio::test]
async fn tcp_dial_candidates_fall_back_after_the_first_address_fails() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reachable = listener.local_addr().unwrap();
    let unavailable = std::net::SocketAddr::new("2001:db8::1".parse().unwrap(), reachable.port());
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);

    let connection = dial_tcp_candidates(vec![unavailable, reachable], &egress)
        .await
        .unwrap();

    assert_eq!(connection.remote, reachable);
    assert_eq!(connection.resolved_candidates, vec![unavailable, reachable]);
    assert_eq!(connection.socket.peer_addr().unwrap(), reachable);
    assert_eq!(connection.attempts.len(), 2);
    assert_eq!(connection.attempts[0].remote, unavailable);
    assert_eq!(connection.attempts[0].outcome, "failed");
    assert_eq!(connection.attempts[0].stage, "select_egress");
    assert!(connection.attempts[0].error_kind.is_some());
    assert!(connection.attempts[0].os_error.is_none());
    assert!(connection.attempts[0].error.is_some());
    assert_eq!(connection.attempts[1].remote, reachable);
    assert_eq!(connection.attempts[1].outcome, "connected");
    assert_eq!(connection.attempts[1].stage, "connected");
    assert!(connection.attempts[1].error_kind.is_none());
    assert!(connection.attempts[1].os_error.is_none());
    assert!(connection.attempts[1].error.is_none());
}

#[tokio::test]
async fn tcp_dial_failure_records_the_platform_socket_error() {
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = reservation.local_addr().unwrap();
    drop(reservation);

    let failure = dial_tcp_candidates(
        vec![unavailable],
        &zero_platform_tokio::EgressInterfaceControl::default(),
    )
    .await
    .expect_err("closed loopback port must fail");

    assert_eq!(failure.attempts.len(), 1);
    let attempt = &failure.attempts[0];
    assert_eq!(attempt.remote, unavailable);
    assert_eq!(attempt.stage, "connect_socket");
    assert_eq!(attempt.outcome, "failed");
    assert!(!attempt.interface_bound);
    assert!(attempt.error_kind.is_some());
    assert!(attempt.os_error.is_some());
    assert!(attempt
        .error
        .as_deref()
        .is_some_and(|error| !error.is_empty()));
}

#[tokio::test]
async fn tcp_dial_failure_attempt_observations_are_ordered_and_bounded() {
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let candidates = (0..(MAX_RECORDED_CONNECTION_ATTEMPTS + 4))
        .map(|index| {
            std::net::SocketAddr::new(
                "2001:db8::1".parse().unwrap(),
                40_000 + u16::try_from(index).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let failure = dial_tcp_candidates(candidates.clone(), &egress)
        .await
        .expect_err("unavailable TUN IPv6 egress must fail");

    assert_eq!(failure.attempts.len(), MAX_RECORDED_CONNECTION_ATTEMPTS);
    assert_eq!(failure.attempts[0].remote, candidates[0]);
    assert_eq!(
        failure.attempts[MAX_RECORDED_CONNECTION_ATTEMPTS - 2].remote,
        candidates[MAX_RECORDED_CONNECTION_ATTEMPTS - 2]
    );
    assert_eq!(failure.attempts.last().unwrap().remote, candidates[19]);
    assert!(failure
        .attempts
        .windows(2)
        .all(|attempts| attempts[0].remote.port() < attempts[1].remote.port()));
    assert!(failure.attempts.iter().all(|attempt| {
        attempt.stage == "select_egress"
            && attempt.outcome == "failed"
            && attempt.error_kind.is_some()
            && attempt.os_error.is_none()
    }));
}

#[tokio::test]
async fn recovered_ipv4_target_uses_trusted_dns_candidates_after_original_fails() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reachable = listener.local_addr().unwrap();
    let original = Address::Ipv4([127, 0, 0, 2]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("localhost".to_owned());
    session.sni = Some("localhost".to_owned());
    session.port = reachable.port();
    let resolver = zero_dns::DnsSystem::build(None).unwrap();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();

    let connection = DirectConnector
        .connect(&session, &resolver, &egress)
        .await
        .unwrap_or_else(|failure| {
            panic!("trusted DNS candidate should connect: {}", failure.error)
        });

    let captured = std::net::SocketAddr::new("127.0.0.2".parse().unwrap(), reachable.port());
    assert_eq!(connection.network.resolved_candidates[0].host, "127.0.0.2");
    assert_eq!(connection.remote, reachable);
    assert!(connection
        .network
        .resolved_candidates
        .iter()
        .any(|candidate| candidate.host == "127.0.0.1"));
    assert_eq!(
        connection
            .network
            .resolved_candidates
            .iter()
            .filter(|candidate| candidate.host == captured.ip().to_string())
            .count(),
        1
    );
}

#[tokio::test]
async fn recovered_ipv4_dns_failure_retains_the_original_candidate() {
    let original = Address::Ipv4([192, 0, 2, 10]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("invalid\0domain".to_owned());
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(
            &session,
            &resolver,
            &zero_platform_tokio::EgressInterfaceControl::default(),
        )
        .await
        .expect("literal target remains valid when DNS refresh fails");

    assert_eq!(
        resolution.candidates,
        vec!["192.0.2.10:443".parse().unwrap()]
    );
    assert_eq!(resolution.udp_candidates(), resolution.candidates);
}

#[tokio::test]
async fn recovered_ipv4_without_host_source_remains_literal_only() {
    let original = Address::Ipv4([127, 0, 0, 2]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("localhost".to_owned());
    session.target_host_source = None;
    session.sni = None;
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(
            &session,
            &resolver,
            &zero_platform_tokio::EgressInterfaceControl::default(),
        )
        .await
        .expect("untrusted recovered host must not replace literal semantics");

    assert_eq!(
        resolution.candidates,
        vec!["127.0.0.2:443".parse().unwrap()]
    );
}

#[tokio::test]
async fn recovered_ipv4_udp_remains_pinned_to_the_original_candidate() {
    let original = Address::Ipv4([127, 0, 0, 2]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("localhost".to_owned());
    session.network = Network::Udp;
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(
            &session,
            &resolver,
            &zero_platform_tokio::EgressInterfaceControl::default(),
        )
        .await
        .expect("UDP should retain the usable literal target");

    assert_eq!(
        resolution.candidates,
        vec!["127.0.0.2:443".parse().unwrap()]
    );
    assert_eq!(
        resolution.udp_candidates(),
        &["127.0.0.2:443".parse().unwrap()]
    );
}

#[tokio::test]
async fn unavailable_tun_ipv6_egress_reresolves_a_trusted_domain_to_ipv4() {
    let original = Address::Ipv6([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("localhost".to_owned());
    session.sni = Some("localhost".to_owned());

    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(&session, &resolver, &egress)
        .await
        .expect("trusted domain should provide IPv4 fallback candidates");

    assert!(!resolution.candidates.is_empty());
    assert!(resolution
        .candidates
        .iter()
        .all(|candidate| candidate.is_ipv4()));
    let network =
        DirectConnector.udp_network_observation(&resolution, resolution.candidates[0], &egress);
    assert_eq!(
        network.address_family_policy.as_deref(),
        Some("prefer_ipv4")
    );
    let fallback = network
        .address_family_fallback
        .expect("fallback decision should be observable");
    assert_eq!(fallback.from, "ipv6");
    assert_eq!(fallback.to, "ipv4");
    assert_eq!(fallback.reason, "tun_ipv6_egress_unavailable");
    assert_eq!(fallback.trigger_egress_generation, egress.generation());
    assert_eq!(
        fallback.unavailable_reason.as_deref(),
        Some("no physical IPv6 default route")
    );
}

#[tokio::test]
async fn unreachable_native_ipv6_candidate_races_one_trusted_ipv4_fallback() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reachable = listener.local_addr().unwrap();
    // Loopback has an authoritative IPv6 route on every supported platform,
    // while this listener is deliberately IPv4-only. That distinguishes an
    // actual IPv6 connect failure from a missing-family egress without relying
    // on the host having a native IPv6 default route.
    let original = Address::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("localhost".to_owned());
    session.sni = Some("localhost".to_owned());
    session.port = reachable.port();

    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.replace_for(
        true,
        Some(zero_platform_tokio::EgressInterface::new("loopback", 1).unwrap()),
    );
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let started = std::time::Instant::now();
    let connection = match DirectConnector.connect(&session, &resolver, &egress).await {
        Ok(connection) => connection,
        Err(failure) => panic!(
            "trusted IPv4 candidate should win after native IPv6 fails: {}",
            failure.error
        ),
    };

    assert!(connection.remote.is_ipv4());
    assert_eq!(connection.remote.port(), reachable.port());
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    assert_eq!(egress.ipv6_to_ipv4_fallbacks(), 1);
    let fallback = connection
        .network
        .address_family_fallback
        .expect("connectivity fallback should be observable");
    assert_eq!(fallback.reason, "ipv6_connect_failed");
}

#[tokio::test]
async fn unavailable_tun_ipv6_egress_applies_to_recovered_fake_ip_domains() {
    let original = Address::Ipv6([0xfd, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("localhost".to_owned());
    session.target_host_source = Some(TargetHostSource::FakeIp);
    session.direct_target = None;

    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(&session, &resolver, &egress)
        .await
        .expect("Fake-IP domain should provide IPv4 fallback candidates");

    assert!(!resolution.candidates.is_empty());
    assert!(resolution
        .candidates
        .iter()
        .all(|candidate| candidate.is_ipv4()));
    assert!(DirectConnector
        .udp_network_observation(&resolution, resolution.candidates[0], &egress)
        .address_family_fallback
        .is_some());
}

#[tokio::test]
async fn failed_ipv4_reresolution_keeps_the_fallback_diagnostics() {
    let original = Address::Ipv6([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut session = tls_sni_session(original);
    session.target = Address::Domain("invalid\0domain".to_owned());
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let failure = match DirectConnector.connect(&session, &resolver, &egress).await {
        Ok(_) => panic!("invalid fallback domain must not connect"),
        Err(failure) => failure,
    };

    assert_eq!(failure.stage, "resolve_direct_target");
    assert_eq!(
        failure.network.connect_stage.as_deref(),
        Some("resolve_target")
    );
    assert_eq!(
        failure.network.address_family_policy.as_deref(),
        Some("prefer_ipv4")
    );
    assert_eq!(
        failure
            .network
            .address_family_fallback
            .as_ref()
            .map(|fallback| fallback.reason.as_str()),
        Some("tun_ipv6_egress_unavailable")
    );
}

#[tokio::test]
async fn direct_ipv6_is_preserved_without_an_active_tun_capture() {
    let original = Address::Ipv6([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let session = tls_sni_session(original.clone());
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(
            &session,
            &resolver,
            &zero_platform_tokio::EgressInterfaceControl::default(),
        )
        .await
        .expect("non-TUN direct target remains connectable through system routing");

    assert_eq!(
        resolution.candidates,
        vec!["[2001:db8::1]:443".parse().unwrap()]
    );
}

#[tokio::test]
async fn literal_ipv6_without_a_recovered_domain_is_not_converted() {
    let original = Address::Ipv6([0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let session = Session::new(1, original, 443, Network::Tcp, ProtocolType::UNKNOWN);
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let resolver = zero_dns::DnsSystem::build(None).unwrap();

    let resolution = DirectConnector
        .resolve_target_addrs(&session, &resolver, &egress)
        .await
        .expect("literal targets are preserved for an explicit egress failure");

    assert_eq!(
        resolution.candidates,
        vec!["[2001:db8::1]:443".parse().unwrap()]
    );

    let failure = DirectConnector
        .connect(&session, &resolver, &egress)
        .await
        .err()
        .expect("unavailable literal IPv6 target must fail");
    assert_eq!(
        failure.error,
        zero_core::Error::Io("tun_ipv6_egress_unavailable")
    );
    assert_eq!(
        failure.network.connect_stage.as_deref(),
        Some("select_egress")
    );
    assert!(failure.network.address_family_fallback.is_none());
}

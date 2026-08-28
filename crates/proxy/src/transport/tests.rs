use zero_core::{Address, Network, ProtocolType, Session, TargetHostSource};

use super::{
    direct_dial::{dial_tcp_candidates, interleave_address_families},
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

#[tokio::test]
async fn tcp_dial_candidates_fall_back_after_the_first_address_fails() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reachable = listener.local_addr().unwrap();
    let unavailable = std::net::SocketAddr::new("127.0.0.2".parse().unwrap(), reachable.port());

    let connection = dial_tcp_candidates(
        vec![unavailable, reachable],
        &zero_platform_tokio::EgressInterfaceControl::default(),
    )
    .await
    .unwrap();

    assert_eq!(connection.remote, reachable);
    assert_eq!(connection.resolved_candidates, vec![unavailable, reachable]);
    assert_eq!(connection.socket.peer_addr().unwrap(), reachable);
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
}

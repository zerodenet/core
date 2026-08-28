use zero_core::{Address, Network, ProtocolType, Session, TargetHostSource};

use super::direct_dial::{dial_tcp_candidates, interleave_address_families};

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

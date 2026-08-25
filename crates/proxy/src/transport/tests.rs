use zero_core::{Address, Network, ProtocolType, Session, TargetHostSource};

use super::direct::direct_socket_target;

fn tls_sni_session(original: Address) -> Session {
    let mut session = Session::new(
        1,
        Address::Domain("exmail.qq.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    session.original_target = Some(original);
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

    assert_eq!(direct_socket_target(&session), &original);
}

#[test]
fn direct_fake_ip_resolves_the_recovered_domain() {
    let mut session = tls_sni_session(Address::Ipv4([198, 18, 0, 1]));
    session.target_host_source = Some(TargetHostSource::FakeIp);

    assert_eq!(direct_socket_target(&session), &session.target);
}

#[test]
fn direct_url_rewrite_resolves_the_rewritten_domain() {
    let mut session = tls_sni_session(Address::Ipv4([183, 2, 144, 108]));
    session.target = Address::Domain("rewritten.example".to_owned());

    assert_eq!(direct_socket_target(&session), &session.target);
}

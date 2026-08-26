use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::RouteDecision;

use super::TcpIngressRuntime;

#[tokio::test]
async fn unmatched_domain_is_rechecked_against_resolved_ip_rules() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "ip",
                            "values": ["127.0.0.0/8", "::1/128"]
                        },
                        "action": { "type": "direct" }
                    }
                ],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse routing config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let runtime = TcpIngressRuntime::new(
        proxy.tcp_runtime_services(),
        "test-inbound".to_owned(),
        None,
    );
    let session = Session::new(
        1,
        Address::Domain("localhost".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );

    assert_eq!(
        runtime.route_decision(&session).await,
        RouteDecision::Direct
    );
}

#[tokio::test]
async fn fake_ip_restoration_records_success_and_missing_mapping() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "system": { "type": "system" } },
                    "default_server": "system",
                    "answer": {
                        "type": "fake_ip",
                        "cidr": "198.18.0.0/15",
                        "ttl_seconds": 60,
                        "max_entries": 16
                    }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse Fake-IP config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let runtime = TcpIngressRuntime::new(proxy.tcp_runtime_services(), "tun-test".to_owned(), None);
    runtime
        .services
        .resolver()
        .answer_udp_query(&dns_a_query("mapped.example"))
        .await
        .expect("allocate mapping");

    let mut mapped = Session::new(
        1,
        Address::Ipv4([198, 18, 0, 1]),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    runtime.resolve_fake_ip_target(&mut mapped).await;
    assert_eq!(mapped.target, Address::Domain("mapped.example".to_owned()));
    assert!(mapped.direct_target.is_none());
    assert_eq!(mapped.original_target, Some(Address::Ipv4([198, 18, 0, 1])));
    assert_eq!(
        mapped.target_host_source,
        Some(zero_core::TargetHostSource::FakeIp)
    );
    assert_eq!(
        mapped.fake_ip_reverse_status,
        Some(zero_core::FakeIpReverseStatus::Resolved)
    );

    let mut missing = Session::new(
        2,
        Address::Ipv4([198, 18, 0, 99]),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    runtime.resolve_fake_ip_target(&mut missing).await;
    assert_eq!(missing.target, Address::Ipv4([198, 18, 0, 99]));
    assert_eq!(
        missing.fake_ip_reverse_status,
        Some(zero_core::FakeIpReverseStatus::Missing)
    );
}

#[tokio::test]
async fn ip_target_without_fake_ip_configuration_remains_unannotated() {
    let config =
        RuntimeConfig::parse(r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#)
            .expect("parse config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let runtime = TcpIngressRuntime::new(proxy.tcp_runtime_services(), "tun-test".to_owned(), None);
    let mut session = Session::new(
        1,
        Address::Ipv4([203, 0, 113, 7]),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );

    runtime.resolve_fake_ip_target(&mut session).await;

    assert_eq!(session.target, Address::Ipv4([203, 0, 113, 7]));
    assert!(session.original_target.is_none());
    assert!(session.fake_ip_reverse_status.is_none());
}

fn dns_a_query(domain: &str) -> Vec<u8> {
    let mut query = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in domain.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&1_u16.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    query
}

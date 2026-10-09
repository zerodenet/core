use std::net::Ipv6Addr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_api::{event_type, EventFilter, EventSource};
use zero_config::RuntimeConfig;
use zero_core::{Address, FakeIpReverseStatus, Network, ProtocolType, Session, TargetHostSource};
use zero_traits::IpAddress;

use super::{dns_a_query, TcpIngressRuntime};

fn fake_ip_proxy(dns_port: u16) -> crate::runtime::Proxy {
    let config = RuntimeConfig::parse(&format!(r#"{{
        "runtime": {{"dns": {{
            "servers": {{"local": {{"type": "udp", "host": "127.0.0.1", "port": {dns_port}}}}},
            "default_server": "local",
            "policy": {{"address_family": "ipv4_only", "timeout_ms": 1000}},
            "answer": {{"type": "fake_ip", "cidr": "198.18.0.0/15", "ttl_seconds": 60, "max_entries": 16}}
        }}}},
        "outbounds": [{{"tag": "strict-v6", "protocol": {{"type": "direct"}}, "dial": {{"address_family": "only_ipv6"}}}}],
        "route": {{
            "rules": [],
            "url_rewrite": [{{"from": "fake-mux.test", "to": "mux-target.test"}}],
            "final": {{"type": "route", "outbound": "strict-v6"}}
        }}
    }}"#)).expect("parse strict Direct Fake-IP config");
    crate::runtime::Proxy::new(config).expect("build strict Direct proxy")
}

#[tokio::test]
async fn mux_open_restores_ipv4_fake_ip_and_rewrites_before_only_ipv6_resolution() {
    let upstream = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let endpoint = upstream.local_addr().unwrap();
    let dns = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy = fake_ip_proxy(dns.local_addr().unwrap().port());
    let runtime = TcpIngressRuntime::new(proxy.tcp_runtime_services(), "mux-test".into(), None);
    let response = runtime
        .resolver()
        .answer_udp_query(&dns_a_query("fake-mux.test"))
        .await
        .unwrap();
    let synthetic = Address::Ipv4(response[response.len() - 4..].try_into().unwrap());
    let dns_task = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = dns.recv_from(&mut request).await.unwrap();
        let question = zero_dns::udp::parse_dns_question(&request[..size]).unwrap();
        assert_eq!(
            question.domain, "mux-target.test",
            "rewrite must follow Fake-IP recovery"
        );
        assert_eq!(
            question.query_type, 28,
            "strict Direct must override global IPv4 lookup"
        );
        let response = zero_dns::udp::build_dns_response(
            &request[..size],
            &[IpAddress::V6(Ipv6Addr::LOCALHOST.octets())],
        );
        dns.send_to(&response, peer).await.unwrap();
    });
    let mut session = Session::new(
        0,
        synthetic.clone(),
        endpoint.port(),
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    // This is exactly the no-sniffing MUX open boundary; do not pre-restore it.
    let mut route = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.open_tcp_upstream(&mut session),
    )
    .await
    .expect("MUX open must finish")
    .expect("restored domain must dial real IPv6");
    assert_eq!(session.target, Address::Domain("mux-target.test".into()));
    assert_eq!(session.original_target, Some(synthetic));
    assert_eq!(session.direct_target, None);
    assert_eq!(session.target_host_source, Some(TargetHostSource::FakeIp));
    assert_eq!(
        session.fake_ip_reverse_status,
        Some(FakeIpReverseStatus::Resolved)
    );
    assert_eq!(route.outbound_tag, "strict-v6");
    assert_eq!(
        route.upstream_endpoint,
        Some(("::1".into(), endpoint.port()))
    );
    assert_eq!(
        proxy.engine().active_sessions().len(),
        1,
        "open must not prematurely finish the relay session"
    );
    assert!(proxy.engine().completed_sessions().is_empty());
    let (mut peer, _) = tokio::time::timeout(Duration::from_secs(1), upstream.accept())
        .await
        .unwrap()
        .unwrap();
    route.upstream.write_all(b"mux").await.unwrap();
    let mut payload = [0; 3];
    peer.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"mux");
    dns_task.await.unwrap();
    let mut handle = runtime.track_session(session.id);
    assert!(handle
        .finish(zero_engine::SessionOutcome::DirectRelayed)
        .is_some());
}

#[tokio::test]
async fn mux_open_reverse_miss_fails_closed_and_completes_exactly_once() {
    let dns = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy = fake_ip_proxy(dns.local_addr().unwrap().port());
    let runtime = TcpIngressRuntime::new(proxy.tcp_runtime_services(), "mux-test".into(), None);
    let synthetic = Address::Ipv4([198, 18, 0, 99]);
    let mut session = Session::new(
        0,
        synthetic.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    let error = match runtime.open_tcp_upstream(&mut session).await {
        Ok(_) => panic!("missing Fake-IP mapping must not reach Direct dispatch"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "fake_ip_reverse_missing");
    assert!(proxy.engine().active_sessions().is_empty());
    let completed = proxy.engine().completed_sessions();
    assert_eq!(completed.len(), 1);
    let record = &completed[0];
    assert_eq!(record.original_target, Some(synthetic));
    assert_eq!(
        record.fake_ip_reverse_status,
        Some(FakeIpReverseStatus::Missing)
    );
    assert_eq!(record.close_reason.as_deref(), Some("target_error"));
    assert_eq!(record.failure.as_ref().unwrap().stage, "target_recovery");
    assert!(record.route.is_none());
    assert!(record.path.network.is_none());
    assert_eq!(record.outbound_tx_bytes, 0);
    assert_eq!(record.outbound_rx_bytes, 0);
    let events = proxy
        .engine()
        .latest(
            usize::MAX,
            EventFilter {
                event_types: vec![event_type::FLOW_COMPLETED.into()],
                ..EventFilter::default()
            },
        )
        .unwrap();
    assert_eq!(events.len(), 1, "the recovery branch must finish only once");
    let mut query = [0; 512];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), dns.recv_from(&mut query))
            .await
            .is_err(),
        "missing synthetic mappings must not trigger DNS lookups"
    );
}

use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::net::UdpSocket;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::{DnsResolver, IpAddress};

use crate::runtime::target::{resolve_dns_target, resolve_packet_targets};

async fn dns(answer: IpAddress) -> (u16, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let task = tokio::spawn(async move {
        let mut request = [0; 4096];
        loop {
            let (size, peer) = socket.recv_from(&mut request).await.unwrap();
            requests.fetch_add(1, Ordering::Relaxed);
            let reply = zero_dns::udp::build_dns_response(&request[..size], &[answer]);
            socket.send_to(&reply, peer).await.unwrap();
        }
    });
    (port, count, task)
}

#[tokio::test]
async fn packet_targets_use_dispatch_and_real_answers_without_changing_direct_or_node_roles() {
    let (company, company_count, company_task) = dns(IpAddress::V4([192, 168, 1, 235])).await;
    let (direct, direct_count, direct_task) = dns(IpAddress::V4([203, 0, 113, 7])).await;
    let (node, node_count, node_task) = dns(IpAddress::V4([127, 0, 0, 1])).await;
    let config = zero_config::RuntimeConfig::parse(
        &json!({"runtime":{"dns":{
        "servers":{
            "company":{"type":"udp","host":"127.0.0.1","port":company},
            "direct":{"type":"udp","host":"127.0.0.1","port":direct},
            "node":{"type":"udp","host":"127.0.0.1","port":node}
        },"default_server":"direct",
        "dispatch":[{"condition":{"type":"domain","values":["starmerx.com"]},"server":"company"}],
        "answer":{"type":"fake_ip","cidr":"198.18.0.0/15","ttl_seconds":60,"max_entries":16},
        "policy":{"direct_server":"direct","node_server":"node","address_family":"ipv4_only"}
    }},"route":{"rules":[],"final":{"type":"direct"}}})
        .to_string(),
    )
    .unwrap();
    let resolver = zero_dns::DnsSystem::build(config.runtime.dns.as_ref()).unwrap();
    let domain = "jms.yt.starmerx.com";
    let private = "192.168.1.235:443".parse().unwrap();
    for network in [Network::Tcp, Network::Udp] {
        let session = Session::new(
            1,
            Address::Domain(domain.into()),
            443,
            network,
            ProtocolType::UNKNOWN,
        );
        assert_eq!(
            resolve_packet_targets(&session, &resolver).await.unwrap(),
            vec![private]
        );
    }
    assert!(company_count.load(Ordering::Relaxed) > 0);
    assert_eq!(direct_count.load(Ordering::Relaxed), 0);
    assert_eq!(node_count.load(Ordering::Relaxed), 0);
    assert_eq!(
        resolver.resolve_direct(domain).await.unwrap(),
        vec![IpAddress::V4([203, 0, 113, 7])]
    );
    assert_eq!(
        resolver.resolve_node(domain).await.unwrap(),
        vec![IpAddress::V4([127, 0, 0, 1])]
    );

    let synthetic = resolver.resolve(domain).await.unwrap()[0];
    let IpAddress::V4(octets) = synthetic else {
        panic!("expected Fake-IP")
    };
    let mut session = Session::new(
        2,
        Address::Ipv4(octets),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    resolve_dns_target(&resolver, &mut session).await.unwrap();
    assert_eq!(
        resolve_packet_targets(&session, &resolver).await.unwrap(),
        vec![private]
    );
    // A recovered transparent target's Direct-only pin must not override the
    // logical business destination selected for a packet-capable outbound.
    session.direct_target = Some(Address::Ipv4([203, 0, 113, 7]));
    assert_eq!(
        resolve_packet_targets(&session, &resolver).await.unwrap(),
        vec![private]
    );
    assert_eq!(session.direct_target, Some(Address::Ipv4([203, 0, 113, 7])));
    company_task.abort();
    direct_task.abort();
    node_task.abort();
}

#[tokio::test]
async fn packet_literal_targets_preserve_both_families_and_reject_a_missing_port_without_dns() {
    let config = zero_config::RuntimeConfig::parse(r#"{"runtime":{"dns":{"servers":{"unavailable":{"type":"udp","host":"127.0.0.1","port":9}},"default_server":"unavailable","policy":{"address_family":"ipv4_only","timeout_ms":10}}},"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap();
    let resolver = zero_dns::DnsSystem::build(config.runtime.dns.as_ref()).unwrap();
    for (target, expected_ip) in [
        (Address::Ipv4([192, 168, 1, 235]), "192.168.1.235"),
        (
            Address::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            "::1",
        ),
    ] {
        let mut session = Session::new(1, target.clone(), 443, Network::Udp, ProtocolType::UNKNOWN);
        let resolved = resolve_packet_targets(&session, &resolver).await.unwrap();
        assert_eq!(resolved[0].ip().to_string(), expected_ip);
        assert_eq!(resolved[0].port(), 443);
        session.port = 0;
        assert!(resolve_packet_targets(&session, &resolver).await.is_err());
    }
}

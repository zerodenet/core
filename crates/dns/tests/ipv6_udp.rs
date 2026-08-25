#![cfg(feature = "udp")]

use std::collections::BTreeMap;

use zero_config::{DnsAnswerConfig, DnsConfig, DnsServerConfig};
use zero_traits::IpAddress;

#[tokio::test]
async fn udp_dns_uses_an_ipv6_socket_for_an_ipv6_server() {
    let server = tokio::net::UdpSocket::bind("[::1]:0")
        .await
        .expect("bind IPv6 DNS server");
    let port = server.local_addr().expect("DNS server address").port();
    let task = tokio::spawn(async move {
        for _ in 0..2 {
            let mut request = [0_u8; 512];
            let (size, peer) = server.recv_from(&mut request).await.expect("receive query");
            let response = zero_dns::udp::build_dns_response(
                &request[..size],
                &[IpAddress::V4([192, 0, 2, 53])],
            );
            server
                .send_to(&response, peer)
                .await
                .expect("send DNS response");
        }
    });
    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([(
            "test".to_owned(),
            DnsServerConfig::Udp {
                host: "::1".to_owned(),
                port,
                bootstrap: Vec::new(),
            },
        )]),
        default_server: "test".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer: DnsAnswerConfig::Real,
        policy: Default::default(),
    }))
    .expect("build IPv6 UDP resolver");

    let addresses = dns
        .resolve_real("ipv6-transport.example")
        .await
        .expect("resolve through IPv6 UDP server");
    assert_eq!(addresses, vec![IpAddress::V4([192, 0, 2, 53])]);
    task.await.expect("DNS task failed");
}

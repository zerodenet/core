use std::collections::BTreeMap;
#[cfg(feature = "socks5")]
use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use zero_config::{
    DnsAddressFamilyPolicy, DnsAnswerConfig, DnsConfig, DnsPolicyConfig, DnsServerConfig,
};
use zero_core::{Address, FakeIpReverseStatus, Network, ProtocolType, Session, TargetHostSource};
use zero_traits::IpAddress;
#[cfg(feature = "socks5")]
use zero_traits::AsyncSocket;

use crate::runtime::target::resolve_dns_target;
use crate::transport::DirectConnector;

#[tokio::test]
async fn mapped_fake_ipv6_recovers_without_sniffing_and_uses_every_ipv4_candidate() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS");
    let port = socket.local_addr().expect("DNS local address").port();
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = socket
            .recv_from(&mut request)
            .await
            .expect("receive A query");
        let question = zero_dns::udp::parse_dns_question(&request[..size])
            .expect("parse direct fallback query");
        assert_eq!(
            question.query_type, 1,
            "IPv6 egress fallback must use a real A query"
        );
        let addresses = [
            IpAddress::V4([192, 0, 2, 1]),
            IpAddress::V4([192, 0, 2, 2]),
            IpAddress::V4([192, 0, 2, 3]),
        ];
        let response = zero_dns::udp::build_dns_response(&request[..size], &addresses);
        socket
            .send_to(&response, peer)
            .await
            .expect("send A response");
    });
    let dns = fake_dns(port);

    let response = dns
        .answer_udp_query(&dns_query("mapped.example", 28))
        .await
        .expect("allocate synthetic AAAA mapping");
    let synthetic = Address::Ipv6(
        response[response.len() - 16..]
            .try_into()
            .expect("sixteen-byte AAAA response"),
    );

    let mut recovered_tcp = None;
    for (id, network) in [(1, Network::Tcp), (2, Network::Udp)] {
        let mut session = Session::new(id, synthetic.clone(), 443, network, ProtocolType::UNKNOWN);
        session.transparent_target = true;

        resolve_dns_target(&dns, &mut session)
            .await
            .expect("restore mapped FakeIPv6 domain");

        assert_eq!(session.target, Address::Domain("mapped.example".to_owned()));
        assert_eq!(session.original_target, Some(synthetic.clone()));
        assert!(session.direct_target.is_none());
        assert_eq!(session.target_host_source, Some(TargetHostSource::FakeIp));
        assert_eq!(
            session.fake_ip_reverse_status,
            Some(FakeIpReverseStatus::Resolved)
        );
        assert!(session.sni.is_none());

        if network == Network::Tcp {
            recovered_tcp = Some(session);
        }
    }

    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no physical IPv6 default route");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let session = recovered_tcp.expect("recovered TCP session");
    let resolution = DirectConnector
        .resolve_target_addrs(&session, &dns, &egress)
        .await
        .expect("trusted Fake-IP domain should resolve through IPv4");

    assert_eq!(
        resolution.candidates,
        [
            Ipv4Addr::new(192, 0, 2, 1),
            Ipv4Addr::new(192, 0, 2, 2),
            Ipv4Addr::new(192, 0, 2, 3),
        ]
        .map(|ip| SocketAddr::new(IpAddr::V4(ip), 443))
    );
    let network =
        DirectConnector.udp_network_observation(&resolution, resolution.candidates[0], &egress);
    let fallback = network
        .address_family_fallback
        .expect("IPv6-to-IPv4 fallback should be observable");
    assert_eq!(fallback.from, "ipv6");
    assert_eq!(fallback.to, "ipv4");
    assert_eq!(fallback.reason, "tun_ipv6_egress_unavailable");
    assert_eq!(
        fallback.unavailable_reason.as_deref(),
        Some("no physical IPv6 default route")
    );

    server.await.expect("DNS server task");
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn mapped_fake_ipv6_is_forwarded_to_a_proxy_as_the_recovered_domain() {
    let dns = fake_dns(9);
    let response = dns
        .answer_udp_query(&dns_query("proxied.example", 28))
        .await
        .expect("allocate synthetic AAAA mapping");
    let synthetic = Address::Ipv6(
        response[response.len() - 16..]
            .try_into()
            .expect("sixteen-byte AAAA response"),
    );
    let mut session = Session::new(
        3,
        synthetic.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    session.transparent_target = true;

    resolve_dns_target(&dns, &mut session)
        .await
        .expect("restore mapped FakeIPv6 domain");

    let mut socket = RecordingSocket::new(&[
        0x05, 0x00, // no-auth accepted
        0x05, 0x00, 0x00, 0x01, // connect succeeded with an IPv4 bound address
        127, 0, 0, 1, 0x00, 0x50,
    ]);
    socks5::Socks5Outbound
        .establish_tunnel(&mut socket, &session)
        .await
        .expect("establish proxied tunnel");

    let domain = b"proxied.example";
    let mut expected = vec![
        0x05,
        0x01,
        0x00,
        0x05,
        0x01,
        0x00,
        0x03,
        domain.len() as u8,
    ];
    expected.extend_from_slice(domain);
    expected.extend_from_slice(&443_u16.to_be_bytes());

    assert_eq!(
        session.target,
        Address::Domain("proxied.example".to_owned())
    );
    assert_eq!(session.original_target, Some(synthetic));
    assert_eq!(session.target_host_source, Some(TargetHostSource::FakeIp));
    assert_eq!(socket.writes, expected);
}

#[cfg(feature = "socks5")]
#[derive(Debug)]
struct RecordingSocket {
    reads: VecDeque<u8>,
    writes: Vec<u8>,
}

#[cfg(feature = "socks5")]
impl RecordingSocket {
    fn new(input: &[u8]) -> Self {
        Self {
            reads: input.iter().copied().collect(),
            writes: Vec::new(),
        }
    }
}

#[cfg(feature = "socks5")]
impl AsyncSocket for RecordingSocket {
    type Error = ();

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let mut read = 0;
        while read < buf.len() {
            let Some(byte) = self.reads.pop_front() else {
                break;
            };
            buf[read] = byte;
            read += 1;
        }
        Ok(read)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.writes.extend_from_slice(buf);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn fake_dns(port: u16) -> zero_dns::DnsSystem {
    zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([(
            "local".to_owned(),
            DnsServerConfig::Udp {
                host: "127.0.0.1".to_owned(),
                port,
                bootstrap: Vec::new(),
                detour: None,
            },
        )]),
        default_server: "local".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/24".to_owned(),
            ipv6_cidr: Some("fd00::/120".to_owned()),
            ttl_seconds: 60,
            max_entries: Some(16),
            exclude_domains: Vec::new(),
        },
        policy: DnsPolicyConfig {
            address_family: DnsAddressFamilyPolicy::PreferIpv4,
            ..Default::default()
        },
    }))
    .expect("build dual-stack Fake-IP DNS")
}

fn dns_query(domain: &str, query_type: u16) -> Vec<u8> {
    let mut query = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in domain.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&query_type.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    query
}

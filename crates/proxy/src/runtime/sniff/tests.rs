use std::collections::BTreeMap;
use std::collections::VecDeque;

use zero_config::{DnsAnswerConfig, DnsConfig, DnsPolicyConfig, DnsServerConfig};
use zero_core::{
    Address, Error, InboundMuxTcpRelay, Network, ProtocolType, Session, TargetHostSource,
};
use zero_traits::AsyncSocket;

use super::{sniff_mux_tcp_session, sniff_tcp_prefix, SniffProgress, SniffingPolicy};

struct ChunkRelay {
    chunks: VecDeque<Vec<u8>>,
    reads: usize,
}

impl InboundMuxTcpRelay for ChunkRelay {
    fn mux_session_id(&self) -> u16 {
        1
    }

    async fn close_stream(&self) {}

    async fn read_inbound_chunk(&mut self, max_bytes: usize) -> Result<Option<Vec<u8>>, Error> {
        self.reads += 1;
        let Some(mut chunk) = self.chunks.pop_front() else {
            return Ok(None);
        };
        if chunk.len() > max_bytes {
            let remainder = chunk.split_off(max_bytes);
            self.chunks.push_front(remainder);
        }
        Ok(Some(chunk))
    }

    async fn relay_stream<S>(self, _upstream: S)
    where
        S: AsyncSocket + 'static,
        S::Error: Send,
    {
    }
}

fn policy(overrides: &[&str], excluded: &[&str], route_only: bool) -> SniffingPolicy {
    SniffingPolicy::new(
        true,
        &overrides
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
        &excluded
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
        false,
        route_only,
    )
    .unwrap()
}

#[test]
fn http_prefix_waits_for_headers_and_extracts_normalized_host() {
    let policy = policy(&["http"], &[], false);
    assert_eq!(
        sniff_tcp_prefix(b"GET / HTTP/1.1\r\nHost: ExAmPle.COM", &policy, false),
        SniffProgress::Pending
    );
    let SniffProgress::Domain(sniffed) = sniff_tcp_prefix(
        b"GET / HTTP/1.1\r\nHost: ExAmPle.COM:8080\r\n\r\nbody",
        &policy,
        false,
    ) else {
        panic!("expected HTTP hostname");
    };
    assert_eq!(sniffed.domain, "example.com");
    assert_eq!(sniffed.source, TargetHostSource::HttpHost);
}

#[test]
fn exclusions_block_exact_and_regex_domains() {
    let policy = policy(&["http"], &["blocked.example", "regexp:^private\\."], false);
    for domain in ["blocked.example", "private.example"] {
        let request = format!("GET / HTTP/1.1\r\nHost: {domain}\r\n\r\n");
        let SniffProgress::Domain(sniffed) = sniff_tcp_prefix(request.as_bytes(), &policy, false)
        else {
            panic!("expected parsed hostname");
        };
        assert!(!policy.accepts(&sniffed));
    }
}

#[test]
fn tls_client_hello_extracts_sni_for_tcp_and_quic_users() {
    let hostname = b"tls.example";
    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0_u16.to_be_bytes());
    extensions.extend_from_slice(&((hostname.len() + 5) as u16).to_be_bytes());
    extensions.extend_from_slice(&((hostname.len() + 3) as u16).to_be_bytes());
    extensions.push(0);
    extensions.extend_from_slice(&(hostname.len() as u16).to_be_bytes());
    extensions.extend_from_slice(hostname);
    let mut hello = vec![0_u8; 34];
    hello.push(0);
    hello.extend_from_slice(&2_u16.to_be_bytes());
    hello.extend_from_slice(&0x1301_u16.to_be_bytes());
    hello.push(1);
    hello.push(0);
    hello.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    hello.extend_from_slice(&extensions);
    let mut handshake = vec![1, 0, 0, 0];
    let length = hello.len();
    handshake[1] = ((length >> 16) & 0xff) as u8;
    handshake[2] = ((length >> 8) & 0xff) as u8;
    handshake[3] = (length & 0xff) as u8;
    handshake.extend_from_slice(&hello);

    let SniffProgress::Domain(sniffed) = super::sniff_tls_handshake(&handshake) else {
        panic!("expected TLS hostname");
    };
    assert_eq!(sniffed.domain, "tls.example");
    assert_eq!(sniffed.source, TargetHostSource::TlsSni);
}

#[test]
fn route_only_changes_route_target_without_changing_dial_target() {
    let policy = policy(&["http"], &[], true);
    let mut session = Session::new(
        1,
        Address::Ipv4([203, 0, 113, 8]),
        80,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let SniffProgress::Domain(sniffed) = sniff_tcp_prefix(
        b"GET / HTTP/1.1\r\nHost: route.example\r\n\r\n",
        &policy,
        false,
    ) else {
        panic!("expected parsed hostname");
    };
    policy.apply_to_session(&mut session, sniffed);
    assert_eq!(session.target, Address::Ipv4([203, 0, 113, 8]));
    assert_eq!(
        session.effective_direct_target(),
        &Address::Ipv4([203, 0, 113, 8])
    );
    assert_eq!(
        session.effective_route_target(),
        &Address::Domain("route.example".to_owned())
    );
    assert!(session.original_target.is_none());
}

#[test]
fn metadata_only_never_requests_payload() {
    let policy = SniffingPolicy::new(
        true,
        &["http".to_owned(), "tls".to_owned(), "quic".to_owned()],
        &[],
        true,
        false,
    )
    .unwrap();
    assert!(!policy.reads_payload(false));
    assert!(!policy.sniffs_tcp(false));
    assert!(!policy.sniffs_quic(false));
}

#[tokio::test]
async fn mux_tcp_reads_split_prefix_before_routing_and_returns_it_for_replay() {
    let policy = policy(&["http"], &[], false);
    let mut session = Session::new(
        1,
        Address::Ipv4([203, 0, 113, 8]),
        80,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let mut relay = ChunkRelay {
        chunks: [
            b"GET / HTTP/1.1\r\nHo".to_vec(),
            b"st: split.example\r\n\r\npayload".to_vec(),
        ]
        .into(),
        reads: 0,
    };

    let prefix = sniff_mux_tcp_session(&policy, &mut session, &mut relay, false).await;
    assert_eq!(
        prefix,
        b"GET / HTTP/1.1\r\nHost: split.example\r\n\r\npayload"
    );
    assert_eq!(session.target, Address::Domain("split.example".to_owned()));
    assert_eq!(relay.reads, 2);
}

#[tokio::test]
async fn metadata_only_mux_tcp_does_not_touch_relay_payload() {
    let policy = SniffingPolicy::new(true, &["http".to_owned()], &[], true, false).unwrap();
    let mut session = Session::new(
        1,
        Address::Ipv4([203, 0, 113, 8]),
        80,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let mut relay = ChunkRelay {
        chunks: [b"GET / HTTP/1.1\r\nHost: ignored.example\r\n\r\n".to_vec()].into(),
        reads: 0,
    };

    assert!(
        sniff_mux_tcp_session(&policy, &mut session, &mut relay, false)
            .await
            .is_empty()
    );
    assert_eq!(relay.reads, 0);
    assert_eq!(session.target, Address::Ipv4([203, 0, 113, 8]));
}

#[tokio::test]
async fn fake_dns_mapping_overrides_target_even_with_route_only_and_honors_exclusion() {
    let dns = fake_dns();
    let response = dns
        .answer_udp_query(&dns_a_query("mapped.example"))
        .await
        .unwrap();
    let target = Address::Ipv4(response[response.len() - 4..].try_into().unwrap());
    let mut session = Session::new(
        1,
        target.clone(),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let restore_policy = policy(&["fakedns"], &[], true);
    assert!(
        !restore_policy
            .apply_fake_dns_metadata(&dns, &mut session)
            .await
    );
    assert_eq!(session.target, Address::Domain("mapped.example".to_owned()));
    assert!(session.route_target.is_none());
    assert!(session.skip_fake_ip_restore);

    let mut excluded = Session::new(
        2,
        target.clone(),
        443,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let policy = policy(&["fakedns"], &["mapped.example"], true);
    assert!(!policy.apply_fake_dns_metadata(&dns, &mut excluded).await);
    assert_eq!(excluded.target, target);
    assert!(excluded.skip_fake_ip_restore);
}

#[tokio::test]
async fn fake_dns_miss_enables_content_fallback_but_metadata_only_does_not_read() {
    let dns = fake_dns();
    let target = Address::Ipv4([198, 18, 0, 77]);
    let mut session = Session::new(
        1,
        target.clone(),
        80,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    let policy = policy(&["fakedns+others"], &[], true);
    assert!(policy.apply_fake_dns_metadata(&dns, &mut session).await);
    let SniffProgress::Domain(sniffed) = sniff_tcp_prefix(
        b"GET / HTTP/1.1\r\nHost: fallback.example\r\n\r\n",
        &policy,
        true,
    ) else {
        panic!("expected FakeDNS content fallback");
    };
    policy.apply_to_session_with_fake_dns_fallback(&mut session, sniffed, true);
    assert_eq!(
        session.target,
        Address::Domain("fallback.example".to_owned())
    );
    assert!(session.route_target.is_none());

    let metadata_only =
        SniffingPolicy::new(true, &["fakedns+others".to_owned()], &[], true, false).unwrap();
    let mut untouched = Session::new(
        2,
        target.clone(),
        80,
        Network::Tcp,
        ProtocolType::new("vless"),
    );
    assert!(
        !metadata_only
            .apply_fake_dns_metadata(&dns, &mut untouched)
            .await
    );
    assert_eq!(untouched.target, target);
    assert!(!metadata_only.sniffs_tcp(true));
}

fn fake_dns() -> zero_dns::DnsSystem {
    zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/24".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 60,
            max_entries: Some(16),
            exclude_domains: Vec::new(),
        },
        policy: DnsPolicyConfig::default(),
    }))
    .unwrap()
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

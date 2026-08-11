use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use zero_config::RuntimeConfig;
use zero_stack::{packet, UserNetworkStack};
use zero_traits::UdpStack;

use super::run;

#[tokio::test]
async fn tun_udp_uses_kernel_direct_dispatch_and_writes_raw_response() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {"rules": [], "final": {"type": "direct"}}
        }"#,
    )
    .expect("parse direct config");
    let proxy = crate::runtime::Proxy::new(config).expect("create proxy");
    let echo = UdpSocket::bind("127.0.0.1:0").await.expect("bind echo");
    let echo_addr = echo.local_addr().expect("echo address");
    tokio::spawn(async move {
        let mut buffer = [0_u8; 64];
        let (size, peer) = echo.recv_from(&mut buffer).await.expect("receive echo");
        echo.send_to(&buffer[..size], peer)
            .await
            .expect("send echo");
    });

    let (outbound, mut packets) = mpsc::channel(8);
    let stack = UserNetworkStack::new(outbound, 1440);
    let (_tcp, udp) = stack.into_parts();
    let task = tokio::spawn(run(proxy, Arc::clone(&udp), "tun-test".to_owned(), false));

    let source_ip = Ipv4Addr::new(10, 0, 0, 2);
    let request = packet::build_udp(
        IpAddr::V4(source_ip),
        echo_addr.ip(),
        53000,
        echo_addr.port(),
        b"through-kernel",
    );
    udp.feed(&request).await;

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), packets.recv())
        .await
        .expect("TUN UDP response timed out")
        .expect("raw response channel closed");
    let response = packet::parse_udp(&response).expect("parse raw UDP response");
    assert_eq!(response.src.ip, echo_addr.ip());
    assert_eq!(response.src.port, echo_addr.port());
    assert_eq!(response.dst.ip, IpAddr::V4(source_ip));
    assert_eq!(response.dst.port, 53000);
    assert_eq!(response.payload, b"through-kernel");

    task.abort();
}

#[tokio::test]
async fn tun_udp_direct_dispatch_supports_ipv6_targets_and_responses() {
    let config = RuntimeConfig::parse(r#"{"route": {"rules": [], "final": {"type": "direct"}}}"#)
        .expect("parse direct config");
    let proxy = crate::runtime::Proxy::new(config).expect("create proxy");
    let echo = UdpSocket::bind("[::1]:0").await.expect("bind IPv6 echo");
    let echo_addr = echo.local_addr().expect("echo address");
    tokio::spawn(async move {
        let mut buffer = [0_u8; 64];
        let (size, peer) = echo.recv_from(&mut buffer).await.expect("receive echo");
        echo.send_to(&buffer[..size], peer)
            .await
            .expect("send echo");
    });

    let (outbound, mut packets) = mpsc::channel(8);
    let stack = UserNetworkStack::new(outbound, 1440);
    let (_tcp, udp) = stack.into_parts();
    let task = tokio::spawn(run(proxy, Arc::clone(&udp), "tun-test".to_owned(), false));
    let source_ip = "fd00::2".parse().expect("source IPv6");
    udp.feed(&packet::build_udp(
        IpAddr::V6(source_ip),
        echo_addr.ip(),
        53001,
        echo_addr.port(),
        b"through-kernel-v6",
    ))
    .await;

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), packets.recv())
        .await
        .expect("TUN IPv6 UDP response timed out")
        .expect("raw response channel closed");
    let response = packet::parse_udp(&response).expect("parse raw IPv6 UDP response");
    assert_eq!(response.src.ip, echo_addr.ip());
    assert_eq!(response.dst.ip, IpAddr::V6(source_ip));
    assert_eq!(response.payload, b"through-kernel-v6");

    task.abort();
}

#[tokio::test]
async fn tun_udp_block_route_prevents_stun_network_leak() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                {"tag": "blocked", "protocol": {"type": "block"}}
            ],
            "route": {"rules": [], "final": {"type": "route", "outbound": "blocked"}}
        }"#,
    )
    .expect("parse blocked config");
    let proxy = crate::runtime::Proxy::new(config).expect("create proxy");
    let sink = UdpSocket::bind("127.0.0.1:0").await.expect("bind sink");
    let sink_addr = sink.local_addr().expect("sink address");
    let (outbound, mut packets) = mpsc::channel(8);
    let stack = UserNetworkStack::new(outbound, 1440);
    let (_tcp, udp) = stack.into_parts();
    let task = tokio::spawn(run(proxy, Arc::clone(&udp), "tun-test".to_owned(), false));

    let request = packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        sink_addr.ip(),
        54000,
        sink_addr.port(),
        &stun_binding_request(),
    );
    udp.feed(&request).await;

    let mut buffer = [0_u8; 64];
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(200),
        sink.recv_from(&mut buffer)
    )
    .await
    .is_err());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), packets.recv())
            .await
            .is_err()
    );

    task.abort();
}

fn stun_binding_request() -> [u8; 20] {
    [
        0x00, 0x01, 0x00, 0x00, // Binding request, no attributes
        0x21, 0x12, 0xa4, 0x42, // STUN magic cookie
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
    ]
}

#[tokio::test]
async fn tun_dns_hijack_answers_with_fake_ip_without_reaching_destination() {
    let config = RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": [{"type": "system"}],
                    "fake_ip": {"cidr": "198.18.0.0/15", "ttl_seconds": 60}
                }
            },
            "route": {"rules": [], "final": {"type": "direct"}}
        }"#,
    )
    .expect("parse DNS config");
    let proxy = crate::runtime::Proxy::new(config).expect("create proxy");
    let (outbound, mut packets) = mpsc::channel(8);
    let stack = UserNetworkStack::new(outbound, 1440);
    let (_tcp, udp) = stack.into_parts();
    let task = tokio::spawn(run(proxy, Arc::clone(&udp), "tun-test".to_owned(), true));

    let source_ip = Ipv4Addr::new(10, 0, 0, 2);
    let dns_ip = Ipv4Addr::new(203, 0, 113, 53);
    let request = packet::build_udp(
        IpAddr::V4(source_ip),
        IpAddr::V4(dns_ip),
        55000,
        53,
        &dns_query("webrtc.example"),
    );
    udp.feed(&request).await;

    let response = tokio::time::timeout(std::time::Duration::from_secs(1), packets.recv())
        .await
        .expect("DNS response timed out")
        .expect("raw response channel closed");
    let response = packet::parse_udp(&response).expect("parse raw UDP response");
    assert_eq!(response.src.ip, IpAddr::V4(dns_ip));
    assert_eq!(response.src.port, 53);
    assert_eq!(response.dst.ip, IpAddr::V4(source_ip));
    assert_eq!(response.dst.port, 55000);
    assert_eq!(
        u16::from_be_bytes([response.payload[6], response.payload[7]]),
        1
    );
    assert_eq!(
        &response.payload[response.payload.len() - 4..],
        &[198, 18, 0, 1]
    );

    task.abort();
}

fn dns_query(domain: &str) -> Vec<u8> {
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

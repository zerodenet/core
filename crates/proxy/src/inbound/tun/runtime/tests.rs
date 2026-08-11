use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use zero_config::RuntimeConfig;
use zero_stack::{packet, UserNetworkStack, UserTcpStack};
use zero_traits::TcpStack;

use super::{accept_tcp, should_drop_non_unicast_udp};

const CLIENT_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 99, 0, 2));
const CLIENT_PORT: u16 = 49152;
const CLIENT_ISN: u32 = 10_000;

#[test]
fn tun_drops_udp_multicast_and_configured_subnet_broadcast() {
    let address = IpAddr::V4(Ipv4Addr::new(10, 66, 0, 1));
    let mask = IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0));
    for destination in [
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 251)),
        IpAddr::V4(Ipv4Addr::new(10, 66, 0, 255)),
        IpAddr::V4(Ipv4Addr::BROADCAST),
        "ff02::fb".parse().unwrap(),
    ] {
        let source = if destination.is_ipv6() {
            "fd66::2".parse().unwrap()
        } else {
            CLIENT_IP
        };
        let packet = packet::build_udp(source, destination, CLIENT_PORT, 5353, b"discovery");
        assert!(should_drop_non_unicast_udp(&packet, &[(address, mask)]));
    }
}

#[test]
fn tun_keeps_unicast_udp_even_when_address_ends_in_255() {
    let address = IpAddr::V4(Ipv4Addr::new(10, 66, 0, 1));
    let mask = IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0));
    let destination = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 255));
    let packet = packet::build_udp(CLIENT_IP, destination, CLIENT_PORT, 3478, b"stun");
    assert!(!should_drop_non_unicast_udp(&packet, &[(address, mask)]));
}

#[tokio::test]
async fn tun_tcp_uses_kernel_direct_route_and_records_traffic() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP echo");
    let destination = listener.local_addr().expect("echo address");
    let echo = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept TUN connection");
        let mut request = [0_u8; 14];
        stream.read_exact(&mut request).await.expect("read request");
        stream.write_all(&request).await.expect("write response");
        stream.shutdown().await.expect("shutdown echo stream");
    });

    let config = RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
        .expect("parse direct config");
    let proxy = crate::runtime::Proxy::new(config).expect("create proxy");
    let (tcp, mut outbound) = new_stack();
    let runtime = tokio::spawn(accept_tcp(
        proxy.clone(),
        Arc::clone(&tcp),
        "tun-test".to_owned(),
        false,
    ));

    complete_handshake(&tcp, &mut outbound, destination).await;
    let payload = b"through-kernel";
    tcp.feed(&client_packet(
        destination,
        packet::tcp_flags::PSH | packet::tcp_flags::ACK,
        CLIENT_ISN + 1,
        0,
        payload,
    ))
    .await;

    let response = receive_payload(&mut outbound).await;
    assert_eq!(response.payload, payload);
    assert_eq!(response.ack, CLIENT_ISN + 1 + payload.len() as u32);

    tcp.feed(&client_packet(
        destination,
        packet::tcp_flags::FIN | packet::tcp_flags::ACK,
        CLIENT_ISN + 1 + payload.len() as u32,
        response.seq + payload.len() as u32,
        &[],
    ))
    .await;
    echo.await.expect("echo task failed");

    wait_for_stat(|| proxy.engine().stats_snapshot().direct_sessions == 1).await;
    let stats = proxy.engine().stats_snapshot();
    assert_eq!(stats.completed_sessions, 1);
    assert!(stats.bytes_up >= payload.len() as u64);
    assert!(stats.bytes_down >= payload.len() as u64);

    runtime.abort();
}

#[tokio::test]
async fn tun_tcp_block_route_never_opens_the_destination() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP sink");
    let destination = listener.local_addr().expect("sink address");
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds":[{"tag":"blocked","protocol":{"type":"block"}}],
            "route":{"rules":[],"final":{"type":"route","outbound":"blocked"}}
        }"#,
    )
    .expect("parse block config");
    let proxy = crate::runtime::Proxy::new(config).expect("create proxy");
    let (tcp, mut outbound) = new_stack();
    let runtime = tokio::spawn(accept_tcp(
        proxy.clone(),
        Arc::clone(&tcp),
        "tun-test".to_owned(),
        false,
    ));

    complete_handshake(&tcp, &mut outbound, destination).await;
    wait_for_stat(|| proxy.engine().stats_snapshot().blocked_sessions == 1).await;
    let closed = receive_tcp(&mut outbound).await;
    assert!(closed.fin && closed.ack_flag, "blocked TUN TCP must close");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(proxy.engine().stats_snapshot().active_sessions, 0);

    runtime.abort();
}

fn new_stack() -> (Arc<UserTcpStack>, mpsc::Receiver<Vec<u8>>) {
    let (outbound, packets) = mpsc::channel(32);
    let stack = UserNetworkStack::new(outbound, 1440);
    let (tcp, _udp) = stack.into_parts();
    (tcp, packets)
}

async fn complete_handshake(
    tcp: &UserTcpStack,
    outbound: &mut mpsc::Receiver<Vec<u8>>,
    destination: SocketAddr,
) {
    tcp.feed(&client_packet(
        destination,
        packet::tcp_flags::SYN,
        CLIENT_ISN,
        0,
        &[],
    ))
    .await;
    let syn_ack = receive_tcp(outbound).await;
    assert!(syn_ack.syn && syn_ack.ack_flag);
    tcp.feed(&client_packet(
        destination,
        packet::tcp_flags::ACK,
        CLIENT_ISN + 1,
        syn_ack.seq + 1,
        &[],
    ))
    .await;
    assert!(
        outbound.try_recv().is_err(),
        "the TCP stack must not answer a pure ACK"
    );
}

async fn receive_payload(outbound: &mut mpsc::Receiver<Vec<u8>>) -> OwnedTcp {
    loop {
        let packet = receive_tcp(outbound).await;
        if !packet.payload.is_empty() {
            return packet;
        }
    }
}

async fn receive_tcp(outbound: &mut mpsc::Receiver<Vec<u8>>) -> OwnedTcp {
    let raw = tokio::time::timeout(Duration::from_secs(2), outbound.recv())
        .await
        .expect("TUN TCP response timed out")
        .expect("TUN TCP response channel closed");
    let parsed = packet::parse_tcp(&raw).expect("parse TUN TCP response");
    OwnedTcp {
        seq: parsed.seq,
        ack: parsed.ack,
        syn: parsed.syn,
        ack_flag: parsed.ack_flag,
        fin: parsed.fin,
        payload: parsed.payload.to_vec(),
    }
}

fn client_packet(
    destination: SocketAddr,
    flags: u8,
    seq: u32,
    ack: u32,
    payload: &[u8],
) -> Vec<u8> {
    packet::build_tcp(
        CLIENT_IP,
        destination.ip(),
        CLIENT_PORT,
        destination.port(),
        seq,
        ack,
        flags,
        payload,
    )
}

async fn wait_for_stat(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("kernel session statistic was not updated");
}

struct OwnedTcp {
    seq: u32,
    ack: u32,
    syn: bool,
    ack_flag: bool,
    fin: bool,
    payload: Vec<u8>,
}

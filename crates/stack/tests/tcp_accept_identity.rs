use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use tokio::sync::mpsc;
use zero_stack::{packet, UserNetworkStack};
use zero_traits::TcpStack;

const CLIENT_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
const SERVER_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
const CLIENT_PORT: u16 = 54_321;
const SERVER_PORT: u16 = 443;

fn client_packet(flags: u8, sequence: u32, acknowledgement: u32) -> Vec<u8> {
    packet::build_tcp(
        CLIENT_IP,
        SERVER_IP,
        CLIENT_PORT,
        SERVER_PORT,
        sequence,
        acknowledgement,
        flags,
        &[],
    )
}

async fn establish(
    stack: &zero_stack::UserTcpStack,
    outbound: &mut mpsc::Receiver<Vec<u8>>,
    client_sequence: u32,
) {
    stack
        .feed(&client_packet(packet::tcp_flags::SYN, client_sequence, 0))
        .await;
    let syn_ack = outbound.recv().await.expect("SYN-ACK packet");
    let syn_ack = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    stack
        .feed(&client_packet(
            packet::tcp_flags::ACK,
            client_sequence + 1,
            syn_ack.seq + 1,
        ))
        .await;
}

#[tokio::test]
async fn stale_accept_entry_cannot_duplicate_a_reused_four_tuple() {
    let (outbound_tx, mut outbound_rx) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound_tx, 1_440);
    let (tcp, _udp) = stack.into_parts();

    establish(&tcp, &mut outbound_rx, 1_000).await;
    tcp.feed(&client_packet(packet::tcp_flags::RST, 1_001, 0))
        .await;
    establish(&tcp, &mut outbound_rx, 2_000).await;

    let (_stream, source, destination) =
        tokio::time::timeout(Duration::from_millis(100), tcp.accept())
            .await
            .expect("current connection was not accepted")
            .expect("accept queue closed");
    assert_eq!(source.port, CLIENT_PORT);
    assert_eq!(destination.port, SERVER_PORT);

    let second = tokio::time::timeout(Duration::from_millis(20), tcp.accept()).await;
    assert!(
        second.is_err(),
        "stale and current generations were both accepted"
    );
}

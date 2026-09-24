use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Instant,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    time::{timeout, Duration},
};
use zero_traits::TcpStack;

use super::{ClientTcpStack, ClientTcpStackError};
use crate::packet;
use crate::tcp::UserTcpStack;
use crate::{FragmentOutcome, FragmentReassembler};

#[tokio::test]
async fn active_tcp_client_roundtrips_ipv4_payload_and_fin() {
    let client_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    let server_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    roundtrip(client_ip, server_ip).await;
}

#[tokio::test]
async fn active_tcp_client_roundtrips_ipv6_payload_and_fin() {
    let client_ip = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let server_ip = IpAddr::V6("2001:db8::1".parse().unwrap());
    roundtrip(client_ip, server_ip).await;
}

#[tokio::test]
async fn matching_icmp_unreachable_fails_half_open_client_connect() {
    let client_v4 = Ipv4Addr::new(10, 0, 0, 2);
    let server_v4 = Ipv4Addr::new(203, 0, 113, 7);
    let client_ip = IpAddr::V4(client_v4);
    let server_ip = IpAddr::V4(server_v4);
    let (outbound, mut packets) = mpsc::channel(16);
    let client =
        std::sync::Arc::new(ClientTcpStack::new(vec![client_ip], outbound, 1_420).unwrap());
    let connecting = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .connect(client_ip, SocketAddr::new(server_ip, 443))
                .await
        })
    };
    let syn = packets.recv().await.unwrap();
    let mut response = vec![0_u8; 20 + 8 + 28];
    response[0] = 0x45;
    let response_len = response.len() as u16;
    response[2..4].copy_from_slice(&response_len.to_be_bytes());
    response[8] = 64;
    response[9] = packet::IPPROTO_ICMP;
    response[12..16].copy_from_slice(&server_v4.octets());
    response[16..20].copy_from_slice(&client_v4.octets());
    response[20] = 3;
    response[21] = 3;
    response[28..].copy_from_slice(&syn[..28]);
    let icmp_checksum = packet::checksum(&response[20..]);
    response[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = packet::checksum(&response[..20]);
    response[10..12].copy_from_slice(&ip_checksum.to_be_bytes());

    let error = packet::parse_icmp_error(&response).unwrap();
    assert!(client.feed_icmp_error(error).await);
    assert!(matches!(
        timeout(Duration::from_secs(1), connecting)
            .await
            .unwrap()
            .unwrap(),
        Err(ClientTcpStackError::DestinationUnreachable)
    ));
}

#[tokio::test]
async fn packet_too_big_fragments_subsequent_client_tcp_packets() {
    let client_ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    let server_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
    let (outbound, mut packets) = mpsc::channel(16);
    let client =
        std::sync::Arc::new(ClientTcpStack::new(vec![client_ip], outbound, 1_420).unwrap());
    let connecting = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .connect(client_ip, SocketAddr::new(server_ip, 443))
                .await
        })
    };
    let syn = packets.recv().await.unwrap();
    let source_port = packet::parse_tcp(&syn).unwrap().src.port;
    let large = packet::build_tcp(
        client_ip,
        server_ip,
        source_port,
        443,
        1,
        1,
        packet::tcp_flags::ACK,
        &vec![0x42; 1_100],
    );
    assert_eq!(client.fragment_outbound_packet(&large).await.len(), 1);

    let error = packet::build_icmp_response(&large, 900).unwrap();
    let error = packet::parse_icmp_error(&error).unwrap();
    assert!(client.feed_icmp_error(error).await);
    let fragments = client.fragment_outbound_packet(&large).await;
    assert!(fragments.len() > 1);
    assert!(fragments.iter().all(|fragment| fragment.len() <= 900));
    let mut reassembler = FragmentReassembler::new();
    let mut rebuilt = None;
    for fragment in &fragments {
        if let FragmentOutcome::Reassembled(packet) = reassembler.process(fragment, Instant::now())
        {
            rebuilt = Some(packet);
        }
    }
    let rebuilt = rebuilt.expect("TCP packet reassembled");
    assert_eq!(
        packet::parse_tcp(&rebuilt).unwrap().payload,
        packet::parse_tcp(&large).unwrap().payload
    );
    assert!(!connecting.is_finished());
    connecting.abort();
}

#[tokio::test]
async fn packet_too_big_reduces_client_tcp_segment_size_for_ipv4_and_ipv6() {
    for (client_ip, server_ip, path_mtu) in [
        (
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
            900,
        ),
        (
            IpAddr::V6("fd00::2".parse().unwrap()),
            IpAddr::V6("2001:db8::7".parse().unwrap()),
            1_280,
        ),
    ] {
        let (outbound, mut packets) = mpsc::channel(16);
        let (server_outbound, mut server_packets) = mpsc::channel(16);
        let client =
            std::sync::Arc::new(ClientTcpStack::new(vec![client_ip], outbound, 1_420).unwrap());
        let server = UserTcpStack::new(server_outbound, 1_420);
        let connecting = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .connect(client_ip, SocketAddr::new(server_ip, 443))
                    .await
            })
        };
        server.feed(&packets.recv().await.unwrap()).await;
        client.feed(&server_packets.recv().await.unwrap()).await;
        let mut stream = connecting.await.unwrap().unwrap();

        stream.write_all(&[0x42; 1_300]).await.unwrap();
        let first = next_data_packet(&mut packets).await;
        assert!(first.len() > path_mtu);
        let error = packet::build_icmp_response(&first, path_mtu).unwrap();
        assert!(
            client
                .feed_icmp_error(packet::parse_icmp_error(&error).unwrap())
                .await
        );

        let sent = stream.write(&[0x43; 1_300]).await.unwrap();
        assert_eq!(sent, path_mtu - 60);
        let next = next_data_packet(&mut packets).await;
        assert!(next.len() <= path_mtu);
        assert_eq!(packet::parse_tcp(&next).unwrap().payload.len(), sent);
    }
}

async fn next_data_packet(packets: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
    loop {
        let packet = timeout(Duration::from_secs(1), packets.recv())
            .await
            .unwrap()
            .unwrap();
        if packet::parse_tcp(&packet).is_some_and(|tcp| !tcp.payload.is_empty()) {
            return packet;
        }
    }
}

async fn roundtrip(client_ip: IpAddr, server_ip: IpAddr) {
    let (client_packets, mut client_rx) = mpsc::channel(256);
    let (server_packets, mut server_rx) = mpsc::channel(256);
    let client =
        std::sync::Arc::new(ClientTcpStack::new(vec![client_ip], client_packets, 1_420).unwrap());
    let server = std::sync::Arc::new(UserTcpStack::new(server_packets, 1_420));
    let server_feed = server.clone();
    let client_feed = client.clone();
    let pump_to_server = tokio::spawn(async move {
        while let Some(packet) = client_rx.recv().await {
            server_feed.feed(&packet).await;
        }
    });
    let pump_to_client = tokio::spawn(async move {
        while let Some(packet) = server_rx.recv().await {
            client_feed.feed(&packet).await;
        }
    });
    let roundtrip = async {
        let mut outbound = client
            .connect(client_ip, SocketAddr::new(server_ip, 443))
            .await
            .expect("client handshake");
        let (mut inbound, _, _) = server.accept().await.expect("server handshake");
        outbound.write_all(b"request").await.expect("client write");
        let mut request = [0_u8; 7];
        inbound.read_exact(&mut request).await.expect("server read");
        assert_eq!(&request, b"request");
        inbound.write_all(b"response").await.expect("server write");
        let mut response = [0_u8; 8];
        outbound
            .read_exact(&mut response)
            .await
            .expect("client read");
        assert_eq!(&response, b"response");
        outbound.shutdown().await.expect("client FIN");
        let mut eof = [0_u8; 1];
        assert_eq!(inbound.read(&mut eof).await.expect("server EOF"), 0);
    };
    timeout(Duration::from_secs(3), roundtrip)
        .await
        .expect("TCP roundtrip timed out");
    pump_to_server.abort();
    pump_to_client.abort();
}

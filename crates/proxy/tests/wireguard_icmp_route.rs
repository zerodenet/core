#![cfg(feature = "wireguard")]

mod support;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use boringtun::x25519::{PublicKey, StaticSecret};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{sleep, timeout, Duration},
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
use zero_stack::packet;

use support::{free_udp_port, spawn_engine};

fn public_key(private: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([private; 32])).as_bytes())
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_icmp_echo_routes_through_wireguard_outbound() {
    routed_packet(IpAddr::V4(Ipv4Addr::LOCALHOST), false, false, false).await;
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_ipv6_icmp_echo_routes_through_wireguard_outbound() {
    routed_packet(IpAddr::V6(Ipv6Addr::LOCALHOST), false, false, false).await;
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_udp_packet_routes_through_wireguard_outbound() {
    routed_packet(IpAddr::V4(Ipv4Addr::LOCALHOST), true, false, false).await;
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_ipv6_udp_packet_routes_through_wireguard_outbound() {
    routed_packet(IpAddr::V6(Ipv6Addr::LOCALHOST), true, false, false).await;
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_large_udp_packet_fragments_at_wireguard_outbound() {
    routed_packet(IpAddr::V4(Ipv4Addr::LOCALHOST), true, true, false).await;
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_tcp_packet_routes_through_wireguard_outbound() {
    routed_packet(IpAddr::V4(Ipv4Addr::LOCALHOST), false, false, true).await;
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn authenticated_ipv6_tcp_packet_routes_through_wireguard_outbound() {
    routed_packet(IpAddr::V6(Ipv6Addr::LOCALHOST), false, false, true).await;
}

async fn routed_packet(target: IpAddr, udp: bool, large: bool, tcp: bool) {
    let ipv6 = target.is_ipv6();
    let echo_socket = if udp {
        Some(UdpSocket::bind((target, 0)).await.unwrap())
    } else {
        None
    };
    let echo_port = echo_socket
        .as_ref()
        .map(|socket| socket.local_addr().unwrap().port())
        .unwrap_or(0);
    let tcp_listener = if tcp {
        Some(TcpListener::bind((target, 0)).await.unwrap())
    } else {
        None
    };
    let echo_port = tcp_listener
        .as_ref()
        .map(|listener| listener.local_addr().unwrap().port())
        .unwrap_or(echo_port);
    let echo_task = echo_socket.map(|socket| {
        tokio::spawn(async move {
            let mut buffer = [0_u8; 2048];
            let (size, sender) = socket.recv_from(&mut buffer).await.unwrap();
            socket.send_to(&buffer[..size], sender).await.unwrap();
        })
    });
    let tcp_task = tcp_listener.map(|listener| {
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut payload = [0_u8; 9];
            stream.read_exact(&mut payload).await.unwrap();
            stream.write_all(&payload).await.unwrap();
        })
    });
    let (client_address, hop_address, client_allowed, target_allowed) = if ipv6 {
        ("fd00::2/128", "fd09::2/128", "fd00::2/128", "::1/128")
    } else {
        ("10.0.0.2/32", "10.9.0.2/32", "10.0.0.2/32", "127.0.0.1/32")
    };

    let first_port = free_udp_port();
    let second_port = free_udp_port();
    let first_private = STANDARD.encode([81_u8; 32]);
    let client_private = STANDARD.encode([82_u8; 32]);
    let hop_private = STANDARD.encode([83_u8; 32]);
    let second_private = STANDARD.encode([84_u8; 32]);
    let first_public = public_key(81);
    let client_public = public_key(82);
    let hop_public = public_key(83);
    let second_public = public_key(84);

    let second_config = RuntimeConfig::parse(&format!(
        r#"{{
        "inbounds": [{{
            "tag": "wg-second", "listen": {{"address": "127.0.0.1", "port": {second_port}}},
            "protocol": {{"type": "wireguard", "private_key": "{second_private}",
            "peers": [{{"public_key": "{hop_public}", "allowed_ips": ["{client_allowed}"]}}]}}
        }}],
        "outbounds": [{{"tag": "direct", "protocol": {{"type": "direct"}}}}],
        "route": {{"rules": [], "final": {{"type": "route", "outbound": "direct"}}}}
    }}"#
    ))
    .unwrap();
    let second = spawn_engine(Proxy::new(second_config).unwrap());
    sleep(Duration::from_millis(100)).await;

    let first_config = RuntimeConfig::parse(&format!(
        r#"{{
        "inbounds": [{{
            "tag": "wg-first", "listen": {{"address": "127.0.0.1", "port": {first_port}}},
            "protocol": {{"type": "wireguard", "private_key": "{first_private}",
                "peers": [{{"public_key": "{client_public}", "allowed_ips": ["{client_allowed}"]}}]}}
        }}],
        "outbounds": [{{"tag": "wg-hop", "protocol": {{
            "type": "wireguard", "private_key": "{hop_private}", "addresses": ["{hop_address}"], "mtu": 1280,
            "peers": [{{"public_key": "{second_public}", "endpoint": "127.0.0.1:{second_port}",
                "allowed_ips": ["{target_allowed}"]}}]
        }}}}],
        "route": {{"rules": [], "final": {{"type": "route", "outbound": "wg-hop"}}}}
    }}"#
    ))
    .unwrap();
    let first = spawn_engine(Proxy::new(first_config).unwrap());
    sleep(Duration::from_millis(100)).await;

    let peers = [PeerInput {
        public_key: &first_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &[if ipv6 { "::/0" } else { "0.0.0.0/0" }],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let addresses = [client_address];
    let profile = validate_outbound(OutboundInput {
        private_key: &client_private,
        addresses: &addresses,
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    let mut client = PeerTunnel::from_validated(&profile, 0).unwrap();
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    socket
        .connect((Ipv4Addr::LOCALHOST, first_port))
        .await
        .unwrap();

    let source = if ipv6 {
        IpAddr::V6("fd00::2".parse().unwrap())
    } else {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))
    };
    let mut message = [0_u8; 17];
    message[0] = if ipv6 { 128 } else { 8 };
    message[4..6].copy_from_slice(&0x1234_u16.to_be_bytes());
    message[6..8].copy_from_slice(&1_u16.to_be_bytes());
    message[8..].copy_from_slice(b"two-hops!");
    let udp_payload = if large {
        vec![0x5a; 1_300]
    } else {
        b"two-hops!".to_vec()
    };
    let request = if tcp {
        packet::build_tcp_with_mss(source, target, 23_456, echo_port, 1_000, 0, 0x02, 1_200)
    } else if udp {
        packet::build_udp(source, target, 23_456, echo_port, &udp_payload)
    } else {
        packet::build_icmp_echo_tunnel_probe(
            &packet::IcmpEchoRequest {
                source,
                destination: target,
                message: &message,
            },
            0x1234,
            source,
            1_420,
        )
        .unwrap()
    };

    send_inner(&mut client, &socket, &request).await;
    let response = timeout(
        Duration::from_secs(15),
        receive_inner(&mut client, &socket, target),
    )
    .await
    .expect("routed packet timed out");
    if tcp {
        let syn_ack = packet::parse_tcp(&response).expect("valid routed TCP SYN-ACK");
        assert!(syn_ack.syn && syn_ack.ack_flag && !syn_ack.rst);
        assert_eq!(syn_ack.ack, 1_001);
        assert_eq!(syn_ack.src.ip, target);
        assert_eq!(syn_ack.dst.ip, source);
        let acknowledgement = syn_ack.seq.wrapping_add(1);
        send_inner(
            &mut client,
            &socket,
            &packet::build_tcp(
                source,
                target,
                23_456,
                echo_port,
                1_001,
                acknowledgement,
                0x10,
                &[],
            ),
        )
        .await;
        send_inner(
            &mut client,
            &socket,
            &packet::build_tcp(
                source,
                target,
                23_456,
                echo_port,
                1_001,
                acknowledgement,
                0x18,
                b"two-hops!",
            ),
        )
        .await;
        let reply = timeout(Duration::from_secs(15), async {
            loop {
                let packet = receive_inner(&mut client, &socket, target).await;
                let Some(reply) = packet::parse_tcp(&packet) else {
                    continue;
                };
                if !reply.payload.is_empty() {
                    assert_eq!(reply.src.ip, target);
                    assert_eq!(reply.dst.ip, source);
                    assert_eq!(reply.payload, b"two-hops!");
                    break;
                }
            }
        })
        .await;
        reply.expect("routed TCP echo timed out");
        tcp_task.unwrap().await.unwrap();
    } else if udp {
        let reply = packet::parse_udp(&response).expect("valid routed UDP packet");
        assert_eq!(reply.src.ip, target);
        assert_eq!(reply.dst.ip, source);
        assert_eq!(reply.src.port, echo_port);
        assert_eq!(reply.dst.port, 23_456);
        assert_eq!(reply.payload, udp_payload);
        echo_task.unwrap().await.unwrap();
    } else {
        let reply = packet::parse_icmp_echo_reply(&response).expect("valid routed ICMP echo reply");
        assert_eq!(reply.source, target);
        assert_eq!(reply.destination, source);
        assert_eq!(&reply.message[4..], &message[4..]);
    }

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

async fn send_inner(client: &mut PeerTunnel, socket: &UdpSocket, packet: &[u8]) {
    for action in client.send_ip_packet(packet).unwrap() {
        if let TunnelAction::SendNetwork(datagram) = action {
            socket.send(&datagram).await.unwrap();
        }
    }
}

async fn receive_inner(client: &mut PeerTunnel, socket: &UdpSocket, source: IpAddr) -> Vec<u8> {
    let mut buffer = [0_u8; 2048];
    loop {
        let size = socket.recv(&mut buffer).await.unwrap();
        for action in client
            .receive_datagram(Some(source), &buffer[..size])
            .unwrap()
        {
            match action {
                TunnelAction::SendNetwork(datagram) => {
                    socket.send(&datagram).await.unwrap();
                }
                TunnelAction::ReceiveIp { packet, .. } => return packet,
            }
        }
    }
}

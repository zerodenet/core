#![cfg(feature = "wireguard")]

use crate::support;

use std::net::{IpAddr, Ipv4Addr};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use tokio::{
    net::UdpSocket,
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

fn peer_tunnel(private: u8, peer_public: &str, address: &str, allowed: &str) -> PeerTunnel {
    let private_key = STANDARD.encode([private; 32]);
    let addresses = [address];
    let allowed_ips = [allowed];
    let peers = [PeerInput {
        public_key: peer_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &allowed_ips,
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &private_key,
        addresses: &addresses,
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    PeerTunnel::from_validated(&profile, 0).unwrap()
}

fn timestamp_request(source: Ipv4Addr, destination: Ipv4Addr) -> Vec<u8> {
    let mut packet = vec![0_u8; 40];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&40_u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = packet::IPPROTO_ICMP;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet[20] = 13;
    packet[24..26].copy_from_slice(&0x1234_u16.to_be_bytes());
    packet[26..28].copy_from_slice(&1_u16.to_be_bytes());
    let icmp_checksum = packet::checksum(&packet[20..]);
    packet[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = packet::checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
    packet
}

#[tokio::test]
async fn authenticated_non_echo_icmp_keeps_packet_plane_through_wireguard() {
    let source = Ipv4Addr::new(10, 0, 0, 2);
    let destination = Ipv4Addr::new(198, 51, 100, 1);
    let first_port = free_udp_port();
    let second_port = free_udp_port();
    let remote_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let remote_port = remote_socket.local_addr().unwrap().port();
    let first_public = public_key(81);
    let client_public = public_key(82);
    let hop_public = public_key(83);
    let remote_public = public_key(84);
    let mut remote = peer_tunnel(84, &hop_public, "198.51.100.1/32", "10.0.0.2/32");
    let mut remote_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        loop {
            let (length, sender) = remote_socket.recv_from(&mut buffer).await.unwrap();
            let Ok(actions) = remote.receive_datagram(Some(sender), &buffer[..length]) else {
                continue;
            };
            for action in actions {
                match action {
                    TunnelAction::SendNetwork(datagram) => {
                        remote_socket.send_to(&datagram, sender).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => return packet,
                }
            }
        }
    });

    let client_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let client_port = client_socket.local_addr().unwrap().port();
    let config = RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[
            {"tag":"wg-in","directions":{"inbound":true,"outbound":false},"listen":{"address":"127.0.0.1","port":first_port},
                "protocol":{"type":"wireguard","private_key":STANDARD.encode([81_u8;32]),"addresses":["10.8.0.1/32"],"peers":[{"public_key":client_public,"endpoint":format!("127.0.0.1:{client_port}"),"allowed_ips":["10.0.0.2/32"]}]}},
            {"tag":"wg-out","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":second_port},"protocol":{"type":"wireguard","private_key":STANDARD.encode([83_u8;32]),"addresses":["10.9.0.2/32"],"peers":[{"public_key":remote_public,"endpoint":format!("127.0.0.1:{remote_port}"),"allowed_ips":["198.51.100.1/32"]}]}}
        ],
        "route":{"rules":[],"final":{"type":"route","outbound":"wg-out"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let zero = spawn_engine(proxy.clone());
    sleep(Duration::from_millis(100)).await;

    let mut client = peer_tunnel(82, &first_public, "10.0.0.2/32", "198.51.100.1/32");
    let socket = client_socket;
    socket
        .connect((Ipv4Addr::LOCALHOST, first_port))
        .await
        .unwrap();
    let request = timestamp_request(source, destination);
    for action in client.send_ip_packet(&request).unwrap() {
        if let TunnelAction::SendNetwork(datagram) = action {
            socket.send(&datagram).await.unwrap();
        }
    }

    let forwarded = timeout(Duration::from_secs(15), async {
        let mut buffer = [0_u8; 2048];
        loop {
            tokio::select! {
                delivered = &mut remote_task => break delivered.unwrap(),
                length = socket.recv(&mut buffer) => {
                    let length = length.unwrap();
                    for action in client
                        .receive_datagram(Some(std::net::SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)), &buffer[..length])
                        .unwrap()
                    {
                        if let TunnelAction::SendNetwork(datagram) = action {
                            socket.send(&datagram).await.unwrap();
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("non-Echo ICMP packet did not reach the WireGuard peer");
    assert_eq!(packet::ip_source(&forwarded), Some(IpAddr::V4(source)));
    assert_eq!(
        packet::ip_destination(&forwarded),
        Some(IpAddr::V4(destination))
    );
    assert_eq!(packet::ip_protocol(&forwarded), Some(packet::IPPROTO_ICMP));
    assert_eq!(forwarded[8], 63, "packet route must decrement IPv4 TTL");
    assert_eq!(&forwarded[20..], &request[20..]);
    use zero_api::{TrafficGetQuery, TrafficScope};
    let measure = |tag: &str| {
        proxy
            .engine()
            .traffic_snapshot(&TrafficGetQuery {
                scope: TrafficScope::Endpoint {
                    endpoint_id: format!("endpoint:{tag}"),
                },
            })
            .unwrap()
    };
    let ingress = measure("wg-in");
    let egress = measure("wg-out");
    assert_eq!(
        ingress.planes[1].counters.rx_bytes,
        Some(request.len() as u64)
    );
    assert_eq!(
        egress.planes[1].counters.tx_bytes,
        Some(request.len() as u64)
    );
    assert_eq!(egress.activity.active_packet_routes, Some(1));
    assert_eq!(
        proxy.stats_snapshot().bytes_up,
        0,
        "native packets do not pretend to be Flow usage"
    );
    assert!(egress.planes[2].counters.tx_bytes.unwrap() > request.len() as u64);
    socket.send(b"invalid-wireguard-datagram").await.unwrap();
    support::wait_for("unidentified carrier rejection is observed", || {
        measure("wg-in").planes[2].counters.dropped_packets == Some(1)
    })
    .await;
    let peer = proxy
        .engine()
        .traffic_snapshot(&TrafficGetQuery {
            scope: TrafficScope::Peer {
                endpoint_id: "endpoint:wg-in".into(),
                peer_id: format!("wireguard:{}", public_key(82)),
            },
        })
        .unwrap();
    assert_eq!(peer.planes[2].counters.dropped_packets, Some(0));
    zero.shutdown().await.unwrap();
}

#[tokio::test]
async fn linked_endpoint_forwards_packet_between_peers_on_one_socket() {
    let source = Ipv4Addr::new(10, 0, 0, 2);
    let destination = Ipv4Addr::new(198, 51, 100, 1);
    let zero_port = free_udp_port();
    let client_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let client_port = client_socket.local_addr().unwrap().port();
    client_socket
        .connect((Ipv4Addr::LOCALHOST, zero_port))
        .await
        .unwrap();
    let remote_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let remote_port = remote_socket.local_addr().unwrap().port();
    let zero_public = public_key(91);
    let client_public = public_key(92);
    let remote_public = public_key(93);
    let mut remote = peer_tunnel(93, &zero_public, "198.51.100.1/32", "10.0.0.2/32");
    let mut remote_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        loop {
            let (length, sender) = remote_socket.recv_from(&mut buffer).await.unwrap();
            for action in remote
                .receive_datagram(Some(sender), &buffer[..length])
                .unwrap()
            {
                match action {
                    TunnelAction::SendNetwork(datagram) => {
                        remote_socket.send_to(&datagram, sender).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => return packet,
                }
            }
        }
    });
    let zero_private = STANDARD.encode([91_u8; 32]);
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [{"tag":"wg-in", "listen":{"address":"127.0.0.1", "port":zero_port},
                "protocol":{"type":"wireguard", "private_key":zero_private,
                    "peers":[
                        {"public_key":client_public, "allowed_ips":["10.0.0.2/32"]},
                        {"public_key":remote_public, "allowed_ips":["198.51.100.1/32"]}
                    ]}}],
            "outbounds": [{"tag":"wg-out", "protocol":{"type":"wireguard",
                "private_key":zero_private, "addresses":["10.9.0.2/32"],
                "inbound_tag":"wg-in", "peers":[
                    {"public_key":client_public, "endpoint":format!("127.0.0.1:{client_port}"),
                        "allowed_ips":["10.0.0.2/32"]},
                    {"public_key":remote_public, "endpoint":format!("127.0.0.1:{remote_port}"),
                        "allowed_ips":["198.51.100.1/32"]}
                ]}}],
            "route":{"rules":[], "final":{"type":"route", "outbound":"wg-out"}}
        })
        .to_string(),
    )
    .unwrap();
    let zero = spawn_engine(Proxy::new(config).unwrap());
    sleep(Duration::from_millis(100)).await;
    let mut client = peer_tunnel(92, &zero_public, "10.0.0.2/32", "198.51.100.1/32");
    let request = timestamp_request(source, destination);
    for action in client.send_ip_packet(&request).unwrap() {
        if let TunnelAction::SendNetwork(datagram) = action {
            client_socket.send(&datagram).await.unwrap();
        }
    }
    let forwarded = timeout(Duration::from_secs(15), async {
        let mut buffer = [0_u8; 2048];
        loop {
            tokio::select! {
                delivered = &mut remote_task => break delivered.unwrap(),
                length = client_socket.recv(&mut buffer) => {
                    let length = length.unwrap();
                    let Ok(actions) = client.receive_datagram(Some(std::net::SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)), &buffer[..length]) else {
                        continue;
                    };
                    for action in actions {
                        if let TunnelAction::SendNetwork(datagram) = action {
                            client_socket.send(&datagram).await.unwrap();
                        }
                    }
                }
            }
        }
    }).await.expect("linked endpoint did not forward packet between peers");
    assert_eq!(packet::ip_source(&forwarded), Some(IpAddr::V4(source)));
    assert_eq!(
        packet::ip_destination(&forwarded),
        Some(IpAddr::V4(destination))
    );
    assert_eq!(forwarded[8], 63);
    assert_eq!(&forwarded[20..], &request[20..]);
    zero.shutdown().await.unwrap();
}

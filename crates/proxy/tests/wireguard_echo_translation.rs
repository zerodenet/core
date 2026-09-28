#![cfg(feature = "wireguard")]

#[path = "support/echo.rs"]
mod echo;
mod support;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::{IpAddr, Ipv4Addr};
use support::{free_udp_port, spawn_engine};
use tokio::{
    net::UdpSocket,
    time::{sleep, timeout, Duration},
};
use wireguard::{
    runtime::{PeerTunnel, PreparedInbound, TunnelAction},
    validation::{validate_outbound, InboundInput, InboundPeerInput, OutboundInput, PeerInput},
};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
use zero_stack::packet;

fn public_key(private: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([private; 32])).as_bytes())
}

#[tokio::test]
async fn ipv4_ping_translation_uses_assigned_address_and_recovers_after_network_change() {
    roundtrip(false).await;
}

#[tokio::test]
async fn ipv6_ping_translation_restores_echo_and_quoted_errors_over_authenticated_wireguard() {
    roundtrip(true).await;
}

async fn roundtrip(v6: bool) {
    let source = if v6 { "fd00::2" } else { "10.0.0.2" };
    let assigned = if v6 { "fd10::11" } else { "10.10.0.11" };
    let target = if v6 { "fd20::235" } else { "192.168.1.235" };
    let source: IpAddr = source.parse().unwrap();
    let assigned: IpAddr = assigned.parse().unwrap();
    let target: IpAddr = target.parse().unwrap();
    let bits = if v6 { 128 } else { 32 };
    let listen_port = free_udp_port();
    let remote = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let endpoint = remote.local_addr().unwrap();
    let remote_private = STANDARD.encode([21; 32]);
    let outbound_public = public_key(22);
    let allowed = format!("{assigned}/{bits}");
    let inbound_peers = [InboundPeerInput {
        public_key: &outbound_public,
        pre_shared_key: None,
        allowed_ips: &[&allowed],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let mut remote_device = PreparedInbound::from_input(InboundInput {
        private_key: &remote_private,
        mtu: 1420,
        peers: &inbound_peers,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let (observed, mut requests) = tokio::sync::mpsc::channel(4);
    let responder = tokio::spawn(async move {
        let mut wire = [0; 2048];
        loop {
            let (size, sender) = remote.recv_from(&mut wire).await.unwrap();
            let received = remote_device
                .receive_datagram(sender, &wire[..size])
                .unwrap();
            for action in received.actions {
                match action {
                    TunnelAction::SendNetwork(wire) => {
                        remote.send_to(&wire, sender).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, source } => {
                        assert_eq!(
                            source, assigned,
                            "remote AllowedIPs admits only the assigned address"
                        );
                        let request = packet::parse_icmp_echo_request(&packet).unwrap();
                        let sequence = u16::from_be_bytes([request.message[6], request.message[7]]);
                        observed.send(packet.clone()).await.unwrap();
                        let reply = if sequence == 3 {
                            packet::build_icmp_time_exceeded_response(&packet, target, 1420)
                                .unwrap()
                        } else {
                            echo::reply(&packet)
                        };
                        for action in remote_device
                            .send_ip_packet(received.peer_index.unwrap(), &reply)
                            .unwrap()
                        {
                            if let TunnelAction::SendNetwork(wire) = action {
                                remote.send_to(&wire, sender).await.unwrap();
                            }
                        }
                    }
                }
            }
        }
    });
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"packet-in", "listen":{"address":"127.0.0.1","port":listen_port},
            "protocol":{"type":"wireguard", "private_key":STANDARD.encode([23;32]),
                "peers":[{"public_key":public_key(24), "allowed_ips":[format!("{source}/{bits}")]}]}}],
        "outbounds":[{"tag":"packet-out", "protocol":{"type":"wireguard",
            "private_key":STANDARD.encode([22;32]), "addresses":[format!("{assigned}/{bits}")],
            "peers":[{"public_key":public_key(21), "endpoint":endpoint.to_string(),
                "allowed_ips":[format!("{target}/{bits}")]}]}}],
        "route":{"rules":[], "final":{"type":"route","outbound":"packet-out"}, "final_mode":"translate"}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let egress = proxy.egress_interface_control();
    let proxy = spawn_engine(proxy);
    sleep(Duration::from_millis(100)).await;
    let client_private = STANDARD.encode([24; 32]);
    let inbound_public = public_key(23);
    let address = format!("{source}/{bits}");
    let addresses = [address.as_str()];
    let peers = [PeerInput {
        public_key: &inbound_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &[if v6 { "::/0" } else { "0.0.0.0/0" }],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &client_private,
        addresses: &addresses,
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let mut client = PeerTunnel::from_validated(&profile, 0).unwrap();
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    socket
        .connect((Ipv4Addr::LOCALHOST, listen_port))
        .await
        .unwrap();
    for sequence in 1..=3 {
        if sequence == 2 {
            egress.invalidate_network();
            sleep(Duration::from_millis(150)).await;
        }
        let original = echo::request(source, target, 1234, sequence, b"native-app-ping");
        for action in client.send_ip_packet(&original).unwrap() {
            if let TunnelAction::SendNetwork(wire) = action {
                socket.send(&wire).await.unwrap();
            }
        }
        let response = timeout(Duration::from_secs(10), receive_inner(&mut client, &socket))
            .await
            .unwrap();
        let sent = timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet::ip_source(&sent), Some(assigned));
        assert_eq!(packet::ip_hop_limit(&sent), Some(63));
        assert_eq!(packet::ip_destination(&response), Some(source));
        if sequence == 3 {
            let offset = if v6 { 40 } else { 20 };
            let mut advanced = original.clone();
            assert!(packet::advance_ip_hop(&mut advanced));
            assert_eq!(&response[offset + 8..], &advanced);
        } else {
            let reply = packet::parse_icmp_echo_reply(&response).unwrap();
            assert_eq!(reply.identifier, 1234);
            assert_eq!(&reply.message[6..8], &sequence.to_be_bytes());
            assert_eq!(&reply.message[8..], b"native-app-ping");
        }
    }
    proxy.shutdown().await.unwrap();
    responder.abort();
}

async fn receive_inner(client: &mut PeerTunnel, socket: &UdpSocket) -> Vec<u8> {
    let mut wire = [0; 2048];
    loop {
        let size = socket.recv(&mut wire).await.unwrap();
        let sender = socket.peer_addr().unwrap();
        for action in client
            .receive_datagram(Some(sender), &wire[..size])
            .unwrap()
        {
            match action {
                TunnelAction::SendNetwork(wire) => {
                    socket.send(&wire).await.unwrap();
                }
                TunnelAction::ReceiveIp { packet, .. } => return packet,
            }
        }
    }
}

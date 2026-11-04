//! Real authenticated Packet ingress over its original UDP, without a privileged
//! ICMP socket: the post-commit response is the existing local Echo handler.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};

fn public_key(private: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([private; 32])).as_bytes())
}
fn echo(sequence: u16, payload: &[u8]) -> Vec<u8> {
    let mut packet = vec![0; 28 + payload.len()];
    packet[0] = 0x45;
    let length = packet.len() as u16;
    packet[2..4].copy_from_slice(&length.to_be_bytes());
    packet[8] = 64;
    packet[9] = 1;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&[10, 0, 0, 1]);
    packet[20] = 8;
    packet[24..26].copy_from_slice(&0x1234_u16.to_be_bytes());
    packet[26..28].copy_from_slice(&sequence.to_be_bytes());
    packet[28..].copy_from_slice(payload);
    let sum = zero_stack::packet::checksum(&packet[20..]);
    packet[22..24].copy_from_slice(&sum.to_be_bytes());
    let sum = zero_stack::packet::checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&sum.to_be_bytes());
    packet
}
fn drops(proxy: &Proxy) -> u64 {
    proxy
        .engine()
        .traffic_snapshot(&zero_api::TrafficGetQuery {
            scope: zero_api::TrafficScope::Global,
        })
        .unwrap()
        .planes
        .iter()
        .filter(|plane| plane.plane == zero_api::TrafficPlane::Inner)
        .flat_map(|plane| &plane.drop_reasons)
        .filter(|drop| drop.reason == zero_api::TrafficDropReason::PolicyRejected)
        .map(|drop| drop.packets)
        .sum()
}
async fn send(tunnel: &mut PeerTunnel, socket: &UdpSocket, packet: &[u8]) {
    for action in tunnel.send_ip_packet(packet).unwrap() {
        if let TunnelAction::SendNetwork(bytes) = action {
            socket.send(&bytes).await.unwrap();
        }
    }
}
async fn receive(tunnel: &mut PeerTunnel, socket: &UdpSocket) -> Option<Vec<u8>> {
    let mut bytes = [0; 2048];
    let size = socket.recv(&mut bytes).await.unwrap();
    let mut packet = None;
    for action in tunnel
        .receive_datagram(
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            &bytes[..size],
        )
        .unwrap()
    {
        match action {
            TunnelAction::SendNetwork(bytes) => {
                socket.send(&bytes).await.unwrap();
            }
            TunnelAction::ReceiveIp {
                packet: received, ..
            } => packet = Some(received.into_vec()),
        }
    }
    packet
}
#[tokio::test]
async fn actual_packet_source_ready_candidate_drops_business_until_same_snapshot_commit() {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (healthy, reserved) = direct("healthy", "127.0.0.1", target.local_addr().unwrap());
    let proxy = Proxy::new(config(serde_json::json!([healthy.clone()]))).unwrap();
    drop(reserved);
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let wg = serde_json::json!({"tag":"packet-candidate","listen":{"address":"127.0.0.3","port":0},
        "protocol":{"type":"wireguard","private_key":STANDARD.encode([71;32]),"addresses":["10.0.0.1/32"],
            "peers":[{"public_key":public_key(72),"allowed_ips":["10.0.0.2/32"]}]}});
    let snapshot = pending(&proxy, &mut state, config(serde_json::json!([healthy, wg]))).await;
    let owner = proxy.inbound_owners.owner("packet-candidate").unwrap();
    assert!(owner.is_ready());
    let address = *owner.original_listener_address.get().unwrap();
    let private = STANDARD.encode([72; 32]);
    let public = public_key(71);
    let endpoint = address.to_string();
    let peers = [PeerInput {
        public_key: &public,
        pre_shared_key: None,
        endpoint: &endpoint,
        allowed_ips: &["10.0.0.1/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &private,
        addresses: &["10.0.0.2/32"],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let mut tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.connect(address).await.unwrap();
    let before = drops(&proxy);
    send(&mut tunnel, &socket, &echo(1, b"before")).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while drops(&proxy) == before {
            tokio::select! {
                result = receive(&mut tunnel, &socket) => assert!(result.is_none(), "uncommitted Packet candidate returned business data"),
                _ = tokio::task::yield_now() => {}
            }
        }
    }).await.unwrap();
    let mut bytes = [0; 2048];
    assert_pending(socket.recv(&mut bytes)).await;
    assert!(proxy.engine().completed_sessions().is_empty());
    commit(&proxy, snapshot).await;
    send(&mut tunnel, &socket, &echo(2, b"after")).await;
    let reply = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(packet) = receive(&mut tunnel, &socket).await {
                break packet;
            }
        }
    })
    .await
    .unwrap();
    let reply = zero_stack::packet::parse_icmp_echo_reply(&reply).unwrap();
    assert_eq!(&reply.message[8..], b"after");
    state.finish(&proxy, Ok(())).await.unwrap();
    let terminal = owner
        .terminal_for_test()
        .expect("same actual Packet source physically closed");
    assert_eq!(terminal.identity(), owner.identity());
    assert!(terminal.closed_io_receipts() > 0);
    assert!(terminal.joined_tasks() > 0);
}

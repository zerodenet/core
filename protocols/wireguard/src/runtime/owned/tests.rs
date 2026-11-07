use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::Ipv4Addr;
use zero_tun::packet_memory::{NativeReadGrant, PacketMemoryLimit, PacketReturnTotals};

fn tunnel(private: u8, remote: u8, address: &str) -> PeerTunnel {
    let key = STANDARD.encode([private; 32]);
    let public = STANDARD.encode(PublicKey::from(&StaticSecret::from([remote; 32])).as_bytes());
    let peers = [crate::validation::PeerInput {
        public_key: &public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = crate::validation::validate_outbound(crate::validation::OutboundInput {
        private_key: &key,
        addresses: &[address],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    PeerTunnel::from_validated(&profile, 0).unwrap()
}
fn root(frames: usize, bytes: usize) -> Arc<PacketReturnTotals> {
    Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 2,
            bytes: 512,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 512,
        },
        PacketMemoryLimit { frames, bytes },
    ))
}
fn received(read: &NativeReadGrant, bytes: &[u8]) -> PacketBuffer {
    let mut buffer = read.try_buffer().unwrap();
    buffer.as_mut_bytes()[..bytes.len()].copy_from_slice(bytes);
    buffer
        .into_packet(bytes.len())
        .unwrap_or_else(|_| panic!("actual wire packet fits read"))
}
fn networks(actions: Vec<TunnelAction>) -> Vec<PacketBuffer> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            TunnelAction::SendNetwork(packet) => Some(packet),
            TunnelAction::ReceiveIp { .. } => None,
        })
        .collect()
}
fn ip() -> Vec<u8> {
    let mut bytes = vec![0; 20];
    bytes[0] = 0x45;
    bytes[2..4].copy_from_slice(&20_u16.to_be_bytes());
    bytes[8] = 64;
    bytes[9] = 1;
    bytes[12..16].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 1).octets());
    bytes[16..20].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 2).octets());
    bytes
}
fn source() -> Option<SocketAddr> {
    Some("127.0.0.1:51820".parse().unwrap())
}

#[test]
fn queued_owned_data_at_exact_tx_limit_does_not_block_real_handshake_or_read_promise() {
    let mut client = tunnel(1, 2, "10.0.0.1/32");
    let mut server = tunnel(2, 1, "10.0.0.2/32");
    // Actual original20 + padded32 + cipher64 + original control148.
    let root = root(4, 264);
    let work = root.datagram_workspaces([256; 2], Some(148)).unwrap();
    let control = work.control.as_ref().unwrap();
    let payload = ip();
    let mut packet = root.transmit_packet(payload.len(), 2, 96).unwrap();
    packet.copy_from_slice(&payload);
    let output = client.send_ip_packet_owned(packet, received(control, &[0; 148]));
    assert!(output.unused_control.is_none());
    let initiation = networks(output.result.unwrap());
    assert!(
        client.has_pending_packets(),
        "real SDK retains the submitted plaintext"
    );
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 4,
            bytes: 264
        }
    );
    assert!(root.reserve_transmit(&[1]).is_err());
    let reply = networks(server.receive_datagram(source(), &initiation[0]).unwrap());
    drop(initiation); // Actual network-send custody ended; same control can return.
    assert!(control.workspace_returned());
    let incoming = received(&work.reads[0], &reply[0]);
    let output = client.receive_datagram_owned(
        source(),
        incoming,
        work.reads[1].try_buffer().unwrap(),
        received(control, &[0; 148]),
    );
    assert!(client.time_since_last_handshake().is_some());
    let mut delivered = Vec::new();
    for packet in networks(output.result.unwrap().actions) {
        delivered.extend(server.receive_datagram(source(), &packet).unwrap());
    }
    assert!(delivered.iter().any(|action| matches!(action,
        TunnelAction::ReceiveIp { packet, .. } if packet.as_ref() == payload.as_slice())));
    drop(output.unused_control);
    drop(delivered);
    assert!(!client.has_pending_packets());
    assert!(work.reads[0].workspace_returned());
    assert!(control.workspace_returned());
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 148
        }
    );
}

#[test]
fn tx_full_rejects_only_unsubmitted_data_and_retry_preserves_real_replay_acceptance() {
    let mut client = tunnel(11, 12, "10.0.0.1/32");
    let mut server = tunnel(12, 11, "10.0.0.2/32");
    let initiation = networks(client.initiate_handshake().unwrap());
    let response = networks(server.receive_datagram(source(), &initiation[0]).unwrap());
    for packet in networks(client.receive_datagram(source(), &response[0]).unwrap()) {
        server.receive_datagram(source(), &packet).unwrap();
    }
    let encrypted = networks(server.send_ip_packet(&ip()).unwrap());
    let root = root(4, 660);
    let work = root.datagram_workspaces([256; 2], Some(148)).unwrap();
    let control = work.control.as_ref().unwrap();
    let occupied = root.reserve_transmit(&[512]).unwrap();
    let rejected = client.receive_datagram_owned(
        source(),
        received(&work.reads[0], &encrypted[0]),
        work.reads[1].try_buffer().unwrap(),
        received(control, &[0; 148]),
    );
    let failure = match rejected.result {
        Err(failure) => failure,
        Ok(_) => panic!("full actual TX rejects before SDK submission"),
    };
    assert_eq!(failure.storage, Some(PacketDeriveError::Busy));
    assert_eq!(
        failure.original.as_ref().unwrap().as_ref(),
        encrypted[0].as_ref()
    );
    assert!(
        !work.reads[0].workspace_returned(),
        "rejection preserves the exact original until caller releases it"
    );
    drop(failure);
    drop(rejected.unused_control);
    assert!(work.reads[0].workspace_returned());
    drop(occupied);
    let accepted = client.receive_datagram_owned(
        source(),
        received(&work.reads[0], &encrypted[0]),
        work.reads[1].try_buffer().unwrap(),
        received(control, &[0; 148]),
    );
    let received = accepted.result.unwrap();
    assert!(
        received.authenticated,
        "capacity refusal never committed SDK replay state"
    );
    assert!(received.actions.iter().any(|action| matches!(action,
        TunnelAction::ReceiveIp { packet, .. } if packet.as_ref() == ip().as_slice())));
    assert!(
        work.reads[0].workspace_returned(),
        "actual Data moved into TX and replenished the same read promise"
    );
    drop(received);
    drop(accepted.unused_control);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 148
        }
    );
}

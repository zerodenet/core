use super::*;
use crate::validation::{validate_outbound, OutboundInput, PeerInput};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};

fn tunnel(private: u8, remote: u8) -> PeerTunnel {
    let key = STANDARD.encode([private; 32]);
    let remote = STANDARD.encode(PublicKey::from(&StaticSecret::from([remote; 32])).as_bytes());
    let peers = [PeerInput {
        public_key: &remote,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 20,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &key,
        addresses: &["10.0.0.1/32"],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    PeerTunnel::from_validated(&profile, 0).unwrap()
}

fn wire(actions: Vec<TunnelAction>) -> zero_traits::PacketBuffer {
    match actions.into_iter().next().unwrap() {
        TunnelAction::SendNetwork(packet) => packet,
        _ => panic!("expected protocol network packet"),
    }
}

#[test]
fn repeated_expired_timer_results_allow_handshake_and_ip_exchange() {
    let mut client = tunnel(11, 22);
    let mut server = tunnel(22, 11);
    assert!(client.timer_enabled());
    // Inject the exact timer result without waiting 90/540 wall-clock seconds.
    for _ in 0..2320 {
        assert!(client
            .collect_timer_result(Err(WireGuardError::ConnectionExpired))
            .unwrap()
            .is_empty());
    }
    assert!(client.time_since_last_handshake().is_none());
    let source = Some("127.0.0.1:51820".parse().unwrap());
    let initiation = wire(client.initiate_handshake().unwrap());
    let response = wire(server.receive_datagram(source, &initiation).unwrap());
    let keepalive = wire(client.receive_datagram(source, &response).unwrap());
    assert!(server
        .receive_datagram(source, &keepalive)
        .unwrap()
        .is_empty());
    assert!(client.time_since_last_handshake().is_some());
    assert!(client.timer_enabled());
    assert!(server.timer_enabled());
    let mut packet = vec![0; 20];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&20_u16.to_be_bytes());
    packet[8] = 64;
    packet[12..16].copy_from_slice(&[10, 0, 0, 1]);
    packet[16..20].copy_from_slice(&[10, 0, 0, 2]);
    let encrypted = wire(client.send_ip_packet(&packet).unwrap());
    let recovered = server.receive_datagram(source, &encrypted).unwrap();
    assert!(
        matches!(recovered.as_slice(), [TunnelAction::ReceiveIp { packet: actual, .. }] if actual.as_ref() == packet)
    );
}

#[test]
#[ignore = "uses the pinned engine's real 90-second handshake expiration clock"]
fn actual_engine_expiration_parks_until_authenticated_input_or_outgoing_demand() {
    let mut client = tunnel(11, 22);
    let mut server = tunnel(22, 11);
    client.initiate_handshake().unwrap();
    server.initiate_handshake().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(91));
    assert!(client.tick().unwrap().is_empty());
    assert!(server.tick().unwrap().is_empty());
    assert!(!client.timer_enabled());
    assert!(!server.timer_enabled());
    assert!(server.receive_datagram(None, &[0; 16]).is_err());
    assert!(!server.timer_enabled());

    let initiation = wire(client.initiate_handshake().unwrap());
    assert!(client.timer_enabled());
    let source = Some("127.0.0.1:51820".parse().unwrap());
    let response = wire(server.receive_datagram(source, &initiation).unwrap());
    assert!(server.timer_enabled());
    let keepalive = wire(client.receive_datagram(source, &response).unwrap());
    server.receive_datagram(source, &keepalive).unwrap();
    assert!(client.time_since_last_handshake().is_some());
    assert!(server.time_since_last_handshake().is_some());
}

#[test]
fn unexpected_timer_failures_still_propagate_and_wire_errors_remain_rejected() {
    let mut peer = tunnel(11, 22);
    for error in [
        WireGuardError::LockFailed,
        WireGuardError::NoCurrentSession,
        WireGuardError::InvalidPacket,
    ] {
        assert_eq!(
            peer.collect_timer_result(Err(error)),
            Err(TunnelError::Engine)
        );
    }
    assert!(peer.receive_datagram(None, &[0; 16]).is_err());
    assert!(peer.collect_timer_result(Ok(None)).unwrap().is_empty());
    // A fresh peer may emit a handshake for its configured persistent keepalive.
    assert!(peer
        .tick()
        .unwrap()
        .iter()
        .all(|action| matches!(action, TunnelAction::SendNetwork(_))));
}

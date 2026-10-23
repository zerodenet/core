//! Factual continuity comes from the original authenticated Noise state.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use wireguard::{
    runtime::PeerTunnel,
    validation::{validate_outbound, OutboundInput, PeerInput},
};

fn network(actions: Vec<TunnelAction>) -> Vec<Vec<u8>> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            TunnelAction::SendNetwork(packet) => Some(packet.into_vec()),
            _ => None,
        })
        .collect()
}
fn ip(payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0; 20 + payload.len()];
    bytes[0] = 0x45;
    let size = bytes.len() as u16;
    bytes[2..4].copy_from_slice(&size.to_be_bytes());
    bytes[8] = 64;
    bytes[9] = 17;
    bytes[12..16].copy_from_slice(&[10, 0, 0, 2]);
    bytes[16..20].copy_from_slice(&[10, 0, 0, 1]);
    bytes[20..].copy_from_slice(payload);
    bytes
}

#[test]
fn same_actual_wireguard_device_preserves_authenticated_roaming_and_rejects_old_ciphertext() {
    let server_key = STANDARD.encode([91; 32]);
    let client_key = STANDARD.encode([92; 32]);
    let server_public = STANDARD.encode(PublicKey::from(&StaticSecret::from([91; 32])).as_bytes());
    let client_public = STANDARD.encode(PublicKey::from(&StaticSecret::from([92; 32])).as_bytes());
    let inbound: InboundConfig = serde_json::from_value(serde_json::json!({
        "tag":"wg", "listen":{"address":"127.0.0.1","port":51820},
        "protocol":{"type":"wireguard","private_key":server_key,"addresses":["10.0.0.1/32"],
            "mtu":1420,"peers":[{"public_key":client_public,"allowed_ips":["10.0.0.2/32"]}]}
    }))
    .unwrap();
    let live = Arc::new(LiveInboundDevice::new(
        prepare_device(&inbound).unwrap(),
        inbound_protocol_identity(&inbound.protocol).unwrap(),
    ));
    let mut original = WireguardInboundDevice(live.clone());
    assert_eq!(original.authenticated_endpoint(0), None);
    let peers = [PeerInput {
        public_key: &server_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let validated = validate_outbound(OutboundInput {
        private_key: &client_key,
        addresses: &["10.0.0.2/32"],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let mut client = PeerTunnel::from_validated(&validated, 0).unwrap();
    let learned: SocketAddr = "127.0.0.1:40123".parse().unwrap();
    let initiation = network(client.send_ip_packet(&ip(b"original")).unwrap());
    let response = original
        .receive_datagram(Some(learned), &initiation[0])
        .unwrap();
    let response = response
        .actions
        .into_iter()
        .find_map(|action| match action {
            RawIpInboundAction::SendNetwork(packet) => Some(packet.into_vec()),
            _ => None,
        })
        .unwrap();
    let queued = network(client.receive_datagram(None, &response).unwrap());
    let mut accepted = None;
    for ciphertext in queued {
        let dispatch = original
            .receive_datagram(Some(learned), &ciphertext)
            .unwrap();
        if dispatch
            .actions
            .iter()
            .any(|action| matches!(action, RawIpInboundAction::ReceiveIp(_)))
        {
            accepted = Some(ciphertext);
        }
    }
    let accepted = accepted.expect("actual encrypted payload must be delivered before handoff");
    assert_eq!(original.authenticated_endpoint(0), Some(learned));
    drop(original);
    let mut retained = WireguardInboundDevice(live.clone());
    assert!(Arc::ptr_eq(&retained.0, &live));
    assert_eq!(retained.authenticated_endpoint(0), Some(learned));
    let configured = [Some("127.0.0.1:51820".parse().unwrap())];
    assert_eq!(
        crate::runtime::inbound_operation::raw_ip::retained_endpoint_for_test(
            &retained,
            &configured,
            0
        ),
        Some(learned)
    );
    assert!(retained
        .receive_datagram(Some("127.0.0.1:49999".parse().unwrap()), &accepted)
        .is_err());
    assert_eq!(retained.authenticated_endpoint(0), Some(learned));
    let next = network(
        client
            .send_ip_packet(&ip(b"same keys next counter"))
            .unwrap(),
    );
    assert_eq!(&next[0][..4], &4_u32.to_le_bytes());
    let previous_counter = u64::from_le_bytes(accepted[8..16].try_into().unwrap());
    let next_counter = u64::from_le_bytes(next[0][8..16].try_into().unwrap());
    assert!(next_counter > previous_counter);
    let roamed: SocketAddr = "127.0.0.1:40124".parse().unwrap();
    let dispatch = retained.receive_datagram(Some(roamed), &next[0]).unwrap();
    assert!(dispatch.authenticated);
    assert!(dispatch
        .actions
        .iter()
        .any(|action| matches!(action, RawIpInboundAction::ReceiveIp(_))));
    assert_eq!(retained.authenticated_endpoint(0), Some(roamed));
}

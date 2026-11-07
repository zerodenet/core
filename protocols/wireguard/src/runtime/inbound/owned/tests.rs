use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_tun::packet_memory::{NativeReadGrant, PacketMemoryLimit, PacketReturnTotals};
fn key(value: u8) -> String {
    STANDARD.encode([value; 32])
}
fn public(value: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([value; 32])).as_bytes())
}
fn read(grant: &NativeReadGrant, bytes: &[u8]) -> PacketBuffer {
    let mut buffer = grant.try_buffer().unwrap();
    buffer.as_mut_bytes()[..bytes.len()].copy_from_slice(bytes);
    buffer
        .into_packet(bytes.len())
        .unwrap_or_else(|_| panic!("fits actual read"))
}
fn network(actions: Vec<TunnelAction>) -> Vec<PacketBuffer> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            TunnelAction::SendNetwork(packet) => Some(packet),
            _ => None,
        })
        .collect()
}
#[test]
fn fallback_candidates_reuse_one_actual_decode_allocation_under_exact_global_limit() {
    use crate::validation::{InboundInput, InboundPeerInput, OutboundInput, PeerInput};
    let secret = key(31);
    let client_public = public(32);
    let wrong_public = public(33);
    let peers = [
        InboundPeerInput {
            public_key: &wrong_public,
            pre_shared_key: None,
            allowed_ips: &["10.0.0.3/32"],
            keepalive_secs: 0,
            reserved: &[],
        },
        InboundPeerInput {
            public_key: &client_public,
            pre_shared_key: None,
            allowed_ips: &["10.0.0.2/32"],
            keepalive_secs: 0,
            reserved: &[],
        },
    ];
    let mut server = PreparedInbound::from_input(InboundInput {
        private_key: &secret,
        addresses: &["10.0.0.1/32"],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let secret = key(32);
    let server_public = public(31);
    let peers = [PeerInput {
        public_key: &server_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let client_profile = crate::runtime::PreparedOutbound::from_input(OutboundInput {
        private_key: &secret,
        addresses: &["10.0.0.2/32"],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let mut client = PeerTunnel::from_prepared(&client_profile, 0).unwrap();
    let roots = Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 2,
            bytes: 512,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 512,
        },
        PacketMemoryLimit {
            frames: 3,
            bytes: 660,
        },
    ));
    let work = roots.datagram_workspaces([256; 2], Some(148)).unwrap();
    let control = work.control.as_ref().unwrap();
    let source = Some("127.0.0.1:51820".parse().unwrap());
    let request = network(client.initiate_handshake().unwrap());
    let output = server.receive_datagram_owned(
        source,
        read(&work.reads[0], &request[0]),
        work.reads[1].try_buffer().unwrap(),
        &work.reads[1],
        read(control, &[0; 148]),
    );
    let response = network(output.result.unwrap().actions);
    drop(output.unused_control);
    for packet in network(client.receive_datagram(source, &response[0]).unwrap()) {
        server
            .receive_datagram_with_source(source, &packet)
            .unwrap();
    }
    drop(response);
    let mut payload = vec![0; 20];
    payload[0] = 0x45;
    payload[2..4].copy_from_slice(&20_u16.to_be_bytes());
    payload[8] = 64;
    payload[9] = 1;
    payload[12..16].copy_from_slice(&[10, 0, 0, 2]);
    payload[16..20].copy_from_slice(&[10, 0, 0, 1]);
    let wire = network(client.send_ip_packet(&payload).unwrap());
    // Exercise the existing bounded-index-history fallback without inventing
    // a second peer/session. Peer zero cannot authenticate this actual packet.
    server.local_indices.clear();
    server.index_order.clear();
    let output = server.receive_datagram_owned(
        source,
        read(&work.reads[0], &wire[0]),
        work.reads[1].try_buffer().unwrap(),
        &work.reads[1],
        read(control, &[0; 148]),
    );
    let dispatch = output.result.unwrap();
    assert_eq!(dispatch.peer_index, Some(1));
    assert!(dispatch.authenticated);
    assert!(dispatch
        .actions
        .iter()
        .any(|action| matches!(action,TunnelAction::ReceiveIp {packet,..}
        if packet.as_ref()==payload.as_slice())));
    assert_eq!(
        roots.transmit().occupied,
        PacketMemoryLimit {
            frames: 3,
            bytes: 660
        }
    );
    drop(dispatch);
    drop(output.unused_control);
    assert!(work.reads.iter().all(NativeReadGrant::workspace_returned));
    assert!(control.workspace_returned());
    assert_eq!(
        roots.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 148
        }
    );
}

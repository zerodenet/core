use super::{ipv4_packet, key, network_packets, public_key, tunnel};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use wireguard::{
    runtime::{InboundDevice, PeerTunnel, PreparedInbound, TunnelAction, TunnelError},
    validation::{InboundInput, InboundPeerInput},
};

fn establish(
    server: &mut InboundDevice,
    client: &mut PeerTunnel,
    index: usize,
    outer_source: SocketAddr,
    inner_source: Ipv4Addr,
) {
    let request = ipv4_packet(inner_source, Ipv4Addr::new(10, 0, 0, 1), b"first payload");
    let initiation = network_packets(client.send_ip_packet(&request).unwrap());
    let handshake = server
        .receive_datagram(outer_source, &initiation[0])
        .unwrap();
    assert_eq!(handshake.peer_index, Some(index));
    let response = network_packets(handshake.actions);
    let queued = network_packets(client.receive_datagram(None, &response[0]).unwrap());
    let mut delivered = false;
    for datagram in queued {
        let dispatch = server.receive_datagram(outer_source, &datagram).unwrap();
        assert_eq!(dispatch.peer_index, Some(index));
        delivered |= dispatch.actions.iter().any(|action| {
            matches!(action, TunnelAction::ReceiveIp { packet, source }
                if packet == &request && *source == IpAddr::V4(inner_source))
        });
    }
    assert!(delivered, "peer {index} did not establish payload flow");
}

#[test]
fn malformed_data_from_one_peer_does_not_break_another_authenticated_peer() {
    let server_private = key(91);
    let first_public = public_key(92);
    let second_public = public_key(93);
    let peers = [
        InboundPeerInput {
            public_key: &first_public,
            pre_shared_key: None,
            allowed_ips: &["10.0.0.2/32"],
            keepalive_secs: 0,
            reserved: &[],
        },
        InboundPeerInput {
            public_key: &second_public,
            pre_shared_key: None,
            allowed_ips: &["10.0.0.3/32"],
            keepalive_secs: 0,
            reserved: &[],
        },
    ];
    let mut server = PreparedInbound::from_input(InboundInput {
        addresses: &[],
        private_key: &server_private,
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let mut first = tunnel(92, 91, "10.0.0.2/32");
    let mut second = tunnel(93, 91, "10.0.0.3/32");
    let first_inner = Ipv4Addr::new(10, 0, 0, 2);
    let second_inner = Ipv4Addr::new(10, 0, 0, 3);
    let first_outer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 92)), 51_820);
    let second_outer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 93)), 51_821);
    establish(&mut server, &mut first, 0, first_outer, first_inner);
    establish(&mut server, &mut second, 1, second_outer, second_inner);
    let first_source = server.peer_source(0).unwrap();
    let second_source = server.peer_source(1).unwrap();
    assert_eq!(first_source.authenticated_endpoint, Some(first_outer));
    assert_eq!(second_source.authenticated_endpoint, Some(second_outer));
    assert_eq!(first_source.source_known, Some(true));
    assert!(second_source.last_authenticated_packet_age.is_some());

    let first_packet = ipv4_packet(first_inner, Ipv4Addr::new(10, 0, 0, 1), b"corrupt this");
    let mut corrupted = network_packets(first.send_ip_packet(&first_packet).unwrap());
    assert_eq!(corrupted.len(), 1);
    let last = corrupted[0].last_mut().unwrap();
    *last ^= 0x80;
    assert_eq!(
        server.receive_datagram(first_outer, &corrupted[0]).err(),
        Some(TunnelError::Engine)
    );
    assert_eq!(
        server.peer_source(0).unwrap().authenticated_endpoint,
        Some(first_outer)
    );
    drop(first);

    let request = ipv4_packet(
        second_inner,
        Ipv4Addr::new(10, 0, 0, 1),
        b"peer two survives",
    );
    let data = network_packets(second.send_ip_packet(&request).unwrap());
    assert_eq!(data.len(), 1);
    let dispatch = server.receive_datagram(second_outer, &data[0]).unwrap();
    assert_eq!(dispatch.peer_index, Some(1));
    assert!(dispatch.actions.iter().any(|action| {
        matches!(action, TunnelAction::ReceiveIp { packet, source }
            if packet == &request && *source == IpAddr::V4(second_inner))
    }));

    let response = ipv4_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        second_inner,
        b"still bidirectional",
    );
    let wire = network_packets(server.send_ip_packet(1, &response).unwrap());
    assert_eq!(wire.len(), 1);
    let decrypted = second.receive_datagram(None, &wire[0]).unwrap();
    assert!(decrypted.iter().any(|action| {
        matches!(action, TunnelAction::ReceiveIp { packet, .. } if packet == &response)
    }));

    let opaque_packet = ipv4_packet(second_inner, Ipv4Addr::new(10, 0, 0, 1), b"opaque carrier");
    let opaque = network_packets(second.send_ip_packet(&opaque_packet).unwrap());
    let dispatch = server
        .receive_datagram_with_source(None, &opaque[0])
        .unwrap();
    assert!(dispatch.authenticated);
    let source = server.peer_source(1).unwrap();
    assert_eq!(source.source_known, Some(false));
    assert_eq!(source.authenticated_endpoint, None);
    assert_eq!(
        server.peer_source(0).unwrap().authenticated_endpoint,
        Some(first_outer)
    );
}

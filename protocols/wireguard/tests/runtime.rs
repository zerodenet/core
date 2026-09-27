use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use wireguard::{
    runtime::{PeerTunnel, PreparedInbound, PreparedOutbound, TunnelAction, TunnelError},
    validation::{validate_outbound, InboundInput, InboundPeerInput, OutboundInput, PeerInput},
};

#[test]
fn protocol_engine_static_secret_is_zeroized_on_drop() {
    // x25519-dalek attaches zeroize(drop) to StaticSecret behind its feature.
    // This bound fails to compile if a dependency update drops that feature.
    fn requires_zeroize<T: zeroize::Zeroize>() {}
    requires_zeroize::<StaticSecret>();
    assert!(std::mem::needs_drop::<StaticSecret>());
}

fn key(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

fn public_key(private: u8) -> String {
    let secret = StaticSecret::from([private; 32]);
    STANDARD.encode(PublicKey::from(&secret).as_bytes())
}

fn outer(ip: IpAddr) -> SocketAddr {
    SocketAddr::new(ip, 51_820)
}

fn tunnel(private: u8, peer_private: u8, address: &str) -> PeerTunnel {
    let private_key = key(private);
    let public_key = public_key(peer_private);
    let peer = PeerInput {
        public_key: &public_key,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 25,
        reserved: &[],
    };
    let addresses = [address];
    let peers = [peer];
    let profile = validate_outbound(OutboundInput {
        private_key: &private_key,
        addresses: &addresses,
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    PeerTunnel::from_validated(&profile, 0).unwrap()
}

fn ipv4_packet(source: Ipv4Addr, destination: Ipv4Addr, payload: &[u8]) -> Vec<u8> {
    let length = 20 + payload.len();
    let mut packet = vec![0_u8; length];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(length as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet[20..].copy_from_slice(payload);
    packet
}

fn ipv6_packet(source: Ipv6Addr, destination: Ipv6Addr, payload: &[u8]) -> Vec<u8> {
    let mut packet = vec![0_u8; 40 + payload.len()];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload.len() as u16).to_be_bytes());
    packet[6] = 17;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet[40..].copy_from_slice(payload);
    packet
}

fn network_packets(actions: Vec<TunnelAction>) -> Vec<Vec<u8>> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            TunnelAction::SendNetwork(packet) => Some(packet),
            TunnelAction::ReceiveIp { .. } => None,
        })
        .collect()
}

#[test]
fn peers_handshake_and_exchange_encrypted_ipv4_payload_both_ways() {
    let mut a = tunnel(1, 2, "10.0.0.1/32");
    let mut b = tunnel(2, 1, "10.0.0.2/32");
    let a_ip = Ipv4Addr::new(10, 0, 0, 1);
    let b_ip = Ipv4Addr::new(10, 0, 0, 2);
    let request = ipv4_packet(a_ip, b_ip, b"request payload");
    let response = ipv4_packet(b_ip, a_ip, b"response payload");

    let initiation = network_packets(a.send_ip_packet(&request).unwrap());
    assert_eq!(initiation.len(), 1);
    assert_eq!(&initiation[0][..4], &1_u32.to_le_bytes());

    let handshake_response = network_packets(
        b.receive_datagram(Some(outer(IpAddr::V4(a_ip))), &initiation[0])
            .unwrap(),
    );
    assert_eq!(handshake_response.len(), 1);
    assert_eq!(&handshake_response[0][..4], &2_u32.to_le_bytes());

    let queued = network_packets(
        a.receive_datagram(Some(outer(IpAddr::V4(b_ip))), &handshake_response[0])
            .unwrap(),
    );
    assert!(a.time_since_last_handshake().is_some());
    assert!(queued
        .iter()
        .any(|packet| packet.starts_with(&4_u32.to_le_bytes())));
    let mut delivered = Vec::new();
    for packet in &queued {
        delivered.extend(
            b.receive_datagram(Some(outer(IpAddr::V4(a_ip))), packet)
                .unwrap(),
        );
    }
    assert!(delivered.iter().any(|action| matches!(
        action,
        TunnelAction::ReceiveIp { packet, source }
            if packet == &request && *source == IpAddr::V4(a_ip)
    )));

    let reply = network_packets(b.send_ip_packet(&response).unwrap());
    assert_eq!(reply.len(), 1);
    let decrypted = a
        .receive_datagram(Some(outer(IpAddr::V4(b_ip))), &reply[0])
        .unwrap();
    assert!(decrypted.iter().any(|action| matches!(
        action,
        TunnelAction::ReceiveIp { packet, source }
            if packet == &response && *source == IpAddr::V4(b_ip)
    )));
}

#[test]
fn peers_handshake_and_exchange_encrypted_ipv6_payload_both_ways() {
    let mut a = tunnel(9, 10, "fd00::9/128");
    let mut b = tunnel(10, 9, "fd00::a/128");
    let a_ip: Ipv6Addr = "fd00::9".parse().unwrap();
    let b_ip: Ipv6Addr = "fd00::a".parse().unwrap();
    let request = ipv6_packet(a_ip, b_ip, b"ipv6 request payload");
    let response = ipv6_packet(b_ip, a_ip, b"ipv6 response payload");

    let initiation = network_packets(a.send_ip_packet(&request).unwrap());
    let handshake_response = network_packets(
        b.receive_datagram(Some(outer(IpAddr::V6(a_ip))), &initiation[0])
            .unwrap(),
    );
    let queued = network_packets(
        a.receive_datagram(Some(outer(IpAddr::V6(b_ip))), &handshake_response[0])
            .unwrap(),
    );
    let mut delivered = Vec::new();
    for packet in &queued {
        delivered.extend(
            b.receive_datagram(Some(outer(IpAddr::V6(a_ip))), packet)
                .unwrap(),
        );
    }
    assert!(delivered.iter().any(|action| matches!(
        action,
        TunnelAction::ReceiveIp { packet, source }
            if packet == &request && *source == IpAddr::V6(a_ip)
    )));

    let reply = network_packets(b.send_ip_packet(&response).unwrap());
    let decrypted = a
        .receive_datagram(Some(outer(IpAddr::V6(b_ip))), &reply[0])
        .unwrap();
    assert!(decrypted.iter().any(|action| matches!(
        action,
        TunnelAction::ReceiveIp { packet, source }
            if packet == &response && *source == IpAddr::V6(b_ip)
    )));
}

#[test]
fn transport_data_is_padded_and_replay_is_rejected() {
    let mut a = tunnel(3, 4, "10.0.0.3/32");
    let mut b = tunnel(4, 3, "10.0.0.4/32");
    let a_ip = Ipv4Addr::new(10, 0, 0, 3);
    let b_ip = Ipv4Addr::new(10, 0, 0, 4);
    let request = ipv4_packet(a_ip, b_ip, b"odd length");
    let initiation = network_packets(a.send_ip_packet(&request).unwrap());
    let response = network_packets(b.receive_datagram(None, &initiation[0]).unwrap());
    let packets = network_packets(a.receive_datagram(None, &response[0]).unwrap());
    let data = packets
        .iter()
        .find(|packet| packet.starts_with(&4_u32.to_le_bytes()) && packet.len() > 32)
        .unwrap();

    assert_eq!((data.len() - 32) % 16, 0);
    assert!(data.len() >= request.len() + 32);
    b.receive_datagram(None, data).unwrap();
    assert_eq!(b.receive_datagram(None, data), Err(TunnelError::Engine));
}

#[test]
fn authenticated_datagrams_allow_endpoint_roaming_but_replay_does_not() {
    let mut a = tunnel(43, 44, "10.0.0.43/32");
    let mut b = tunnel(44, 43, "10.0.0.44/32");
    let a_ip = Ipv4Addr::new(10, 0, 0, 43);
    let b_ip = Ipv4Addr::new(10, 0, 0, 44);
    let request = ipv4_packet(a_ip, b_ip, b"roaming");

    let initiation = network_packets(a.send_ip_packet(&request).unwrap());
    let received = b
        .receive_datagram_with_authentication(None, &initiation[0])
        .unwrap();
    assert!(received.authenticated);
    let response = network_packets(received.actions);
    let received = a
        .receive_datagram_with_authentication(None, &response[0])
        .unwrap();
    assert!(received.authenticated);
    let queued = network_packets(received.actions);
    let data = queued
        .iter()
        .find(|packet| packet.starts_with(&4_u32.to_le_bytes()) && packet.len() > 32)
        .unwrap();

    assert!(
        b.receive_datagram_with_authentication(None, data)
            .unwrap()
            .authenticated
    );
    assert_eq!(
        b.receive_datagram_with_authentication(None, data).err(),
        Some(TunnelError::Engine)
    );
    let mut forged = data.clone();
    let last = forged.len() - 1;
    forged[last] ^= 1;
    assert_eq!(
        b.receive_datagram_with_authentication(None, &forged).err(),
        Some(TunnelError::Engine)
    );
}

#[test]
fn transport_data_counter_at_local_rejection_limit_is_rejected() {
    let mut receiver = tunnel(4, 3, "10.0.0.4/32");
    let mut datagram = vec![0_u8; 32];
    datagram[..4].copy_from_slice(&4_u32.to_le_bytes());
    datagram[8..16].copy_from_slice(&(1_u64 << 32).to_le_bytes());
    assert_eq!(
        receiver.receive_datagram(None, &datagram),
        Err(TunnelError::MessageLimit)
    );
    datagram[8..16].copy_from_slice(&((1_u64 << 32) - 1).to_le_bytes());
    assert_ne!(
        receiver.receive_datagram(None, &datagram),
        Err(TunnelError::MessageLimit)
    );
}

#[test]
fn oversized_inner_packet_fails_before_encryption() {
    let mut tunnel = tunnel(5, 6, "10.0.0.5/32");
    assert_eq!(tunnel.send_ip_packet(&[]), Err(TunnelError::EmptyPacket));
    assert_eq!(
        tunnel.send_ip_packet(&vec![0_u8; 1_421]),
        Err(TunnelError::PacketTooLarge)
    );
}

#[test]
fn prepared_profile_owns_peer_route_and_keys_after_input_is_dropped() {
    let profile = {
        let private_key = key(7);
        let public_key = public_key(8);
        let addresses = ["10.0.0.7/32"];
        let allowed_ips = ["10.0.0.8/32"];
        let peers = [PeerInput {
            public_key: &public_key,
            pre_shared_key: None,
            endpoint: "127.0.0.1:51820",
            allowed_ips: &allowed_ips,
            keepalive_secs: 25,
            reserved: &[],
        }];
        PreparedOutbound::from_input(OutboundInput {
            private_key: &private_key,
            addresses: &addresses,
            mtu: 1_420,
            peers: &peers,
        })
        .unwrap()
    };
    let remote = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8));
    assert_eq!(profile.peer_for_destination(remote), Some(0));
    assert_eq!(
        profile.local_address_for(remote),
        Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)))
    );
    assert!(profile.allows_authenticated_source(0, remote));
    assert_eq!(profile.peer(0).unwrap().endpoint_host(), "127.0.0.1");
    assert!(!network_packets(
        PeerTunnel::from_prepared(&profile, 0)
            .unwrap()
            .initiate_handshake()
            .unwrap()
    )
    .is_empty());
    assert!(matches!(
        PeerTunnel::from_prepared(&profile, 1),
        Err(TunnelError::UnknownPeer)
    ));
}

#[test]
fn inbound_device_demultiplexes_handshake_and_checks_authenticated_inner_source() {
    let server_private = key(31);
    let client_public = public_key(32);
    let inbound_peer = [InboundPeerInput {
        public_key: &client_public,
        pre_shared_key: None,
        allowed_ips: &["10.0.0.2/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let mut server = PreparedInbound::from_input(InboundInput {
        private_key: &server_private,
        mtu: 1420,
        peers: &inbound_peer,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let mut client = tunnel(32, 31, "10.0.0.2/32");
    let client_ip = Ipv4Addr::new(10, 0, 0, 2);
    let server_ip = Ipv4Addr::new(10, 0, 0, 1);
    let outer_ip = Ipv4Addr::new(192, 0, 2, 32);
    let request = ipv4_packet(client_ip, server_ip, b"hello");

    let initiation = network_packets(client.send_ip_packet(&request).unwrap());
    let dispatch = server
        .receive_datagram(outer(IpAddr::V4(outer_ip)), &initiation[0])
        .unwrap();
    assert_eq!(dispatch.peer_index, Some(0));
    assert!(dispatch.authenticated);
    let handshake_response = network_packets(dispatch.actions);
    let queued = network_packets(
        client
            .receive_datagram(Some(outer(IpAddr::V4(server_ip))), &handshake_response[0])
            .unwrap(),
    );
    let mut delivered = Vec::new();
    for datagram in queued {
        let dispatch = server
            .receive_datagram(outer(IpAddr::V4(outer_ip)), &datagram)
            .unwrap();
        assert_eq!(dispatch.peer_index, Some(0));
        delivered.extend(dispatch.actions);
    }
    assert!(delivered.iter().any(|action| matches!(action,
        TunnelAction::ReceiveIp { packet, source }
            if packet == &request && *source == IpAddr::V4(client_ip)
    )));

    let forged_source = ipv4_packet(Ipv4Addr::new(10, 0, 0, 99), server_ip, b"spoofed");
    let encrypted = network_packets(client.send_ip_packet(&forged_source).unwrap());
    let dropped = server
        .receive_datagram(outer(IpAddr::V4(outer_ip)), &encrypted[0])
        .unwrap();
    assert!(dropped.actions.is_empty());
    assert_eq!(server.peer_for_destination(IpAddr::V4(client_ip)), Some(0));
    assert_eq!(server.peer_for_destination(IpAddr::V4(server_ip)), None);
}

#[test]
fn inbound_device_rejects_unknown_peer_handshake() {
    let server_private = key(41);
    let known_public = public_key(42);
    let inbound_peer = [InboundPeerInput {
        public_key: &known_public,
        pre_shared_key: None,
        allowed_ips: &["10.0.0.2/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let mut server = PreparedInbound::from_input(InboundInput {
        private_key: &server_private,
        mtu: 1420,
        peers: &inbound_peer,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let mut unknown = tunnel(43, 41, "10.0.0.3/32");
    let initiation = network_packets(unknown.initiate_handshake().unwrap());
    assert!(matches!(
        server.receive_datagram(outer(IpAddr::V4(Ipv4Addr::LOCALHOST)), &initiation[0]),
        Err(TunnelError::UnknownPeer)
    ));
}

#[test]
fn inbound_handshake_rate_limit_resets_after_one_second() {
    let server_private = key(91);
    let client_public = public_key(92);
    let peers = [InboundPeerInput {
        public_key: &client_public,
        pre_shared_key: None,
        allowed_ips: &["10.0.0.2/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let mut server = PreparedInbound::from_input(InboundInput {
        private_key: &server_private,
        mtu: 1420,
        peers: &peers,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let mut client = tunnel(92, 91, "10.0.0.2/32");
    let initiation = network_packets(client.initiate_handshake().unwrap()).remove(0);
    let source = outer(IpAddr::V4(Ipv4Addr::LOCALHOST));

    for _ in 0..100 {
        let _ = server.receive_datagram(source, &initiation);
    }
    let challenge = server.receive_datagram(source, &initiation).unwrap();
    assert!(
        matches!(challenge.actions.first(), Some(TunnelAction::SendNetwork(packet)) if packet[0] == 3)
    );

    std::thread::sleep(std::time::Duration::from_millis(1_100));
    let after_reset = server.receive_datagram(source, &initiation);
    assert!(
        !matches!(after_reset, Ok(dispatch) if matches!(dispatch.actions.first(), Some(TunnelAction::SendNetwork(packet)) if packet[0] == 3))
    );
}

#[test]
fn opaque_outer_carrier_rejects_handshakes_at_cookie_threshold() {
    let server_private = key(93);
    let client_public = public_key(94);
    let peers = [InboundPeerInput {
        public_key: &client_public,
        pre_shared_key: None,
        allowed_ips: &["10.0.0.2/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let mut server = PreparedInbound::from_input(InboundInput {
        private_key: &server_private,
        mtu: 1420,
        peers: &peers,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let mut outbound = tunnel(93, 94, "10.0.0.1/32");
    let mut client = tunnel(94, 93, "10.0.0.2/32");
    let initiation = network_packets(client.initiate_handshake().unwrap()).remove(0);

    for _ in 0..100 {
        let _ = server.receive_datagram_with_source(None, &initiation);
        let _ = outbound.receive_datagram(None, &initiation);
    }
    assert!(matches!(
        server.receive_datagram_with_source(None, &initiation),
        Err(TunnelError::RateLimited)
    ));
    assert!(matches!(
        outbound.receive_datagram(None, &initiation),
        Err(TunnelError::RateLimited)
    ));

    std::thread::sleep(std::time::Duration::from_millis(1_100));
    assert_ne!(
        server.receive_datagram_with_source(None, &initiation).err(),
        Some(TunnelError::RateLimited)
    );
    assert_ne!(
        outbound.receive_datagram(None, &initiation).err(),
        Some(TunnelError::RateLimited)
    );
}

#[test]
fn inbound_device_routes_transport_packets_to_each_authenticated_peer() {
    let server_private = key(71);
    let first_public = public_key(72);
    let second_public = public_key(73);
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
        private_key: &server_private,
        mtu: 1420,
        peers: &peers,
    })
    .unwrap()
    .into_device()
    .unwrap();
    let destination = Ipv4Addr::new(10, 0, 0, 1);
    for (peer_index, private, address) in [
        (0, 72, Ipv4Addr::new(10, 0, 0, 2)),
        (1, 73, Ipv4Addr::new(10, 0, 0, 3)),
    ] {
        let mut client = tunnel(private, 71, &format!("{address}/32"));
        let request = ipv4_packet(address, destination, b"peer-specific payload");
        let initiation = network_packets(client.send_ip_packet(&request).unwrap());
        let response = server
            .receive_datagram(outer(IpAddr::V4(Ipv4Addr::LOCALHOST)), &initiation[0])
            .unwrap();
        assert_eq!(response.peer_index, Some(peer_index));
        let response = network_packets(response.actions);
        let queued = network_packets(client.receive_datagram(None, &response[0]).unwrap());
        let mut delivered_payload = false;
        for datagram in queued {
            let delivered = server
                .receive_datagram(outer(IpAddr::V4(Ipv4Addr::LOCALHOST)), &datagram)
                .unwrap();
            assert_eq!(delivered.peer_index, Some(peer_index));
            delivered_payload |= delivered.actions.iter().any(|action| {
                matches!(action,
                    TunnelAction::ReceiveIp { packet, source }
                        if packet == &request && *source == IpAddr::V4(address)
                )
            });
        }
        assert!(delivered_payload);
    }
}

#[path = "runtime/multi_peer.rs"]
mod multi_peer;

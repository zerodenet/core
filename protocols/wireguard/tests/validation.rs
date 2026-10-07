use base64::{engine::general_purpose::STANDARD, Engine as _};

#[test]
fn peer_identity_uses_public_key_bytes_and_is_independent_of_text_encoding() {
    let base64 = STANDARD.encode([9; 32]);
    let hex = "09".repeat(32);
    assert_eq!(
        wireguard::validation::public_peer_id(&base64).unwrap(),
        wireguard::validation::public_peer_id(&hex).unwrap()
    );
    assert!(wireguard::validation::public_peer_id("invalid").is_err());
}
use std::net::{IpAddr, Ipv4Addr};
use wireguard::validation::{
    parse_endpoint, parse_key, parse_network, validate_inbound, validate_outbound, EndpointError,
    InboundInput, InboundPeerInput, InboundValidationError, KeyError, NetworkError, OutboundInput,
    PeerInput, ValidationError, DEFAULT_MTU, MAX_MTU,
};
use zeroize::Zeroize;

fn key(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

fn peer<'a>(public_key: &'a str, allowed_ips: &'a [&'a str]) -> PeerInput<'a> {
    PeerInput {
        public_key,
        pre_shared_key: None,
        endpoint: "peer.example:51820",
        allowed_ips,
        keepalive_secs: 25,
        reserved: &[],
    }
}

#[test]
fn only_listening_profiles_allow_learning_a_peer_address() {
    let allowed = ["10.0.0.2/32"];
    let public = key(2);
    let private = key(1);
    let mut peer = peer(&public, &allowed);
    peer.endpoint = "";
    let peers = [peer];
    let addresses = ["10.0.0.1/32"];
    let input = OutboundInput {
        private_key: &private,
        addresses: &addresses,
        mtu: DEFAULT_MTU,
        peers: &peers,
    };
    assert!(validate_outbound(input).is_err());
    assert!(wireguard::validation::validate_listening_endpoint(input)
        .unwrap()
        .peers[0]
        .endpoint
        .is_none());
}

#[test]
fn inbound_assigned_addresses_reuse_interface_validation_and_remain_optional() {
    let private = key(31);
    let public = key(32);
    let peers = [InboundPeerInput {
        public_key: &public,
        pre_shared_key: None,
        allowed_ips: &["10.0.0.2/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let mut input = InboundInput {
        private_key: &private,
        addresses: &[],
        mtu: 1420,
        peers: &peers,
    };
    assert!(validate_inbound(input).unwrap().addresses.is_empty());
    input.addresses = &["10.0.0.11/24", "fd00::11/64"];
    let validated = validate_inbound(input).unwrap();
    assert_eq!(
        validated.addresses[0].address(),
        "10.0.0.11".parse::<IpAddr>().unwrap()
    );
    input.mtu = 1200;
    assert!(matches!(
        validate_inbound(input),
        Err(InboundValidationError::Address(
            ValidationError::Ipv6MtuTooSmall { .. }
        ))
    ));
    input.mtu = 1420;
    input.addresses = &["not-an-address"];
    assert!(matches!(
        validate_inbound(input),
        Err(InboundValidationError::Address(
            ValidationError::Address { .. }
        ))
    ));
    input.addresses = &["10.0.0.1/32", "10.0.0.1/32"];
    assert!(matches!(
        validate_inbound(input),
        Err(InboundValidationError::Address(
            ValidationError::DuplicateAddress { .. }
        ))
    ));
}

#[test]
fn validates_inbound_peers_and_rejects_ambiguous_source_ownership() {
    let private = key(51);
    let peer_a = key(52);
    let peer_b = key(53);
    let allowed_a = ["10.0.0.2/32"];
    let allowed_b = ["10.0.0.3/32"];
    let peers = [
        InboundPeerInput {
            public_key: &peer_a,
            pre_shared_key: None,
            allowed_ips: &allowed_a,
            keepalive_secs: 0,
            reserved: &[],
        },
        InboundPeerInput {
            public_key: &peer_b,
            pre_shared_key: None,
            allowed_ips: &allowed_b,
            keepalive_secs: 0,
            reserved: &[],
        },
    ];
    assert_eq!(
        validate_inbound(InboundInput {
            addresses: &[],
            private_key: &private,
            mtu: DEFAULT_MTU,
            peers: &peers,
        })
        .unwrap()
        .peers
        .len(),
        2
    );
    let conflict = ["10.0.0.2/32"];
    let conflicting = [
        peers[0],
        InboundPeerInput {
            allowed_ips: &conflict,
            ..peers[1]
        },
    ];
    assert_eq!(
        validate_inbound(InboundInput {
            addresses: &[],
            private_key: &private,
            mtu: DEFAULT_MTU,
            peers: &conflicting,
        }),
        Err(InboundValidationError::ConflictingAllowedIp {
            first_peer: 0,
            second_peer: 1
        })
    );
}

#[test]
fn inbound_peer_table_is_bounded_before_allocating_peer_state() {
    let private = key(54);
    let public = key(55);
    let peers = vec![
        InboundPeerInput {
            public_key: &public,
            pre_shared_key: None,
            allowed_ips: &["10.0.0.2/32"],
            keepalive_secs: 0,
            reserved: &[],
        };
        129
    ];
    assert_eq!(
        validate_inbound(InboundInput {
            addresses: &[],
            private_key: &private,
            mtu: DEFAULT_MTU,
            peers: &peers,
        }),
        Err(InboundValidationError::TooManyPeers)
    );
}

#[test]
fn parses_standard_base64_and_hex_keys() {
    let expected = [0xabu8; 32];
    assert_eq!(
        parse_key(&STANDARD.encode(expected)).unwrap().as_bytes(),
        &expected
    );
    assert_eq!(parse_key(&"ab".repeat(32)).unwrap().as_bytes(), &expected);
    assert_eq!(parse_key("not-a-key"), Err(KeyError::InvalidEncoding));
    assert_eq!(
        parse_key(&STANDARD.encode([1_u8; 31])),
        Err(KeyError::InvalidLength)
    );
}

#[test]
fn key_debug_output_is_redacted() {
    let secret = key(7);
    let parsed = parse_key(&secret).unwrap();
    let debug = format!("{parsed:?}");
    assert_eq!(debug, "Key([REDACTED])");
    assert!(!debug.contains(&secret));
}

#[test]
fn key_material_can_be_zeroized() {
    let secret = [7_u8; 32];
    let mut parsed = parse_key(&STANDARD.encode(secret)).unwrap();
    parsed.zeroize();
    assert_eq!(parsed.as_bytes(), &[0_u8; 32]);
}

#[test]
fn parses_and_matches_ip_networks() {
    let network = parse_network("192.0.2.9/24").unwrap();
    assert_eq!(
        network.network_address(),
        "192.0.2.0".parse::<IpAddr>().unwrap()
    );
    assert!(network.contains(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 200))));
    assert!(!network.contains(IpAddr::V4(Ipv4Addr::new(192, 0, 3, 1))));
    assert_eq!(parse_network("192.0.2.1"), Err(NetworkError::MissingPrefix));
    assert_eq!(
        parse_network("192.0.2.1/33"),
        Err(NetworkError::InvalidPrefix)
    );
}

#[test]
fn parses_domain_ipv4_and_bracketed_ipv6_endpoints() {
    assert_eq!(parse_endpoint("peer.example:51820").unwrap().port, 51820);
    assert_eq!(parse_endpoint("192.0.2.1:80").unwrap().host, "192.0.2.1");
    assert_eq!(
        parse_endpoint("[2001:db8::1]:443").unwrap().host,
        "2001:db8::1"
    );
    assert_eq!(
        parse_endpoint("2001:db8::1:443"),
        Err(EndpointError::InvalidFormat)
    );
    assert_eq!(
        parse_endpoint("peer.example:0"),
        Err(EndpointError::InvalidPort)
    );
}

#[test]
fn validates_a_dual_stack_outbound_profile() {
    let private = key(1);
    let public = key(2);
    let addresses = ["172.16.0.2/32", "fd00::2/128"];
    let allowed = ["0.0.0.0/0", "::/0"];
    let peers = [peer(&public, &allowed)];
    let profile = validate_outbound(OutboundInput {
        private_key: &private,
        addresses: &addresses,
        mtu: DEFAULT_MTU,
        peers: &peers,
    })
    .unwrap();

    assert_eq!(profile.addresses.len(), 2);
    assert_eq!(profile.peers.len(), 1);
    assert_eq!(profile.peers[0].endpoint.unwrap().port, 51820);
}

#[test]
fn rejects_ipv6_when_mtu_is_below_the_ipv6_minimum() {
    let private = key(1);
    let public = key(2);
    let addresses = ["fd00::2/128"];
    let allowed = ["::/0"];
    let peers = [peer(&public, &allowed)];
    assert_eq!(
        validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &addresses,
            mtu: 1279,
            peers: &peers,
        }),
        Err(ValidationError::Ipv6MtuTooSmall { mtu: 1279 })
    );
}

#[test]
fn rejects_mtu_outside_ip_and_wire_datagram_limits() {
    let private = key(1);
    let public = key(2);
    let addresses = ["10.0.0.1/32"];
    let allowed = ["10.0.0.2/32"];
    let peers = [peer(&public, &allowed)];
    for mtu in [67, MAX_MTU + 1] {
        assert_eq!(
            validate_outbound(OutboundInput {
                private_key: &private,
                addresses: &addresses,
                mtu,
                peers: &peers,
            }),
            Err(ValidationError::InvalidMtu)
        );
    }
}

#[test]
fn rejects_non_zero_reserved_bytes_before_runtime() {
    let private = key(1);
    let public = key(2);
    let allowed = ["10.0.0.2/32"];
    let peers = [PeerInput {
        reserved: &[1, 0, 0],
        ..peer(&public, &allowed)
    }];
    assert_eq!(
        validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &["10.0.0.1/32"],
            mtu: DEFAULT_MTU,
            peers: &peers,
        }),
        Err(ValidationError::UnsupportedReserved { peer: 0 })
    );
}

#[test]
fn rejects_duplicate_peers_and_ambiguous_allowed_ips() {
    let private = key(1);
    let public_a = key(2);
    let public_b = key(3);
    let addresses = ["172.16.0.2/32"];
    let allowed_a = ["10.0.0.1/8"];
    let allowed_b = ["10.1.2.3/8"];

    let duplicate_peers = [peer(&public_a, &allowed_a), peer(&public_a, &allowed_b)];
    assert_eq!(
        validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &addresses,
            mtu: DEFAULT_MTU,
            peers: &duplicate_peers,
        }),
        Err(ValidationError::DuplicatePeerKey {
            first: 0,
            second: 1
        })
    );

    let conflicting_routes = [peer(&public_a, &allowed_a), peer(&public_b, &allowed_b)];
    assert_eq!(
        validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &addresses,
            mtu: DEFAULT_MTU,
            peers: &conflicting_routes,
        }),
        Err(ValidationError::ConflictingAllowedIp {
            first_peer: 0,
            second_peer: 1,
        })
    );
}

#[test]
fn validation_errors_do_not_echo_key_material() {
    let secret = "secret-key-material-that-must-not-be-logged";
    let error = validate_outbound(OutboundInput {
        private_key: secret,
        addresses: &["172.16.0.2/32"],
        mtu: DEFAULT_MTU,
        peers: &[],
    })
    .unwrap_err();
    assert!(!format!("{error:?}").contains(secret));
}

use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::net::{IpAddr, Ipv4Addr};
use wireguard::validation::{
    parse_endpoint, parse_key, parse_network, validate_outbound, EndpointError, KeyError,
    NetworkError, OutboundInput, PeerInput, ValidationError, DEFAULT_MTU,
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
    assert_eq!(profile.peers[0].endpoint.port, 51820);
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

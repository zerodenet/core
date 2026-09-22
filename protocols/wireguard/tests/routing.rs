use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::net::IpAddr;
use wireguard::{
    routing::PeerRoutes,
    validation::{validate_outbound, OutboundInput, PeerInput, DEFAULT_MTU},
};

fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}

fn key(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

#[test]
fn selects_longest_prefix_across_peers_and_families() {
    let private = key(1);
    let public_a = key(2);
    let public_b = key(3);
    let public_c = key(4);
    let defaults = ["0.0.0.0/0", "::/0"];
    let subnet = ["10.1.0.0/16", "2001:db8:1::/48"];
    let host = ["10.1.2.3/32"];
    let peers = [
        peer(&public_a, &defaults),
        peer(&public_b, &subnet),
        peer(&public_c, &host),
    ];
    let profile = validate_outbound(OutboundInput {
        private_key: &private,
        addresses: &["172.16.0.2/32", "fd00::2/128"],
        mtu: DEFAULT_MTU,
        peers: &peers,
    })
    .unwrap();
    let routes = PeerRoutes::from_validated(&profile);

    assert_eq!(routes.peer_for_destination(ip("8.8.8.8")), Some(0));
    assert_eq!(routes.peer_for_destination(ip("10.1.2.4")), Some(1));
    assert_eq!(routes.peer_for_destination(ip("10.1.2.3")), Some(2));
    assert_eq!(routes.peer_for_destination(ip("2001:db8:1::1")), Some(1));
    assert_eq!(routes.peer_for_destination(ip("2001:4860::1")), Some(0));
}

#[test]
fn rejects_spoofed_source_from_less_specific_or_unmatched_peer() {
    let private = key(1);
    let public_a = key(2);
    let public_b = key(3);
    let defaults = ["0.0.0.0/0"];
    let subnet = ["10.1.0.0/16"];
    let peers = [peer(&public_a, &defaults), peer(&public_b, &subnet)];
    let profile = validate_outbound(OutboundInput {
        private_key: &private,
        addresses: &["172.16.0.2/32"],
        mtu: DEFAULT_MTU,
        peers: &peers,
    })
    .unwrap();
    let routes = PeerRoutes::from_validated(&profile);

    assert!(routes.allows_authenticated_source(0, ip("8.8.8.8")));
    assert!(!routes.allows_authenticated_source(0, ip("10.1.2.3")));
    assert!(routes.allows_authenticated_source(1, ip("10.1.2.3")));
    assert!(!routes.allows_authenticated_source(1, ip("8.8.8.8")));
    assert!(!routes.allows_authenticated_source(2, ip("8.8.8.8")));
    assert!(!routes.allows_authenticated_source(0, ip("2001:db8::1")));
}

#[test]
fn keeps_nested_prefixes_within_one_peer_unambiguous() {
    let private = key(1);
    let public = key(2);
    let ranges = ["10.0.0.0/8", "10.1.0.0/16"];
    let peers = [peer(&public, &ranges)];
    let profile = validate_outbound(OutboundInput {
        private_key: &private,
        addresses: &["172.16.0.2/32"],
        mtu: DEFAULT_MTU,
        peers: &peers,
    })
    .unwrap();
    let routes = PeerRoutes::from_validated(&profile);

    assert_eq!(routes.peer_for_destination(ip("10.1.2.3")), Some(0));
    assert_eq!(routes.peer_for_destination(ip("10.2.2.3")), Some(0));
    assert_eq!(routes.peer_for_destination(ip("11.0.0.1")), None);
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

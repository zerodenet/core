use std::io;
use std::net::{IpAddr, Ipv4Addr};

use super::ipv4_peer;

#[test]
fn selects_the_next_address_as_the_point_to_point_peer() {
    assert_eq!(
        ipv4_peer(
            Ipv4Addr::new(10, 66, 0, 1),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)),
        )
        .unwrap(),
        Ipv4Addr::new(10, 66, 0, 2)
    );
}

#[test]
fn selects_the_previous_address_at_the_end_of_the_subnet() {
    assert_eq!(
        ipv4_peer(
            Ipv4Addr::new(10, 66, 0, 255),
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)),
        )
        .unwrap(),
        Ipv4Addr::new(10, 66, 0, 254)
    );
}

#[test]
fn rejects_an_ipv4_host_route_without_a_peer() {
    let error = ipv4_peer(
        Ipv4Addr::new(10, 66, 0, 1),
        IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
    )
    .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

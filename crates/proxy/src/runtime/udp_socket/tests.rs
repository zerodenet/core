use std::net::SocketAddr;

use zero_core::Address;

use super::{select_stable_udp_target, DirectUdpSocketBinding};

#[test]
fn udp_target_selection_is_stable_across_same_family_answer_reordering() {
    let target = Address::Domain("Example.COM".to_owned());
    let first: Vec<SocketAddr> = [
        "192.0.2.10:443",
        "192.0.2.11:443",
        "192.0.2.12:443",
        "[2001:db8::10]:443",
    ]
    .into_iter()
    .map(|value| value.parse().unwrap())
    .collect();
    let reordered = vec![first[2], first[0], first[1], first[3]];

    let selected = select_stable_udp_target(&target, &first, true).unwrap();
    assert!(selected.is_ipv4());
    assert_eq!(
        select_stable_udp_target(&target, &reordered, true),
        Some(selected)
    );
}

#[test]
fn udp_target_selection_falls_back_to_an_available_family() {
    let target = Address::Domain("ipv6-only.example".to_owned());
    let candidates = ["[2001:db8::10]:443".parse().unwrap()];

    assert_eq!(select_stable_udp_target(&target, &candidates, false), None);
    assert_eq!(
        select_stable_udp_target(&target, &candidates, true),
        Some(candidates[0])
    );
}

#[test]
fn direct_udp_binding_is_scoped_by_family_and_egress() {
    let physical = zero_platform_tokio::EgressInterface::new("physical0", 7).unwrap();
    let physical_v4 = DirectUdpSocketBinding {
        ipv6: false,
        egress: Some(physical.clone()),
    };
    let system_route_v4 = DirectUdpSocketBinding {
        ipv6: false,
        egress: None,
    };
    let physical_v6 = DirectUdpSocketBinding {
        ipv6: true,
        egress: Some(physical),
    };

    assert_ne!(physical_v4, system_route_v4);
    assert_ne!(physical_v4, physical_v6);
}

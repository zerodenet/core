use std::net::SocketAddr;

use zero_core::Address;

use super::select_stable_udp_target;

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

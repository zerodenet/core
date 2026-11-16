use super::*;

#[test]
fn only_this_tuns_exact_addresses_and_peers_are_reserved() {
    let addresses = [
        (
            "10.19.7.1".parse().unwrap(),
            "255.255.255.0".parse().unwrap(),
        ),
        (
            "fd99::1".parse().unwrap(),
            "ffff:ffff:ffff:ffff::".parse().unwrap(),
        ),
    ];
    let actual = tun_local_addresses(&addresses).unwrap();
    for ip in ["10.19.7.1", "10.19.7.2", "fd99::1"] {
        assert!(actual.contains(&ip.parse().unwrap()));
    }
    for ip in [
        "10.0.0.2",
        "10.19.7.53",
        "192.168.1.180",
        "fd99::53",
        "fd99::2",
    ] {
        assert!(!actual.contains(&ip.parse().unwrap()));
    }
    assert!(tun_local_addresses(&[(addresses[0].0, addresses[1].1)]).is_err());
}

use super::select_candidate_egress;
use zero_platform_tokio::EgressInterfaceControl;

#[test]
fn active_tun_without_physical_egress_fails_closed_for_each_family() {
    let egress = EgressInterfaceControl::default();
    egress.replace_tunnel_addresses([
        "10.66.0.1".parse().expect("IPv4 TUN address"),
        "fd66::1".parse().expect("IPv6 TUN address"),
    ]);

    for candidate in ["192.0.2.1:443", "[2001:db8::1]:443"] {
        let candidate = candidate.parse().expect("test candidate");
        let error = select_candidate_egress(candidate, &egress)
            .expect_err("active TUN without matching physical egress must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
    }
}

#[test]
fn each_connection_observes_the_latest_egress_generation() {
    let egress = EgressInterfaceControl::default();
    let candidate = "192.0.2.1:443".parse().expect("IPv4 candidate");

    assert!(select_candidate_egress(candidate, &egress).is_ok());
    let idle_generation = egress.generation();

    egress.replace_tunnel_addresses(["10.66.0.1".parse().expect("IPv4 TUN address")]);
    assert!(egress.generation() > idle_generation);
    assert!(select_candidate_egress(candidate, &egress).is_err());
}

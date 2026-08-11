#![cfg(target_os = "windows")]

use std::net::IpAddr;

use zero_tun::TunDevice;

#[test]
#[ignore = "requires Administrator privileges and wintun.dll"]
fn windows_configures_ipv4_and_ipv6_addresses_on_one_adapter() {
    let device = zero_tun::create(Some("ZeroTunAddressTest")).expect("create Wintun adapter");
    let addresses = [
        (
            "10.67.0.1".parse::<IpAddr>().unwrap(),
            "255.255.255.0".parse::<IpAddr>().unwrap(),
        ),
        (
            "fd67::1".parse::<IpAddr>().unwrap(),
            "ffff:ffff:ffff:ffff::".parse::<IpAddr>().unwrap(),
        ),
    ];

    device
        .configure_addresses(&addresses, 1400)
        .expect("configure dual-stack Wintun addresses");
}

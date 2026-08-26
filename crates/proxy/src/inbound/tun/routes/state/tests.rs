use zero_platform_tokio::{EgressInterface, EgressInterfaceControl};

use super::withdraw_managed_egress;

#[test]
fn strict_failure_withdraws_only_the_managed_family() {
    let control = EgressInterfaceControl::default();
    let ipv4 = EgressInterface::new("physical-v4", 7).unwrap();
    let ipv6 = EgressInterface::new("physical-v6", 14).unwrap();
    control.replace_for(false, Some(ipv4));
    control.replace_for(true, Some(ipv6.clone()));
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let generation = control.generation();

    withdraw_managed_egress(&control, true, false);

    assert!(control.current_for(false).is_none());
    assert_eq!(control.current_for(true), Some(ipv6));
    assert_eq!(control.generation(), generation + 1);
    assert!(control
        .select_for_peer("0.0.0.0:0".parse().unwrap())
        .ensure_connectable()
        .is_err());
}

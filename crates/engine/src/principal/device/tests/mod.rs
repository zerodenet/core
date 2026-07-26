use super::PrincipalDeviceRegistry;
use zero_core::Address;

#[test]
fn counts_unique_addresses_and_releases_by_reference() {
    let registry = PrincipalDeviceRegistry::default();
    let first_ip = Address::Ipv4([192, 0, 2, 1]);
    let second_ip = Address::Ipv4([192, 0, 2, 2]);

    let first = registry.acquire("account:1", first_ip.clone(), 1).unwrap();
    let duplicate = registry.acquire("account:1", first_ip, 1).unwrap();
    assert!(registry
        .acquire("account:1", second_ip.clone(), 1)
        .is_none());
    assert_eq!(registry.active_device_count("account:1"), 1);

    drop(first);
    assert!(registry
        .acquire("account:1", second_ip.clone(), 1)
        .is_none());
    drop(duplicate);

    let second = registry.acquire("account:1", second_ip, 1).unwrap();
    assert_eq!(registry.active_device_count("account:1"), 1);
    drop(second);
    assert_eq!(registry.active_device_count("account:1"), 0);
}

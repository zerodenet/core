use super::*;

#[test]
fn nft_policy_is_atomic_and_fail_closed() {
    let script = policy_script(
        "zero_killswitch_test",
        "tun0",
        &["0.0.0.0/1".parse().unwrap(), "8000::/1".parse().unwrap()],
        &["192.0.2.1".parse().unwrap(), "2001:db8::1".parse().unwrap()],
        0x1234_abcd,
        true,
    );
    assert!(script.starts_with("delete table inet zero_killswitch_test\n"));
    assert!(script.contains("add table inet zero_killswitch_test\n"));
    assert!(script.contains("oifname \"tun0\" accept"));
    assert!(script.contains("meta mark 0x1234abcd accept"));
    assert!(!script.contains("meta skuid"));
    assert!(script.contains("ip daddr 192.0.2.1 accept"));
    assert!(script.contains("ip6 daddr 2001:db8::1 accept"));
    assert!(script.contains("ip daddr 0.0.0.0/1 reject"));
    assert!(script.ends_with("ip6 daddr 8000::/1 reject\n"));
}

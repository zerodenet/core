use super::*;
#[test]
fn owned_system_dns_keeps_original_nonlocal_sorted_unique_policy_and_local_only_error() {
    let (observed,servers)=parse_dns(b" nameserver[1] : 127.0.0.53\n nameserver[3] : 9.9.9.9\nnameserver\t1.1.1.1 # configured\nnameserver[4] : 9.9.9.9\nnameserver[5] : fe80::1%en0\nnameserver[6] : ff02::1\n");
    assert!(observed);
    assert_eq!(
        servers,
        vec![
            "1.1.1.1".parse::<IpAddr>().unwrap(),
            "9.9.9.9".parse().unwrap()
        ]
    );
    let (observed, local) = parse_dns(b"nameserver[0] : ::1\nnameserver 169.254.1.1\n");
    assert!(observed);
    assert_eq!(
        require_servers(local).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
}
#[test]
fn discovery_keeps_the_existing_first_32_sorted_addresses_without_an_unbounded_intermediate() {
    let mut bytes = String::new();
    for last in (1..=80).rev() {
        bytes.push_str(&format!("nameserver 10.0.0.{last}\n"));
    }
    let (_, servers) = parse_dns(bytes.as_bytes());
    assert_eq!(servers.len(), 32);
    assert_eq!(servers[0], "10.0.0.1".parse::<IpAddr>().unwrap());
    assert_eq!(servers[31], "10.0.0.32".parse::<IpAddr>().unwrap());
}

use crate::fake_ip::FakeIpAllocator;

#[tokio::test]
async fn fake_ip_allocator_rejects_reserved_localhost_names_without_allocating() {
    let allocator = FakeIpAllocator::new(zero_config::FakeIpConfigRef {
        cidr: "198.18.0.0/24",
        ipv6_cidr: Some("fd00::/120"),
        ttl_seconds: 3600,
        max_entries: None,
        exclude_domains: &[],
    })
    .unwrap();
    for name in ["localhost", "LOCALHOST.", "deep.app.localhost"] {
        assert!(allocator.is_excluded(name));
        assert_eq!(allocator.alloc_ipv4(name).await.unwrap(), None);
        assert_eq!(allocator.alloc_ipv6(name).await.unwrap(), None);
    }
    assert_eq!(allocator.stats().await.live_mappings, 0);
    assert!(!allocator.is_excluded("localhost.example"));
    assert!(allocator
        .alloc_ipv4("localhost.example")
        .await
        .unwrap()
        .is_some());
}

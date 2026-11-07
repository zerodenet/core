use super::*;

fn config<'a>(cidr: &'a str, exclusions: &'a [String]) -> FakeIpConfigRef<'a> {
    FakeIpConfigRef {
        cidr,
        ipv6_cidr: None,
        ttl_seconds: 3600,
        max_entries: None,
        exclude_domains: exclusions,
    }
}

#[tokio::test]
async fn normalizes_names_for_forward_and_reverse_lookup() {
    let allocator = FakeIpAllocator::new(config("198.18.0.0/24", &[])).unwrap();
    let first = allocator.alloc_ipv4("Example.COM.").await.unwrap().unwrap();
    let second = allocator.alloc_ipv4("example.com").await.unwrap().unwrap();
    assert_eq!(first, second);
    assert_eq!(
        allocator.lookup(&first).await.as_deref(),
        Some("example.com")
    );
}

#[tokio::test]
async fn ipv4_and_ipv6_share_one_domain_lifecycle_slot() {
    let mut config = config("198.18.0.0/24", &[]);
    config.ipv6_cidr = Some("fd00::/120");
    config.max_entries = Some(1);
    let allocator = FakeIpAllocator::new(config).unwrap();
    let ipv4 = allocator.alloc_ipv4("dual.test").await.unwrap().unwrap();
    let ipv6 = allocator.alloc_ipv6("dual.test").await.unwrap().unwrap();
    assert!(ipv4.is_v4());
    assert!(ipv6.is_v6());
    assert_eq!(allocator.stats().await.live_mappings, 1);
    assert_eq!(allocator.lookup(&ipv4).await.as_deref(), Some("dual.test"));
    assert_eq!(allocator.lookup(&ipv6).await.as_deref(), Some("dual.test"));
}

#[tokio::test]
async fn expires_forward_and_reverse_mappings_consistently() {
    let mut config = config("198.18.0.0/24", &[]);
    config.ttl_seconds = 1;
    let allocator = FakeIpAllocator::new(config).unwrap();
    let ip = allocator.alloc_ipv4("expired.test").await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(allocator.lookup(&ip).await.is_none());
    assert!(allocator.lookup_domain("expired.test").await.is_none());
    assert_eq!(allocator.stats().await.expirations, 1);
}

#[tokio::test]
async fn evicts_lru_mapping_at_configured_capacity() {
    let mut config = config("198.18.0.0/24", &[]);
    config.max_entries = Some(2);
    let allocator = FakeIpAllocator::new(config).unwrap();
    let first = allocator.alloc_ipv4("one.test").await.unwrap().unwrap();
    let second = allocator.alloc_ipv4("two.test").await.unwrap().unwrap();
    let _ = allocator.lookup(&first).await;
    let third = allocator.alloc_ipv4("three.test").await.unwrap().unwrap();
    assert!(allocator.lookup_domain("one.test").await.is_some());
    assert!(allocator.lookup_domain("two.test").await.is_none());
    assert_ne!(third, second, "an evicted address must enter quarantine");
    let stats = allocator.stats().await;
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.retired_addresses, 1);
}

#[tokio::test]
async fn retired_pool_pressure_fails_closed_until_quarantine_expires() {
    let mut config = config("198.18.0.0/30", &[]);
    config.ttl_seconds = 1;
    config.max_entries = Some(1);
    let allocator = FakeIpAllocator::new(config).unwrap();

    let first = allocator.alloc_ipv4("one.test").await.unwrap().unwrap();
    let second = allocator.alloc_ipv4("two.test").await.unwrap().unwrap();
    assert_ne!(first, second);
    assert!(allocator.lookup(&first).await.is_none());

    assert!(allocator.alloc_ipv4("three.test").await.unwrap().is_none());
    assert_eq!(
        allocator.lookup(&second).await.as_deref(),
        Some("two.test"),
        "exhaustion must not evict the last live mapping"
    );
    let stats = allocator.stats().await;
    assert_eq!(stats.retired_addresses, 1);
    assert_eq!(stats.exhaustions, 1);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let third = allocator
        .alloc_ipv4("three.test")
        .await
        .unwrap()
        .expect("expired quarantine should release one address");
    assert_eq!(third, first);
    assert_ne!(third, second);
}

#[tokio::test]
async fn dual_stack_eviction_retires_both_addresses() {
    let mut config = config("198.18.0.0/30", &[]);
    config.ipv6_cidr = Some("fd00::/126");
    config.max_entries = Some(1);
    let allocator = FakeIpAllocator::new(config).unwrap();

    let old_ipv4 = allocator.alloc_ipv4("old.test").await.unwrap().unwrap();
    let old_ipv6 = allocator.alloc_ipv6("old.test").await.unwrap().unwrap();
    let new_ipv4 = allocator.alloc_ipv4("new.test").await.unwrap().unwrap();
    let new_ipv6 = allocator.alloc_ipv6("new.test").await.unwrap().unwrap();

    assert_ne!(new_ipv4, old_ipv4);
    assert_ne!(new_ipv6, old_ipv6);
    assert!(allocator.lookup(&old_ipv4).await.is_none());
    assert!(allocator.lookup(&old_ipv6).await.is_none());
    assert_eq!(allocator.stats().await.retired_addresses, 2);
}

#[test]
fn exclusion_matching_is_normalized_and_suffix_aware() {
    let exclusions = vec!["*.Internal.Example.".to_owned(), "Exact.Test".to_owned()];
    let allocator = FakeIpAllocator::new(config("198.18.0.0/24", &exclusions)).unwrap();
    assert!(allocator.is_excluded("api.internal.example"));
    assert!(allocator.is_excluded("INTERNAL.EXAMPLE."));
    assert!(allocator.is_excluded("exact.test."));
    assert!(!allocator.is_excluded("notinternal.example"));
}

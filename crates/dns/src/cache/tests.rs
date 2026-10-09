use super::*;

fn cache(max_entries: usize) -> DnsCache {
    DnsCache::new(&DnsCacheConfig {
        max_entries,
        max_ttl_seconds: None,
    })
}

#[tokio::test]
async fn separates_a_and_aaaa_entries() {
    let cache = cache(4);
    cache
        .put(
            QueryScope::new(DnsQueryRole::Default, 0),
            "Example.COM.",
            1,
            vec![IpAddress::V4([192, 0, 2, 1])],
            60,
        )
        .await;
    cache
        .put(
            QueryScope::new(DnsQueryRole::Default, 0),
            "example.com",
            28,
            vec![IpAddress::V6([1; 16])],
            60,
        )
        .await;
    assert!(matches!(
        cache
            .get(QueryScope::new(DnsQueryRole::Default, 0), "example.com", 1)
            .await
            .as_deref(),
        Some([IpAddress::V4(_)])
    ));
    assert!(matches!(
        cache
            .get(
                QueryScope::new(DnsQueryRole::Default, 0),
                "EXAMPLE.COM.",
                28
            )
            .await
            .as_deref(),
        Some([IpAddress::V6(_)])
    ));
}

#[tokio::test]
async fn evicts_least_recently_used_entry() {
    let cache = cache(2);
    cache
        .put(
            QueryScope::new(DnsQueryRole::Default, 0),
            "one.test",
            1,
            vec![],
            60,
        )
        .await;
    cache
        .put(
            QueryScope::new(DnsQueryRole::Default, 0),
            "two.test",
            1,
            vec![],
            60,
        )
        .await;
    let _ = cache
        .get(QueryScope::new(DnsQueryRole::Default, 0), "one.test", 1)
        .await;
    cache
        .put(
            QueryScope::new(DnsQueryRole::Default, 0),
            "three.test",
            1,
            vec![],
            60,
        )
        .await;
    assert!(cache
        .get(QueryScope::new(DnsQueryRole::Default, 0), "one.test", 1)
        .await
        .is_some());
    assert!(cache
        .get(QueryScope::new(DnsQueryRole::Default, 0), "two.test", 1)
        .await
        .is_none());
    assert!(cache
        .get(QueryScope::new(DnsQueryRole::Default, 0), "three.test", 1)
        .await
        .is_some());
}

#[tokio::test]
async fn isolates_entries_by_query_role() {
    let cache = cache(4);
    cache
        .put(
            QueryScope::new(DnsQueryRole::Node, 0),
            "shared.test",
            1,
            vec![IpAddress::V4([192, 0, 2, 1])],
            60,
        )
        .await;

    assert!(cache
        .get(QueryScope::new(DnsQueryRole::Direct, 0), "shared.test", 1)
        .await
        .is_none());
}

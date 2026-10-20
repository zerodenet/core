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

#[tokio::test]
async fn final_caller_cancelled_while_cache_lock_is_held_cannot_commit() {
    use crate::coordinator::{QueryCoordinator, QueryKey};
    let cache = cache(4);
    let coordinator = QueryCoordinator::<Vec<IpAddress>>::default();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let scope = QueryScope::new(DnsQueryRole::Default, egress.generation());
    let guard = cache.inner.state.lock().await;
    let (entered, received) = tokio::sync::oneshot::channel();
    let caller = {
        let cache = cache.clone();
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator
                .resolve(QueryKey::new("cancelled.test", 1, scope), async move {
                    entered.send(()).unwrap();
                    cache
                        .put(
                            scope,
                            "cancelled.test",
                            1,
                            vec![IpAddress::V4([192, 0, 2, 10])],
                            60,
                        )
                        .await;
                    Ok(vec![])
                })
                .await
        })
    };
    received.await.unwrap();
    assert!(
        !caller.is_finished(),
        "query is suspended on the actual cache lock"
    );
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(guard);
    coordinator
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(cache.get(scope, "cancelled.test", 1).await.is_none());
}

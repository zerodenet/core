//! Query-type-aware TTL cache with deterministic LRU eviction.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::DnsCacheConfig;
use zero_traits::IpAddress;

use crate::message::normalize_domain;
use crate::DnsQueryRole;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    role: DnsQueryRole,
    domain: String,
    query_type: u16,
}

struct CacheEntry {
    addresses: Vec<IpAddress>,
    raw_query: Option<Vec<u8>>,
    raw_response: Option<Vec<u8>>,
    expires_at: Instant,
    last_used: u64,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    clock: u64,
}

#[derive(Clone)]
pub(crate) struct DnsCache {
    inner: Arc<DnsCacheInner>,
}

struct DnsCacheInner {
    state: Mutex<CacheState>,
    max_entries: usize,
    max_ttl: Option<Duration>,
}

impl DnsCache {
    pub(crate) fn new(config: &DnsCacheConfig) -> Self {
        Self {
            inner: Arc::new(DnsCacheInner {
                state: Mutex::new(CacheState::default()),
                max_entries: config.max_entries,
                max_ttl: config.max_ttl_seconds.map(Duration::from_secs),
            }),
        }
    }

    pub(crate) async fn get(
        &self,
        role: DnsQueryRole,
        domain: &str,
        query_type: u16,
    ) -> Option<Vec<IpAddress>> {
        let domain = normalize_domain(domain).ok()?;
        let key = CacheKey {
            role,
            domain,
            query_type,
        };
        let mut state = self.inner.state.lock().await;
        let now = Instant::now();
        if state
            .entries
            .get(&key)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            state.entries.remove(&key);
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(&key)?;
        entry.last_used = clock;
        Some(entry.addresses.clone())
    }

    pub(crate) async fn get_response(
        &self,
        role: DnsQueryRole,
        domain: &str,
        query_type: u16,
        query: &[u8],
    ) -> Option<Vec<u8>> {
        let domain = normalize_domain(domain).ok()?;
        let key = CacheKey {
            role,
            domain,
            query_type,
        };
        let mut state = self.inner.state.lock().await;
        let now = Instant::now();
        if state
            .entries
            .get(&key)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            state.entries.remove(&key);
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let entry = state.entries.get_mut(&key)?;
        let cached_query = entry.raw_query.as_deref()?;
        if cached_query.get(2..)? != query.get(2..)? {
            return None;
        }
        let mut response = entry.raw_response.clone()?;
        response.get_mut(..2)?.copy_from_slice(query.get(..2)?);
        entry.last_used = clock;
        Some(response)
    }

    pub(crate) async fn inspect(&self, domain: &str) -> Option<(Vec<IpAddress>, u64)> {
        let domain = normalize_domain(domain).ok()?;
        let mut state = self.inner.state.lock().await;
        remove_expired(&mut state);
        let now = Instant::now();
        let matching = state
            .entries
            .iter()
            .filter(|(key, _)| key.domain == domain)
            .collect::<Vec<_>>();
        let ttl = matching
            .iter()
            .map(|(_, entry)| entry.expires_at.duration_since(now).as_secs())
            .min()?;
        let addresses = matching
            .into_iter()
            .flat_map(|(_, entry)| entry.addresses.iter().copied())
            .collect();
        Some((addresses, ttl))
    }

    pub(crate) async fn entries(&self, limit: usize) -> Vec<(String, Vec<IpAddress>, u64)> {
        let mut state = self.inner.state.lock().await;
        remove_expired(&mut state);
        let now = Instant::now();
        let mut grouped: BTreeMap<String, (Vec<IpAddress>, u64)> = BTreeMap::new();
        for (key, entry) in &state.entries {
            let ttl = entry.expires_at.duration_since(now).as_secs();
            let group = grouped
                .entry(key.domain.clone())
                .or_insert_with(|| (Vec::new(), ttl));
            group.0.extend(entry.addresses.iter().copied());
            group.1 = group.1.min(ttl);
        }
        grouped
            .into_iter()
            .take(limit)
            .map(|(domain, (addresses, ttl))| (domain, addresses, ttl))
            .collect()
    }

    pub(crate) async fn put(
        &self,
        role: DnsQueryRole,
        domain: &str,
        query_type: u16,
        addresses: Vec<IpAddress>,
        ttl_seconds: u32,
    ) {
        let Ok(domain) = normalize_domain(domain) else {
            return;
        };
        let effective_ttl = self
            .inner
            .max_ttl
            .map(|max| max.min(Duration::from_secs(u64::from(ttl_seconds))))
            .unwrap_or(Duration::from_secs(u64::from(ttl_seconds)));
        if effective_ttl.is_zero() {
            return;
        }
        self.put_entry(
            role,
            &domain,
            query_type,
            addresses,
            None,
            None,
            effective_ttl,
        )
        .await;
    }

    pub(crate) async fn put_response(
        &self,
        role: DnsQueryRole,
        domain: &str,
        query_type: u16,
        addresses: Vec<IpAddress>,
        query: Vec<u8>,
        response: Vec<u8>,
        ttl_seconds: u32,
    ) {
        let Ok(domain) = normalize_domain(domain) else {
            return;
        };
        let effective_ttl = self
            .inner
            .max_ttl
            .map(|max| max.min(Duration::from_secs(u64::from(ttl_seconds))))
            .unwrap_or(Duration::from_secs(u64::from(ttl_seconds)));
        if effective_ttl.is_zero() {
            return;
        }
        self.put_entry(
            role,
            &domain,
            query_type,
            addresses,
            Some(query),
            Some(response),
            effective_ttl,
        )
        .await;
    }

    async fn put_entry(
        &self,
        role: DnsQueryRole,
        domain: &str,
        query_type: u16,
        addresses: Vec<IpAddress>,
        raw_query: Option<Vec<u8>>,
        raw_response: Option<Vec<u8>>,
        effective_ttl: Duration,
    ) {
        let mut state = self.inner.state.lock().await;
        remove_expired(&mut state);
        let key = CacheKey {
            role,
            domain: domain.to_owned(),
            query_type,
        };
        if !state.entries.contains_key(&key) && state.entries.len() >= self.inner.max_entries {
            if let Some(lru) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            {
                state.entries.remove(&lru);
            }
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let existing_response = state
            .entries
            .get(&key)
            .and_then(|entry| entry.raw_response.clone());
        let existing_query = state
            .entries
            .get(&key)
            .and_then(|entry| entry.raw_query.clone());
        state.entries.insert(
            key,
            CacheEntry {
                addresses,
                raw_query: raw_query.or(existing_query),
                raw_response: raw_response.or(existing_response),
                expires_at: Instant::now() + effective_ttl,
                last_used: clock,
            },
        );
    }
}

fn remove_expired(state: &mut CacheState) {
    let now = Instant::now();
    state.entries.retain(|_, entry| entry.expires_at > now);
}

#[cfg(test)]
mod tests {
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
                DnsQueryRole::Default,
                "Example.COM.",
                1,
                vec![IpAddress::V4([192, 0, 2, 1])],
                60,
            )
            .await;
        cache
            .put(
                DnsQueryRole::Default,
                "example.com",
                28,
                vec![IpAddress::V6([1; 16])],
                60,
            )
            .await;
        assert!(matches!(
            cache
                .get(DnsQueryRole::Default, "example.com", 1)
                .await
                .as_deref(),
            Some([IpAddress::V4(_)])
        ));
        assert!(matches!(
            cache
                .get(DnsQueryRole::Default, "EXAMPLE.COM.", 28)
                .await
                .as_deref(),
            Some([IpAddress::V6(_)])
        ));
    }

    #[tokio::test]
    async fn evicts_least_recently_used_entry() {
        let cache = cache(2);
        cache
            .put(DnsQueryRole::Default, "one.test", 1, vec![], 60)
            .await;
        cache
            .put(DnsQueryRole::Default, "two.test", 1, vec![], 60)
            .await;
        let _ = cache.get(DnsQueryRole::Default, "one.test", 1).await;
        cache
            .put(DnsQueryRole::Default, "three.test", 1, vec![], 60)
            .await;
        assert!(cache
            .get(DnsQueryRole::Default, "one.test", 1)
            .await
            .is_some());
        assert!(cache
            .get(DnsQueryRole::Default, "two.test", 1)
            .await
            .is_none());
        assert!(cache
            .get(DnsQueryRole::Default, "three.test", 1)
            .await
            .is_some());
    }

    #[tokio::test]
    async fn isolates_entries_by_query_role() {
        let cache = cache(4);
        cache
            .put(
                DnsQueryRole::Node,
                "shared.test",
                1,
                vec![IpAddress::V4([192, 0, 2, 1])],
                60,
            )
            .await;

        assert!(cache
            .get(DnsQueryRole::Direct, "shared.test", 1)
            .await
            .is_none());
    }
}

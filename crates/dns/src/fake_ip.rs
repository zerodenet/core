//! Bounded Fake-IP mapping lifecycle for transparent proxying.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::FakeIpConfigRef;
use zero_traits::IpAddress;

use crate::message::normalize_domain;

const DEFAULT_MAX_ENTRIES: usize = 65_536;

pub struct FakeIpAllocator {
    inner: Mutex<AllocatorState>,
    network: ipnet::Ipv4Net,
    ttl: Duration,
    max_entries: usize,
    exclusions: Vec<DomainExclusion>,
    stats: FakeIpCounters,
}

struct AllocatorState {
    next_ip: u32,
    base: u32,
    mask: u32,
    clock: u64,
    forward: HashMap<String, [u8; 4]>,
    reverse: HashMap<[u8; 4], Mapping>,
}

struct Mapping {
    domain: String,
    expires_at: Instant,
    last_used: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeIpStats {
    pub allocations: u64,
    pub expirations: u64,
    pub evictions: u64,
    pub exhaustions: u64,
    pub collisions: u64,
    pub reverse_misses: u64,
    pub live_mappings: usize,
    pub capacity: usize,
}

#[derive(Default)]
struct FakeIpCounters {
    allocations: AtomicU64,
    expirations: AtomicU64,
    evictions: AtomicU64,
    exhaustions: AtomicU64,
    collisions: AtomicU64,
    reverse_misses: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DomainExclusion {
    Exact(String),
    Suffix(String),
}

impl FakeIpAllocator {
    pub fn new(config: FakeIpConfigRef<'_>) -> Result<Self, String> {
        let network = match config
            .cidr
            .parse::<ipnet::IpNet>()
            .map_err(|error| format!("invalid Fake-IP CIDR: {error}"))?
        {
            ipnet::IpNet::V4(network) => network,
            ipnet::IpNet::V6(_) => return Err("Fake-IP only supports IPv4 CIDR".to_owned()),
        };
        if network.prefix_len() > 30 {
            return Err("Fake-IP IPv4 CIDR must contain at least four addresses".to_owned());
        }
        if config.ttl_seconds == 0 {
            return Err("Fake-IP TTL must be greater than zero".to_owned());
        }
        let usable = ((1_u128 << (32 - network.prefix_len())) - 2).min(usize::MAX as u128) as usize;
        let max_entries = config
            .max_entries
            .unwrap_or(DEFAULT_MAX_ENTRIES)
            .min(usable);
        if max_entries == 0
            || config
                .max_entries
                .is_some_and(|configured| configured > usable)
        {
            return Err(format!(
                "Fake-IP max_entries must be between 1 and {usable}"
            ));
        }
        let exclusions = config
            .exclude_domains
            .iter()
            .map(|pattern| parse_exclusion(pattern))
            .collect::<Result<Vec<_>, _>>()?;
        let base = u32::from(network.network());
        Ok(Self {
            inner: Mutex::new(AllocatorState {
                next_ip: base + 1,
                base,
                mask: u32::from(network.netmask()),
                clock: 0,
                forward: HashMap::new(),
                reverse: HashMap::new(),
            }),
            network,
            ttl: Duration::from_secs(config.ttl_seconds),
            max_entries,
            exclusions,
            stats: FakeIpCounters::default(),
        })
    }

    pub fn compatible_with(&self, config: FakeIpConfigRef<'_>) -> bool {
        let Ok(network) = config.cidr.parse::<ipnet::Ipv4Net>() else {
            return false;
        };
        let usable = ((1_u128 << (32 - network.prefix_len())) - 2).min(usize::MAX as u128) as usize;
        let max_entries = config
            .max_entries
            .unwrap_or(DEFAULT_MAX_ENTRIES)
            .min(usable);
        let Ok(exclusions) = config
            .exclude_domains
            .iter()
            .map(|pattern| parse_exclusion(pattern))
            .collect::<Result<Vec<_>, _>>()
        else {
            return false;
        };
        self.network == network
            && self.ttl == Duration::from_secs(config.ttl_seconds)
            && self.max_entries == max_entries
            && self.exclusions == exclusions
    }

    pub fn contains(&self, address: std::net::IpAddr) -> bool {
        matches!(address, std::net::IpAddr::V4(address) if self.network.contains(&address))
    }

    pub fn ttl_seconds(&self) -> u32 {
        self.ttl.as_secs().min(u64::from(u32::MAX)) as u32
    }

    pub fn record_collision(&self) {
        self.stats.collisions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn is_excluded(&self, domain: &str) -> bool {
        let Ok(domain) = normalize_domain(domain) else {
            return true;
        };
        self.exclusions.iter().any(|pattern| match pattern {
            DomainExclusion::Exact(exact) => domain == *exact,
            DomainExclusion::Suffix(suffix) => {
                domain == *suffix || domain.ends_with(&format!(".{suffix}"))
            }
        })
    }

    pub async fn alloc(&self, domain: &str) -> Option<IpAddress> {
        let domain = normalize_domain(domain).ok()?;
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        expire_mappings(&mut state, now, &self.stats);
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;

        if let Some(octets) = state.forward.get(&domain).copied() {
            if let Some(mapping) = state.reverse.get_mut(&octets) {
                mapping.expires_at = now + self.ttl;
                mapping.last_used = clock;
                return Some(IpAddress::V4(octets));
            }
            state.forward.remove(&domain);
        }

        let reusable = if state.reverse.len() >= self.max_entries {
            let lru = state
                .reverse
                .iter()
                .min_by_key(|(_, mapping)| mapping.last_used)
                .map(|(ip, _)| *ip);
            if let Some(ip) = lru {
                remove_mapping(&mut state, ip);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
            lru
        } else {
            None
        };
        let octets = reusable.or_else(|| allocate_free(&mut state));
        let Some(octets) = octets else {
            self.stats.exhaustions.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        state.forward.insert(domain.clone(), octets);
        state.reverse.insert(
            octets,
            Mapping {
                domain,
                expires_at: now + self.ttl,
                last_used: clock,
            },
        );
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        Some(IpAddress::V4(octets))
    }

    pub async fn lookup(&self, ip: &IpAddress) -> Option<String> {
        let IpAddress::V4(octets) = ip else {
            return None;
        };
        if !self.network.contains(&std::net::Ipv4Addr::from(*octets)) {
            return None;
        }
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        if state
            .reverse
            .get(octets)
            .is_some_and(|mapping| mapping.expires_at <= now)
        {
            remove_mapping(&mut state, *octets);
            self.stats.expirations.fetch_add(1, Ordering::Relaxed);
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let Some(mapping) = state.reverse.get_mut(octets) else {
            self.stats.reverse_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        mapping.last_used = clock;
        Some(mapping.domain.clone())
    }

    pub async fn lookup_domain(&self, domain: &str) -> Option<IpAddress> {
        let domain = normalize_domain(domain).ok()?;
        let mut state = self.inner.lock().await;
        let octets = state.forward.get(&domain).copied()?;
        let now = Instant::now();
        if state
            .reverse
            .get(&octets)
            .is_none_or(|mapping| mapping.expires_at <= now)
        {
            remove_mapping(&mut state, octets);
            self.stats.expirations.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        if let Some(mapping) = state.reverse.get_mut(&octets) {
            mapping.last_used = clock;
        }
        Some(IpAddress::V4(octets))
    }

    pub async fn stats(&self) -> FakeIpStats {
        let mut state = self.inner.lock().await;
        expire_mappings(&mut state, Instant::now(), &self.stats);
        FakeIpStats {
            allocations: self.stats.allocations.load(Ordering::Relaxed),
            expirations: self.stats.expirations.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            exhaustions: self.stats.exhaustions.load(Ordering::Relaxed),
            collisions: self.stats.collisions.load(Ordering::Relaxed),
            reverse_misses: self.stats.reverse_misses.load(Ordering::Relaxed),
            live_mappings: state.reverse.len(),
            capacity: self.max_entries,
        }
    }
}

fn parse_exclusion(pattern: &str) -> Result<DomainExclusion, String> {
    let pattern = pattern.trim();
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return normalize_domain(suffix)
            .map(DomainExclusion::Suffix)
            .map_err(|error| error.to_string());
    }
    normalize_domain(pattern)
        .map(DomainExclusion::Exact)
        .map_err(|error| error.to_string())
}

fn allocate_free(state: &mut AllocatorState) -> Option<[u8; 4]> {
    let broadcast = state.base | !state.mask;
    let start = state.next_ip;
    let mut candidate = start;
    loop {
        let octets = candidate.to_be_bytes();
        if !state.reverse.contains_key(&octets) {
            state.next_ip = if candidate >= broadcast - 1 {
                state.base + 1
            } else {
                candidate + 1
            };
            return Some(octets);
        }
        candidate = if candidate >= broadcast - 1 {
            state.base + 1
        } else {
            candidate + 1
        };
        if candidate == start {
            return None;
        }
    }
}

fn expire_mappings(state: &mut AllocatorState, now: Instant, stats: &FakeIpCounters) {
    let expired = state
        .reverse
        .iter()
        .filter(|(_, mapping)| mapping.expires_at <= now)
        .map(|(ip, _)| *ip)
        .collect::<Vec<_>>();
    for ip in expired {
        remove_mapping(state, ip);
        stats.expirations.fetch_add(1, Ordering::Relaxed);
    }
}

fn remove_mapping(state: &mut AllocatorState, ip: [u8; 4]) {
    if let Some(mapping) = state.reverse.remove(&ip) {
        state.forward.remove(&mapping.domain);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config<'a>(cidr: &'a str, exclusions: &'a [String]) -> FakeIpConfigRef<'a> {
        FakeIpConfigRef {
            cidr,
            ttl_seconds: 3600,
            max_entries: None,
            exclude_domains: exclusions,
        }
    }

    #[tokio::test]
    async fn normalizes_names_for_forward_and_reverse_lookup() {
        let allocator = FakeIpAllocator::new(config("198.18.0.0/24", &[])).unwrap();
        let first = allocator.alloc("Example.COM.").await.unwrap();
        let second = allocator.alloc("example.com").await.unwrap();
        assert_eq!(first, second);
        assert_eq!(
            allocator.lookup(&first).await.as_deref(),
            Some("example.com")
        );
    }

    #[tokio::test]
    async fn expires_forward_and_reverse_mappings_consistently() {
        let mut config = config("198.18.0.0/24", &[]);
        config.ttl_seconds = 1;
        let allocator = FakeIpAllocator::new(config).unwrap();
        let ip = allocator.alloc("expired.test").await.unwrap();
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
        let first = allocator.alloc("one.test").await.unwrap();
        let _second = allocator.alloc("two.test").await.unwrap();
        let _ = allocator.lookup(&first).await;
        let _third = allocator.alloc("three.test").await.unwrap();
        assert!(allocator.lookup_domain("one.test").await.is_some());
        assert!(allocator.lookup_domain("two.test").await.is_none());
        assert_eq!(allocator.stats().await.evictions, 1);
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
}

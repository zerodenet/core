//! Bounded Fake-IP mapping lifecycle for transparent proxying.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::FakeIpConfigRef;
use zero_traits::IpAddress;

use crate::message::normalize_domain;

mod persistence;
mod state_path;

use persistence::{
    unix_time_ms, FakeIpPersistence, FakeIpStateLease, PersistedMapping, PersistenceMetadata,
};

pub(crate) use persistence::FakeIpStateLease as StateLease;
pub use state_path::default_fake_ip_state_path;

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
    persistence: Option<FakeIpPersistence>,
}

struct Mapping {
    domain: String,
    expires_at: Instant,
    expires_at_unix_ms: u64,
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
    #[cfg(test)]
    pub fn new(config: FakeIpConfigRef<'_>) -> Result<Self, String> {
        Self::new_with_state(config, None).map_err(|error| error.to_string())
    }

    pub fn new_with_state(
        config: FakeIpConfigRef<'_>,
        state_lease: Option<Arc<FakeIpStateLease>>,
    ) -> io::Result<Self> {
        let network = match config.cidr.parse::<ipnet::IpNet>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid Fake-IP CIDR: {error}"),
            )
        })? {
            ipnet::IpNet::V4(network) => network,
            ipnet::IpNet::V6(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Fake-IP only supports IPv4 CIDR",
                ));
            }
        };
        if network.prefix_len() > 30 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP IPv4 CIDR must contain at least four addresses",
            ));
        }
        if config.ttl_seconds == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP TTL must be greater than zero",
            ));
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Fake-IP max_entries must be between 1 and {usable}"),
            ));
        }
        let exclusions = config
            .exclude_domains
            .iter()
            .map(|pattern| parse_exclusion(pattern))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let base = u32::from(network.network());
        let metadata = PersistenceMetadata {
            cidr: network.to_string(),
            ttl_seconds: config.ttl_seconds,
            max_entries,
            exclusions: exclusions.iter().map(DomainExclusion::as_pattern).collect(),
        };
        let (persistence, recovered) = match state_lease {
            Some(lease) => {
                let (persistence, mappings) = FakeIpPersistence::open(lease, metadata)?;
                (Some(persistence), mappings)
            }
            None => (None, Vec::new()),
        };
        let mut state = AllocatorState {
            next_ip: base + 1,
            base,
            mask: u32::from(network.netmask()),
            clock: 0,
            forward: HashMap::new(),
            reverse: HashMap::new(),
            persistence,
        };
        if let Err(error) = restore_mappings(
            &mut state,
            &network,
            Duration::from_secs(config.ttl_seconds),
            recovered,
        ) {
            tracing::warn!(%error, "discarding invalid Fake-IP persistence mappings");
            state.forward.clear();
            state.reverse.clear();
            if let Some(persistence) = state.persistence.as_mut() {
                persistence.compact(&[])?;
            }
        }
        Ok(Self {
            inner: Mutex::new(state),
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

    pub async fn alloc(&self, domain: &str) -> io::Result<Option<IpAddress>> {
        let domain = normalize_domain(domain)?;
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        expire_mappings(&mut state, now, &self.stats);
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let expires_at_unix_ms = if state.persistence.is_some() {
            unix_time_ms()?.saturating_add(duration_millis(self.ttl))
        } else {
            0
        };

        if let Some(octets) = state.forward.get(&domain).copied() {
            persist_upsert(
                &mut state,
                &PersistedMapping {
                    domain: domain.clone(),
                    ip: octets,
                    expires_at_unix_ms,
                },
            )?;
            if let Some(mapping) = state.reverse.get_mut(&octets) {
                mapping.expires_at = now + self.ttl;
                mapping.expires_at_unix_ms = expires_at_unix_ms;
                mapping.last_used = clock;
                compact_if_needed(&mut state);
                return Ok(Some(IpAddress::V4(octets)));
            }
            state.forward.remove(&domain);
        }

        let reusable = if state.reverse.len() >= self.max_entries {
            let lru = state
                .reverse
                .iter()
                .min_by_key(|(_, mapping)| mapping.last_used)
                .map(|(ip, _)| *ip);
            lru
        } else {
            None
        };
        let octets = reusable.or_else(|| allocate_free(&mut state));
        let Some(octets) = octets else {
            self.stats.exhaustions.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        persist_upsert(
            &mut state,
            &PersistedMapping {
                domain: domain.clone(),
                ip: octets,
                expires_at_unix_ms,
            },
        )?;
        if reusable.is_some() {
            remove_mapping(&mut state, octets);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
        state.forward.insert(domain.clone(), octets);
        state.reverse.insert(
            octets,
            Mapping {
                domain,
                expires_at: now + self.ttl,
                expires_at_unix_ms,
                last_used: clock,
            },
        );
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        compact_if_needed(&mut state);
        Ok(Some(IpAddress::V4(octets)))
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

impl DomainExclusion {
    fn as_pattern(&self) -> String {
        match self {
            Self::Exact(domain) => domain.clone(),
            Self::Suffix(domain) => format!("*.{domain}"),
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

fn persist_upsert(state: &mut AllocatorState, mapping: &PersistedMapping) -> io::Result<()> {
    if let Some(persistence) = state.persistence.as_mut() {
        persistence.append_upsert(mapping)?;
    }
    Ok(())
}

fn compact_if_needed(state: &mut AllocatorState) {
    let should_compact = state
        .persistence
        .as_ref()
        .is_some_and(|persistence| persistence.should_compact(state.reverse.len()));
    if !should_compact {
        return;
    }
    let mut mappings = state
        .reverse
        .iter()
        .map(|(ip, mapping)| {
            (
                mapping.last_used,
                PersistedMapping {
                    domain: mapping.domain.clone(),
                    ip: *ip,
                    expires_at_unix_ms: mapping.expires_at_unix_ms,
                },
            )
        })
        .collect::<Vec<_>>();
    mappings.sort_by_key(|(last_used, _)| *last_used);
    let mappings = mappings
        .into_iter()
        .map(|(_, mapping)| mapping)
        .collect::<Vec<_>>();
    if let Some(persistence) = state.persistence.as_mut() {
        if let Err(error) = persistence.compact(&mappings) {
            tracing::warn!(%error, "failed to compact Fake-IP persistence state");
        }
    }
}

fn restore_mappings(
    state: &mut AllocatorState,
    network: &ipnet::Ipv4Net,
    ttl: Duration,
    mappings: Vec<PersistedMapping>,
) -> io::Result<()> {
    let now_unix_ms = unix_time_ms()?;
    let now = Instant::now();
    let base = u32::from(network.network());
    let broadcast = u32::from(network.broadcast());
    for persisted in mappings {
        let domain = normalize_domain(&persisted.domain)?;
        let address = u32::from_be_bytes(persisted.ip);
        if domain != persisted.domain
            || address <= base
            || address >= broadcast
            || !network.contains(&std::net::Ipv4Addr::from(persisted.ip))
            || state.forward.contains_key(&domain)
            || state.reverse.contains_key(&persisted.ip)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Fake-IP state contains an invalid or duplicate mapping",
            ));
        }
        let remaining_ms = persisted
            .expires_at_unix_ms
            .saturating_sub(now_unix_ms)
            .min(duration_millis(ttl));
        if remaining_ms == 0 {
            continue;
        }
        state.clock = state.clock.wrapping_add(1);
        state.forward.insert(domain.clone(), persisted.ip);
        state.reverse.insert(
            persisted.ip,
            Mapping {
                domain,
                expires_at: now + Duration::from_millis(remaining_ms),
                expires_at_unix_ms: now_unix_ms.saturating_add(remaining_ms),
                last_used: state.clock,
            },
        );
    }
    Ok(())
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
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
        let first = allocator.alloc("Example.COM.").await.unwrap().unwrap();
        let second = allocator.alloc("example.com").await.unwrap().unwrap();
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
        let ip = allocator.alloc("expired.test").await.unwrap().unwrap();
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
        let first = allocator.alloc("one.test").await.unwrap().unwrap();
        let _second = allocator.alloc("two.test").await.unwrap().unwrap();
        let _ = allocator.lookup(&first).await;
        let _third = allocator.alloc("three.test").await.unwrap().unwrap();
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

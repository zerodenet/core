//! Bounded dual-stack Fake-IP mapping lifecycle for transparent proxying.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::FakeIpConfigRef;
use zero_traits::IpAddress;

use crate::message::normalize_domain;

mod management;
mod persistence;
mod retirement;
mod state_path;

use persistence::{
    unix_time_ms, FakeIpPersistence, FakeIpStateLease, PersistedMapping, PersistenceMetadata,
};
use retirement::{
    expire_mappings, persisted_retired_addresses, prune_retired, restore_retired_addresses,
    retire_domain,
};

pub use management::{FakeIpClearResult, FakeIpClearTarget};
pub(crate) use persistence::FakeIpStateLease as StateLease;
pub use state_path::default_fake_ip_state_path;

const DEFAULT_MAX_ENTRIES: usize = 65_536;

pub struct FakeIpAllocator {
    inner: Mutex<AllocatorState>,
    ipv4_network: ipnet::Ipv4Net,
    ipv6_network: Option<ipnet::Ipv6Net>,
    ttl: Duration,
    max_entries: usize,
    exclusions: Vec<DomainExclusion>,
    stats: FakeIpCounters,
}

struct AllocatorState {
    next_ipv4: u32,
    next_ipv6: Option<u128>,
    clock: u64,
    forward: HashMap<String, Mapping>,
    reverse: HashMap<IpAddr, String>,
    retired: HashMap<IpAddr, RetiredAddress>,
    persistence: Option<FakeIpPersistence>,
}

#[derive(Debug, Clone, Copy)]
struct RetiredAddress {
    reusable_at: Instant,
    reusable_after_unix_ms: u64,
}

struct Mapping {
    ipv4: Option<Ipv4Addr>,
    ipv6: Option<Ipv6Addr>,
    expires_at: Instant,
    expires_at_unix_ms: u64,
    last_used: u64,
}

impl Mapping {
    fn address(&self, family: AddressFamily) -> Option<IpAddr> {
        match family {
            AddressFamily::Ipv4 => self.ipv4.map(IpAddr::V4),
            AddressFamily::Ipv6 => self.ipv6.map(IpAddr::V6),
        }
    }

    fn set_address(&mut self, address: IpAddr) {
        match address {
            IpAddr::V4(address) => self.ipv4 = Some(address),
            IpAddr::V6(address) => self.ipv6 = Some(address),
        }
    }

    fn addresses(&self) -> impl Iterator<Item = IpAddr> {
        self.ipv4
            .map(IpAddr::V4)
            .into_iter()
            .chain(self.ipv6.map(IpAddr::V6))
    }
}

#[derive(Clone, Copy)]
enum AddressFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeIpStats {
    pub allocations: u64,
    pub expirations: u64,
    pub evictions: u64,
    pub exhaustions: u64,
    pub collisions: u64,
    pub reverse_misses: u64,
    /// Number of live domains. One domain may own both an IPv4 and IPv6
    /// address while consuming one shared capacity slot.
    pub live_mappings: usize,
    /// Number of synthetic addresses quarantined after their mapping was
    /// removed. Retired addresses cannot be allocated or reverse-resolved.
    pub retired_addresses: usize,
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
        let ipv4_network = config.cidr.parse::<ipnet::Ipv4Net>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid IPv4 Fake-IP CIDR: {error}"),
            )
        })?;
        if ipv4_network.prefix_len() > 30 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP IPv4 CIDR must contain at least four addresses",
            ));
        }
        let ipv6_network = config
            .ipv6_cidr
            .map(|cidr| {
                cidr.parse::<ipnet::Ipv6Net>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid IPv6 Fake-IP CIDR: {error}"),
                    )
                })
            })
            .transpose()?;
        if ipv6_network.is_some_and(|network| network.prefix_len() > 126) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP IPv6 CIDR must contain at least four addresses",
            ));
        }
        if config.ttl_seconds == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP TTL must be greater than zero",
            ));
        }

        let usable = ipv6_network.map_or_else(
            || usable_ipv4(&ipv4_network),
            |network| usable_ipv4(&ipv4_network).min(usable_ipv6(&network)),
        );
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
        let metadata = PersistenceMetadata {
            cidr: ipv4_network.to_string(),
            ipv6_cidr: ipv6_network.map(|network| network.to_string()),
            ttl_seconds: config.ttl_seconds,
            max_entries,
            exclusions: exclusions.iter().map(DomainExclusion::as_pattern).collect(),
        };
        let (persistence, recovered) = match state_lease {
            Some(lease) => {
                let (persistence, mappings) = FakeIpPersistence::open(lease, metadata)?;
                (Some(persistence), mappings)
            }
            None => (None, Default::default()),
        };
        let mut state = AllocatorState {
            next_ipv4: u32::from(ipv4_network.network()) + 1,
            next_ipv6: ipv6_network.map(|network| u128::from(network.network())),
            clock: 0,
            forward: HashMap::new(),
            reverse: HashMap::new(),
            retired: HashMap::new(),
            persistence,
        };
        let restored = restore_mappings(
            &mut state,
            &ipv4_network,
            ipv6_network.as_ref(),
            Duration::from_secs(config.ttl_seconds),
            max_entries,
            recovered.mappings,
        )
        .and_then(|()| {
            restore_retired_addresses(
                &mut state,
                &ipv4_network,
                ipv6_network.as_ref(),
                Duration::from_secs(config.ttl_seconds),
                recovered.retired,
            )
        });
        if let Err(error) = restored {
            tracing::warn!(%error, "discarding invalid Fake-IP persistence state");
            state.forward.clear();
            state.reverse.clear();
            state.retired.clear();
            if let Some(persistence) = state.persistence.as_mut() {
                persistence.compact(&[], &[])?;
            }
        }
        Ok(Self {
            inner: Mutex::new(state),
            ipv4_network,
            ipv6_network,
            ttl: Duration::from_secs(config.ttl_seconds),
            max_entries,
            exclusions,
            stats: FakeIpCounters::default(),
        })
    }

    pub fn compatible_with(&self, config: FakeIpConfigRef<'_>) -> bool {
        let Ok(ipv4_network) = config.cidr.parse::<ipnet::Ipv4Net>() else {
            return false;
        };
        let Ok(ipv6_network) = config
            .ipv6_cidr
            .map(str::parse::<ipnet::Ipv6Net>)
            .transpose()
        else {
            return false;
        };
        let usable = ipv6_network.map_or_else(
            || usable_ipv4(&ipv4_network),
            |network| usable_ipv4(&ipv4_network).min(usable_ipv6(&network)),
        );
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
        self.ipv4_network == ipv4_network
            && self.ipv6_network == ipv6_network
            && self.ttl == Duration::from_secs(config.ttl_seconds)
            && self.max_entries == max_entries
            && self.exclusions == exclusions
    }

    pub fn contains(&self, address: IpAddr) -> bool {
        match address {
            IpAddr::V4(address) => self.ipv4_network.contains(&address),
            IpAddr::V6(address) => self
                .ipv6_network
                .as_ref()
                .is_some_and(|network| network.contains(&address)),
        }
    }

    pub fn ttl_seconds(&self) -> u32 {
        self.ttl.as_secs().min(u64::from(u32::MAX)) as u32
    }

    pub(crate) fn ipv6_enabled(&self) -> bool {
        self.ipv6_network.is_some()
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

    pub async fn alloc_ipv4(&self, domain: &str) -> io::Result<Option<IpAddress>> {
        self.alloc(domain, AddressFamily::Ipv4).await
    }

    pub async fn alloc_ipv6(&self, domain: &str) -> io::Result<Option<IpAddress>> {
        if self.ipv6_network.is_none() {
            return Ok(None);
        }
        self.alloc(domain, AddressFamily::Ipv6).await
    }

    async fn alloc(&self, domain: &str, family: AddressFamily) -> io::Result<Option<IpAddress>> {
        let domain = normalize_domain(domain)?;
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        let now_unix_ms = if state.persistence.is_some() {
            unix_time_ms()?
        } else {
            0
        };
        expire_mappings(&mut state, now, now_unix_ms, self.ttl, &self.stats)?;
        prune_retired(&mut state, now);
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let expires_at_unix_ms = now_unix_ms.saturating_add(duration_millis(self.ttl));

        if let Some(address) = state
            .forward
            .get(&domain)
            .and_then(|mapping| mapping.address(family))
        {
            let addresses = state
                .forward
                .get(&domain)
                .map(|mapping| mapping.addresses().collect::<Vec<_>>())
                .unwrap_or_default();
            persist_addresses(&mut state, &domain, &addresses, expires_at_unix_ms)?;
            if let Some(mapping) = state.forward.get_mut(&domain) {
                mapping.expires_at = now + self.ttl;
                mapping.expires_at_unix_ms = expires_at_unix_ms;
                mapping.last_used = clock;
            }
            compact_if_needed(&mut state);
            return Ok(Some(to_trait_address(address)));
        }

        let Some(address) = allocate_free(
            &mut state,
            family,
            &self.ipv4_network,
            self.ipv6_network.as_ref(),
        ) else {
            self.stats.exhaustions.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };

        let victim =
            if !state.forward.contains_key(&domain) && state.forward.len() >= self.max_entries {
                state
                    .forward
                    .iter()
                    .min_by_key(|(_, mapping)| mapping.last_used)
                    .map(|(domain, _)| domain.clone())
            } else {
                None
            };
        if let Some(victim) = &victim {
            retire_domain(&mut state, victim, now, now_unix_ms, self.ttl)?;
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }

        let mut persisted_addresses = state
            .forward
            .get(&domain)
            .map(|mapping| mapping.addresses().collect::<Vec<_>>())
            .unwrap_or_default();
        persisted_addresses.push(address);
        persist_addresses(
            &mut state,
            &domain,
            &persisted_addresses,
            expires_at_unix_ms,
        )?;

        state.reverse.insert(address, domain.clone());
        match state.forward.entry(domain) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let mapping = entry.get_mut();
                mapping.set_address(address);
                mapping.expires_at = now + self.ttl;
                mapping.expires_at_unix_ms = expires_at_unix_ms;
                mapping.last_used = clock;
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let mut mapping = Mapping {
                    ipv4: None,
                    ipv6: None,
                    expires_at: now + self.ttl,
                    expires_at_unix_ms,
                    last_used: clock,
                };
                mapping.set_address(address);
                entry.insert(mapping);
            }
        }
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        compact_if_needed(&mut state);
        Ok(Some(to_trait_address(address)))
    }

    pub async fn lookup(&self, ip: &IpAddress) -> Option<String> {
        let address = to_std_address(*ip);
        if !self.contains(address) {
            return None;
        }
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        prune_retired(&mut state, now);
        let Some(domain) = state.reverse.get(&address).cloned() else {
            self.stats.reverse_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if state
            .forward
            .get(&domain)
            .is_none_or(|mapping| mapping.expires_at <= now)
        {
            let now_unix_ms = unix_time_ms().unwrap_or_default();
            if let Err(error) = retire_domain(&mut state, &domain, now, now_unix_ms, self.ttl) {
                tracing::warn!(%error, %domain, "failed to persist expired Fake-IP retirement");
            }
            self.stats.expirations.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        if let Some(mapping) = state.forward.get_mut(&domain) {
            mapping.last_used = clock;
        }
        Some(domain)
    }

    pub async fn lookup_domain(&self, domain: &str) -> Option<IpAddress> {
        let domain = normalize_domain(domain).ok()?;
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        prune_retired(&mut state, now);
        if state
            .forward
            .get(&domain)
            .is_none_or(|mapping| mapping.expires_at <= now)
        {
            if state.forward.contains_key(&domain) {
                let now_unix_ms = unix_time_ms().unwrap_or_default();
                if let Err(error) = retire_domain(&mut state, &domain, now, now_unix_ms, self.ttl) {
                    tracing::warn!(%error, %domain, "failed to persist expired Fake-IP retirement");
                }
                self.stats.expirations.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let mapping = state.forward.get_mut(&domain)?;
        mapping.last_used = clock;
        mapping
            .ipv4
            .map(|address| IpAddress::V4(address.octets()))
            .or_else(|| mapping.ipv6.map(|address| IpAddress::V6(address.octets())))
    }

    pub async fn stats(&self) -> FakeIpStats {
        let mut state = self.inner.lock().await;
        let now = Instant::now();
        let now_unix_ms = unix_time_ms().unwrap_or_default();
        if let Err(error) = expire_mappings(&mut state, now, now_unix_ms, self.ttl, &self.stats) {
            tracing::warn!(%error, "failed to persist expired Fake-IP retirements");
        }
        prune_retired(&mut state, now);
        FakeIpStats {
            allocations: self.stats.allocations.load(Ordering::Relaxed),
            expirations: self.stats.expirations.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            exhaustions: self.stats.exhaustions.load(Ordering::Relaxed),
            collisions: self.stats.collisions.load(Ordering::Relaxed),
            reverse_misses: self.stats.reverse_misses.load(Ordering::Relaxed),
            live_mappings: state.forward.len(),
            retired_addresses: state.retired.len(),
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

fn usable_ipv4(network: &ipnet::Ipv4Net) -> usize {
    ((1_u128 << (32 - network.prefix_len())) - 2).min(usize::MAX as u128) as usize
}

fn usable_ipv6(network: &ipnet::Ipv6Net) -> usize {
    let host_bits = 128 - network.prefix_len();
    if u32::from(host_bits) >= usize::BITS {
        usize::MAX
    } else {
        1_usize << host_bits
    }
}

fn allocate_free(
    state: &mut AllocatorState,
    family: AddressFamily,
    ipv4_network: &ipnet::Ipv4Net,
    ipv6_network: Option<&ipnet::Ipv6Net>,
) -> Option<IpAddr> {
    match family {
        AddressFamily::Ipv4 => allocate_free_ipv4(state, ipv4_network).map(IpAddr::V4),
        AddressFamily::Ipv6 => allocate_free_ipv6(state, ipv6_network?).map(IpAddr::V6),
    }
}

fn allocate_free_ipv4(state: &mut AllocatorState, network: &ipnet::Ipv4Net) -> Option<Ipv4Addr> {
    let base = u32::from(network.network());
    let last = u32::from(network.broadcast()) - 1;
    let start = state.next_ipv4;
    let mut candidate = start;
    loop {
        let address = Ipv4Addr::from(candidate);
        if !state.reverse.contains_key(&IpAddr::V4(address))
            && !state.retired.contains_key(&IpAddr::V4(address))
        {
            state.next_ipv4 = if candidate >= last {
                base + 1
            } else {
                candidate + 1
            };
            return Some(address);
        }
        candidate = if candidate >= last {
            base + 1
        } else {
            candidate + 1
        };
        if candidate == start {
            return None;
        }
    }
}

fn allocate_free_ipv6(state: &mut AllocatorState, network: &ipnet::Ipv6Net) -> Option<Ipv6Addr> {
    let base = u128::from(network.network());
    let last = u128::from(network.broadcast());
    let start = state.next_ipv6?;
    let mut candidate = start;
    loop {
        let address = Ipv6Addr::from(candidate);
        if !state.reverse.contains_key(&IpAddr::V6(address))
            && !state.retired.contains_key(&IpAddr::V6(address))
        {
            state.next_ipv6 = Some(if candidate >= last {
                base
            } else {
                candidate + 1
            });
            return Some(address);
        }
        candidate = if candidate >= last {
            base
        } else {
            candidate + 1
        };
        if candidate == start {
            return None;
        }
    }
}

fn persist_addresses(
    state: &mut AllocatorState,
    domain: &str,
    addresses: &[IpAddr],
    expires_at_unix_ms: u64,
) -> io::Result<()> {
    if let Some(persistence) = state.persistence.as_mut() {
        for address in addresses {
            persistence.append_upsert(&PersistedMapping {
                domain: domain.to_owned(),
                ip: *address,
                expires_at_unix_ms,
            })?;
        }
    }
    Ok(())
}

fn compact_if_needed(state: &mut AllocatorState) {
    let live_records = state.forward.len().saturating_add(state.retired.len());
    let should_compact = state
        .persistence
        .as_ref()
        .is_some_and(|persistence| persistence.should_compact(live_records));
    if !should_compact {
        return;
    }
    let mappings = persisted_mappings(state, None);
    let retired = persisted_retired_addresses(state);
    if let Some(persistence) = state.persistence.as_mut() {
        if let Err(error) = persistence.compact(&mappings, &retired) {
            tracing::warn!(%error, "failed to compact Fake-IP persistence state");
        }
    }
}

fn persisted_mappings(
    state: &AllocatorState,
    excluded_domains: Option<&HashSet<String>>,
) -> Vec<PersistedMapping> {
    let mut mappings = state
        .forward
        .iter()
        .filter(|(domain, _)| {
            excluded_domains.is_none_or(|excluded| !excluded.contains(domain.as_str()))
        })
        .flat_map(|(domain, mapping)| {
            mapping.addresses().map(|ip| {
                (
                    mapping.last_used,
                    PersistedMapping {
                        domain: domain.clone(),
                        ip,
                        expires_at_unix_ms: mapping.expires_at_unix_ms,
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    mappings.sort_by_key(|(last_used, _)| *last_used);
    mappings
        .into_iter()
        .map(|(_, mapping)| mapping)
        .collect::<Vec<_>>()
}

fn restore_mappings(
    state: &mut AllocatorState,
    ipv4_network: &ipnet::Ipv4Net,
    ipv6_network: Option<&ipnet::Ipv6Net>,
    ttl: Duration,
    max_entries: usize,
    mappings: Vec<PersistedMapping>,
) -> io::Result<()> {
    let now_unix_ms = unix_time_ms()?;
    let now = Instant::now();
    for persisted in mappings {
        let domain = normalize_domain(&persisted.domain)?;
        let in_pool = match persisted.ip {
            IpAddr::V4(address) => {
                ipv4_network.contains(&address)
                    && address != ipv4_network.network()
                    && address != ipv4_network.broadcast()
            }
            IpAddr::V6(address) => ipv6_network.is_some_and(|network| network.contains(&address)),
        };
        if domain != persisted.domain || !in_pool || state.reverse.contains_key(&persisted.ip) {
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
        let expires_at_unix_ms = now_unix_ms.saturating_add(remaining_ms);
        let clock = state.clock;
        let mapping = state
            .forward
            .entry(domain.clone())
            .or_insert_with(|| Mapping {
                ipv4: None,
                ipv6: None,
                expires_at: now + Duration::from_millis(remaining_ms),
                expires_at_unix_ms,
                last_used: clock,
            });
        if mapping.address(persisted.ip.into()).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Fake-IP state contains a duplicate domain family",
            ));
        }
        mapping.set_address(persisted.ip);
        mapping.expires_at = mapping
            .expires_at
            .min(now + Duration::from_millis(remaining_ms));
        mapping.expires_at_unix_ms = mapping.expires_at_unix_ms.min(expires_at_unix_ms);
        mapping.last_used = clock;
        state.reverse.insert(persisted.ip, domain);
    }
    if state.forward.len() > max_entries {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Fake-IP state exceeds configured domain capacity",
        ));
    }
    Ok(())
}

impl From<IpAddr> for AddressFamily {
    fn from(address: IpAddr) -> Self {
        match address {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }
}

fn to_trait_address(address: IpAddr) -> IpAddress {
    match address {
        IpAddr::V4(address) => IpAddress::V4(address.octets()),
        IpAddr::V6(address) => IpAddress::V6(address.octets()),
    }
}

fn to_std_address(address: IpAddress) -> IpAddr {
    match address {
        IpAddress::V4(octets) => IpAddr::V4(Ipv4Addr::from(octets)),
        IpAddress::V6(octets) => IpAddr::V6(Ipv6Addr::from(octets)),
    }
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
}

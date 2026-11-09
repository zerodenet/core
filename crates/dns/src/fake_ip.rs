//! Bounded dual-stack Fake-IP mapping lifecycle for transparent proxying.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::FakeIpConfigRef;
use zero_traits::IpAddress;

use crate::message::normalize_domain;

mod allocation;
mod checkpoint;
mod construction;
mod helpers;
mod observation;
use helpers::*;
mod journal;
mod management;
pub(crate) mod persistence;
mod retirement;
mod state_path;

use persistence::{
    unix_time_ms, FakeIpPersistence, FakeIpStateLease, PersistedMapping, PersistenceMetadata,
};
use retirement::{
    expire_mappings, prune_retired, restore_retired_addresses, retire_domain,
    retire_domain_in_memory,
};

pub use management::{FakeIpClearResult, FakeIpClearTarget};
pub(crate) use persistence::FakeIpStateLease as StateLease;
pub use persistence::{FakeIpStateOpenFailure, JournalGenerationExhausted};
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

#[cfg(test)]
#[path = "tests/fake_ip.rs"]
mod tests;

//! Bounded real-IP reverse index for transparent traffic target recovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zero_config::DnsReverseMappingConfig;
use zero_traits::IpAddress;

use crate::message::normalize_domain;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealIpReverseLookup {
    Missing,
    Ambiguous,
    Resolved(String),
}

#[derive(Clone)]
pub(crate) struct RealIpReverseIndex {
    inner: Arc<ReverseIndexInner>,
}

struct ReverseIndexInner {
    state: Mutex<ReverseIndexState>,
    max_entries: usize,
    max_domains_per_address: usize,
    max_ttl: Duration,
}

#[derive(Default)]
struct ReverseIndexState {
    entries: HashMap<IpAddress, ReverseEntry>,
    clock: u64,
}

struct ReverseEntry {
    candidates: Vec<DomainCandidate>,
    last_used: u64,
}

struct DomainCandidate {
    domain: String,
    expires_at: Instant,
    last_observed: u64,
}

impl RealIpReverseIndex {
    pub(crate) fn new(config: &DnsReverseMappingConfig) -> Self {
        Self {
            inner: Arc::new(ReverseIndexInner {
                state: Mutex::new(ReverseIndexState::default()),
                max_entries: config.max_entries,
                max_domains_per_address: config.max_domains_per_address,
                max_ttl: Duration::from_secs(config.max_ttl_seconds),
            }),
        }
    }

    pub(crate) fn compatible_with(&self, config: &DnsReverseMappingConfig) -> bool {
        self.inner.max_entries == config.max_entries
            && self.inner.max_domains_per_address == config.max_domains_per_address
            && self.inner.max_ttl == Duration::from_secs(config.max_ttl_seconds)
    }

    pub(crate) async fn record(&self, domain: &str, addresses: &[IpAddress], ttl_seconds: u32) {
        let Ok(domain) = normalize_domain(domain) else {
            return;
        };
        let ttl = self
            .inner
            .max_ttl
            .min(Duration::from_secs(u64::from(ttl_seconds)));
        if ttl.is_zero() || addresses.is_empty() {
            return;
        }

        let mut state = self.inner.state.lock().await;
        remove_expired(&mut state);
        for address in addresses.iter().copied() {
            if !state.entries.contains_key(&address)
                && state.entries.len() >= self.inner.max_entries
            {
                evict_lru_address(&mut state);
            }
            state.clock = state.clock.wrapping_add(1);
            let clock = state.clock;
            let entry = state
                .entries
                .entry(address)
                .or_insert_with(|| ReverseEntry {
                    candidates: Vec::new(),
                    last_used: clock,
                });
            entry.last_used = clock;
            if let Some(candidate) = entry
                .candidates
                .iter_mut()
                .find(|candidate| candidate.domain == domain)
            {
                candidate.expires_at = Instant::now() + ttl;
                candidate.last_observed = clock;
                continue;
            }
            if entry.candidates.len() >= self.inner.max_domains_per_address {
                if let Some(index) = entry
                    .candidates
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, candidate)| candidate.last_observed)
                    .map(|(index, _)| index)
                {
                    entry.candidates.swap_remove(index);
                }
            }
            entry.candidates.push(DomainCandidate {
                domain: domain.clone(),
                expires_at: Instant::now() + ttl,
                last_observed: clock,
            });
        }
    }

    pub(crate) async fn lookup(&self, address: IpAddress) -> RealIpReverseLookup {
        let mut state = self.inner.state.lock().await;
        remove_expired(&mut state);
        state.clock = state.clock.wrapping_add(1);
        let clock = state.clock;
        let Some(entry) = state.entries.get_mut(&address) else {
            return RealIpReverseLookup::Missing;
        };
        entry.last_used = clock;
        match entry.candidates.as_slice() {
            [candidate] => RealIpReverseLookup::Resolved(candidate.domain.clone()),
            [] => RealIpReverseLookup::Missing,
            _ => RealIpReverseLookup::Ambiguous,
        }
    }
}

fn remove_expired(state: &mut ReverseIndexState) {
    let now = Instant::now();
    state.entries.retain(|_, entry| {
        entry
            .candidates
            .retain(|candidate| candidate.expires_at > now);
        !entry.candidates.is_empty()
    });
}

fn evict_lru_address(state: &mut ReverseIndexState) {
    if let Some(address) = state
        .entries
        .iter()
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(address, _)| *address)
    {
        state.entries.remove(&address);
    }
}

#[cfg(test)]
mod tests;

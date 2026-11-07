use super::persistence::unix_time_ms;
use super::retirement::{expire_mappings, prune_retired};
use super::{duration_millis, to_std_address, FakeIpAllocator, RetiredAddress};
use crate::message::normalize_domain;
use std::{io, time::Instant};
use zero_traits::IpAddress;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeIpClearTarget {
    All,
    Domain(String),
    Address(IpAddress),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FakeIpClearResult {
    /// Number of domain mappings removed. A dual-stack domain counts once.
    pub removed_mappings: usize,
    /// Number of IPv4 and IPv6 reverse-map entries removed.
    pub removed_addresses: usize,
    pub live_mappings: usize,
    pub retired_addresses: usize,
}

impl FakeIpAllocator {
    pub(crate) async fn clear(&self, target: FakeIpClearTarget) -> io::Result<FakeIpClearResult> {
        let clear_all = matches!(&target, FakeIpClearTarget::All);
        let mut state = self.inner.lock().await;
        if state
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.paused())
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Fake-IP journal mutation is paused",
            ));
        }
        let now = Instant::now();
        let now_unix_ms = if state.persistence.is_some() {
            unix_time_ms()?
        } else {
            0
        };
        expire_mappings(&mut state, now, now_unix_ms, self.ttl, &self.stats)?;
        prune_retired(&mut state, now);
        let selected = match target {
            FakeIpClearTarget::All => ClearSelection::All,
            FakeIpClearTarget::Domain(domain) => ClearSelection::Domain(normalize_domain(&domain)?),
            FakeIpClearTarget::Address(address) => ClearSelection::Address(to_std_address(address)),
        };
        let removed_mappings = state
            .forward
            .iter()
            .filter(|(domain, mapping)| selected.matches(domain, mapping))
            .count();
        let removed_addresses = state
            .forward
            .iter()
            .filter(|(domain, mapping)| selected.matches(domain, mapping))
            .map(|(_, mapping)| mapping.addresses().count())
            .sum();
        let retirement = RetiredAddress {
            reusable_at: now + self.ttl,
            reusable_after_unix_ms: now_unix_ms.saturating_add(duration_millis(self.ttl)),
        };
        if clear_all || removed_mappings != 0 {
            if let Some(persistence) = &state.persistence {
                let checkpoint = super::checkpoint::Checkpoint::clearing(
                    &state,
                    &selected,
                    retirement.reusable_after_unix_ms,
                )?;
                persistence.compact_checkpoint(&checkpoint)?;
            }
            let super::AllocatorState {
                forward,
                reverse,
                retired,
                ..
            } = &mut *state;
            forward.retain(|domain, mapping| {
                if !selected.matches(domain, mapping) {
                    return true;
                }
                for address in mapping.addresses() {
                    reverse.remove(&address);
                    retired.insert(address, retirement);
                }
                false
            });
        }
        Ok(FakeIpClearResult {
            removed_mappings,
            removed_addresses,
            live_mappings: state.forward.len(),
            retired_addresses: state.retired.len(),
        })
    }
}

pub(super) enum ClearSelection {
    All,
    Domain(String),
    Address(std::net::IpAddr),
}
impl ClearSelection {
    pub(super) fn matches(&self, domain: &str, mapping: &super::Mapping) -> bool {
        match self {
            Self::All => true,
            Self::Domain(selected) => selected == domain,
            Self::Address(address) => mapping.addresses().any(|actual| actual == *address),
        }
    }
}

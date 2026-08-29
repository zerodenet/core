use std::collections::HashSet;
use std::io;
use std::time::Instant;

use zero_traits::IpAddress;

use super::persistence::{unix_time_ms, PersistedRetiredAddress};
use super::retirement::{
    expire_mappings, persisted_retired_addresses, prune_retired, retire_domain_in_memory,
};
use super::{duration_millis, persisted_mappings, to_std_address, FakeIpAllocator, RetiredAddress};
use crate::message::normalize_domain;

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
        let now = Instant::now();
        let now_unix_ms = if state.persistence.is_some() {
            unix_time_ms()?
        } else {
            0
        };
        expire_mappings(&mut state, now, now_unix_ms, self.ttl, &self.stats)?;
        prune_retired(&mut state, now);
        let domains = match target {
            FakeIpClearTarget::All => state.forward.keys().cloned().collect::<HashSet<_>>(),
            FakeIpClearTarget::Domain(domain) => {
                let domain = normalize_domain(&domain)?;
                state
                    .forward
                    .contains_key(&domain)
                    .then_some(domain)
                    .into_iter()
                    .collect()
            }
            FakeIpClearTarget::Address(address) => state
                .reverse
                .get(&to_std_address(address))
                .cloned()
                .into_iter()
                .collect(),
        };
        let removed_addresses = domains
            .iter()
            .filter_map(|domain| state.forward.get(domain))
            .map(|mapping| mapping.addresses().count())
            .sum();

        // Rewrite persistence before changing memory so a failed journal
        // transaction cannot resurrect a mapping that the command reported as
        // removed. An all-cache clear compacts even an already-empty allocator
        // to discard historical upserts from the journal.
        if clear_all || !domains.is_empty() {
            let mappings = persisted_mappings(&state, Some(&domains));
            let retirement = RetiredAddress {
                reusable_at: now + self.ttl,
                reusable_after_unix_ms: now_unix_ms.saturating_add(duration_millis(self.ttl)),
            };
            let mut retired = persisted_retired_addresses(&state);
            retired.extend(domains.iter().flat_map(|domain| {
                state
                    .forward
                    .get(domain)
                    .into_iter()
                    .flat_map(|mapping| mapping.addresses())
                    .map(|ip| PersistedRetiredAddress {
                        ip,
                        reusable_after_unix_ms: retirement.reusable_after_unix_ms,
                    })
            }));
            retired.sort_by_key(|entry| (entry.reusable_after_unix_ms, entry.ip));
            if let Some(persistence) = state.persistence.as_mut() {
                persistence.compact(&mappings, &retired)?;
            }
            for domain in &domains {
                retire_domain_in_memory(&mut state, domain, retirement);
            }
        }
        Ok(FakeIpClearResult {
            removed_mappings: domains.len(),
            removed_addresses,
            live_mappings: state.forward.len(),
            retired_addresses: state.retired.len(),
        })
    }
}

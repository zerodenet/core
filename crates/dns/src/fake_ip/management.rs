use std::collections::HashSet;
use std::io;
use std::time::Instant;

use zero_traits::IpAddress;

use super::{expire_mappings, persisted_mappings, remove_domain, to_std_address, FakeIpAllocator};
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
}

impl FakeIpAllocator {
    pub(crate) async fn clear(&self, target: FakeIpClearTarget) -> io::Result<FakeIpClearResult> {
        let clear_all = matches!(&target, FakeIpClearTarget::All);
        let mut state = self.inner.lock().await;
        expire_mappings(&mut state, Instant::now(), &self.stats);
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
            if let Some(persistence) = state.persistence.as_mut() {
                persistence.compact(&mappings)?;
            }
        }
        for domain in &domains {
            remove_domain(&mut state, domain);
        }
        Ok(FakeIpClearResult {
            removed_mappings: domains.len(),
            removed_addresses,
            live_mappings: state.forward.len(),
        })
    }
}

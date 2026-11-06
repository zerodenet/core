//! Real DNS requests and warm carrier loans quiesce in the existing owners.
use crate::{DnsSystem, native::NativeDnsProvider, query_tasks::QueryPause};
use std::{collections::BTreeSet, io, sync::Arc};
use zero_core::component::ComponentInstanceId;

/// Reversible admission fence. Dropping it restores only query registration;
/// actual retired clients stay closed, and unknown originals remain in their
/// original provider/pool. Keep it in the same serialized replacement owner.
#[must_use]
pub struct DnsDonorPause {
    _queries: Vec<QueryPause>,
    _providers: Vec<Arc<dyn NativeDnsProvider>>,
    #[cfg(feature = "doh")]
    owners: Vec<Arc<crate::backends::DohDriverOwner>>,
    #[cfg(feature = "doh")]
    replaced: Vec<ComponentInstanceId>,
}
impl DnsDonorPause {
    /// No new owner, driver or executor: retire selected existing clients and
    /// consume their actual native/loan, original joins and registration ACKs.
    pub async fn retire(&self, end: tokio::time::Instant) -> io::Result<()> {
        #[cfg(feature = "doh")]
        {
            let mut failure = None;
            for owner in &self.owners {
                if let Err(error) = owner.retire_instances(&self.replaced, end).await {
                    failure.get_or_insert(error);
                }
            }
            // Each original owner keeps every unknown/faulty client and real
            // Critical task failure; returning the first does not discard them.
            failure.map_or(Ok(()), Err)
        }
        #[cfg(not(feature = "doh"))]
        {
            let _ = end;
            Ok(())
        }
    }
}
impl DnsSystem {
    fn donor_providers(&self) -> Vec<Arc<dyn NativeDnsProvider>> {
        let mut result = Vec::new();
        for source in self
            .native_source
            .as_ref()
            .into_iter()
            .chain(self.query_coordinator.native_source())
            .chain(self.wire_query_coordinator.native_source())
            .chain(self.node_queries.as_ref().map(|node| &node.source))
        {
            if let Ok(provider) = source.provider() {
                if !result
                    .iter()
                    .any(|old: &Arc<dyn NativeDnsProvider>| Arc::ptr_eq(old, &provider))
                {
                    result.push(provider);
                }
            }
        }
        result
    }
    /// Actual installed query owners and retained LoanOwner ancestor instances.
    /// Cold configured tags are not evidence and this grants no IO authority.
    pub fn native_donor_instances(&self) -> io::Result<Vec<ComponentInstanceId>> {
        let mut instances = BTreeSet::new();
        for provider in self.donor_providers() {
            if let Some((instance, _scope)) = provider.host_scope() {
                instances.insert(instance);
            }
        }
        #[cfg(feature = "doh")]
        for owner in self.transport_owners.actual_doh_owners() {
            instances.extend(owner.retained_instances()?);
        }
        Ok(instances.into_iter().collect())
    }
    /// Only a real old-instance intersection pauses DNS. Unrelated edits leave
    /// every healthy pool/query running. The actual two purposes can share one
    /// canonical DoH driver, so both installed query registrations are fenced
    /// before inspecting zero leases or retiring any matching warm client.
    pub fn try_pause_donor_replacement(
        &self,
        replaced: &[ComponentInstanceId],
    ) -> io::Result<DnsDonorPause> {
        let mut guard = DnsDonorPause {
            _queries: Vec::new(),
            _providers: Vec::new(),
            #[cfg(feature = "doh")]
            owners: Vec::new(),
            #[cfg(feature = "doh")]
            replaced: replaced.to_vec(),
        };
        if replaced.is_empty()
            || !self
                .native_donor_instances()?
                .iter()
                .any(|actual| replaced.contains(actual))
        {
            return Ok(guard);
        }
        let providers = self.donor_providers();
        for provider in &providers {
            let tasks = provider.tasks();
            if !guard._queries.iter().any(|old| old.same_owner(&tasks)) {
                guard._queries.push(tasks.try_pause_queries()?);
            }
        }
        guard._providers = providers;
        #[cfg(feature = "doh")]
        for owner in self.transport_owners.actual_doh_owners() {
            if owner
                .retained_instances()?
                .iter()
                .any(|actual| replaced.contains(actual))
            {
                // Cross-purpose sharing requires these same actual sources,
                // not an equal budget or fabricated Scope identity.
                for tasks in owner.source_tasks()? {
                    if !guard._queries.iter().any(|old| old.same_owner(&tasks)) {
                        guard._queries.push(tasks.try_pause_queries()?);
                    }
                }
                guard.owners.push(owner);
            }
        }
        Ok(guard)
    }
}

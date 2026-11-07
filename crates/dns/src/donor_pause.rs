//! Real DNS requests and warm carrier loans quiesce in the existing owners.
use crate::{native::NativeDnsProvider, query_tasks::QueryPause, DnsSystem};
use std::{collections::BTreeSet, io, sync::Arc};
use zero_core::component::ComponentInstanceId;

/// Reversible admission fence. Dropping it restores only query registration;
/// actual retired clients stay closed, and unknown originals remain in their
/// original provider/pool. Keep it in the same serialized replacement owner.
#[must_use]
pub struct DnsDonorPause {
    _queries: Vec<QueryPause>,
    _providers: Vec<Arc<dyn NativeDnsProvider>>,
    _fake_ip: Option<crate::fake_ip::persistence::JournalPause>,
    #[cfg(feature = "doh")]
    owners: Vec<Arc<crate::backends::DohDriverOwner>>,
    replaced: Vec<ComponentInstanceId>,
    cancel_sources: Vec<Arc<crate::native::NativeDnsSource>>,
    // A retry checkpoint set only after all selected Query/Blocking actual
    // joins, original native custody and registration ACKs were observed.
    queries_retired: std::sync::atomic::AtomicBool,
}
impl DnsDonorPause {
    pub(crate) fn has_fake_ip_journal(&self) -> bool {
        self._fake_ip.is_some()
    }
    pub(crate) fn matches_replacement(&self, replaced: &[ComponentInstanceId]) -> bool {
        self.replaced
            == replaced
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
    }
    pub(crate) fn cancel_queries(&self, dns: &DnsSystem) -> io::Result<()> {
        if self
            .queries_retired
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        for source in &self.cancel_sources {
            let provider = source.provider()?;
            if !self
                ._providers
                .iter()
                .any(|actual| Arc::ptr_eq(actual, &provider))
            {
                return Err(io::Error::other(
                    "actual DNS query source changed before retirement",
                ));
            }
        }
        for source in &self.cancel_sources {
            dns.retire_native_query_source(source);
        }
        // Cancel every selected original before awaiting one purpose: a
        // Default finalizer can legitimately wait its real Node descendants.
        for query in &self._queries {
            query.cancel_queries()?;
        }
        Ok(())
    }
    /// No new owner, driver or executor: retire selected existing clients and
    /// consume their actual native/loan, original joins and registration ACKs.
    pub async fn retire(&self, end: tokio::time::Instant) -> io::Result<()> {
        // Query cancellation closes only original per-request custody. Pool
        // drivers remain alive until all those actual users have returned.
        if !self
            .queries_retired
            .load(std::sync::atomic::Ordering::Acquire)
        {
            let mut failure = None;
            for query in &self._queries {
                if let Err(source) = query.settle_queries(end).await {
                    failure.get_or_insert(source);
                }
            }
            if let Some(source) = failure {
                return Err(source);
            }
            self.queries_retired
                .store(true, std::sync::atomic::Ordering::Release);
        }
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
    fn donor_sources(&self) -> Vec<Arc<crate::native::NativeDnsSource>> {
        let mut sources = Vec::new();
        for source in self
            .native_source
            .as_ref()
            .into_iter()
            .chain(self.query_coordinator.native_source())
            .chain(self.wire_query_coordinator.native_source())
            .chain(self.node_queries.as_ref().map(|node| &node.source))
        {
            if !sources.iter().any(|actual| Arc::ptr_eq(actual, source)) {
                sources.push(source.clone());
            }
        }
        sources
    }
    fn donor_providers(&self) -> Vec<Arc<dyn NativeDnsProvider>> {
        let mut providers = Vec::new();
        for source in self.donor_sources() {
            if let Ok(provider) = source.provider() {
                if !providers
                    .iter()
                    .any(|actual| Arc::ptr_eq(actual, &provider))
                {
                    providers.push(provider);
                }
            }
        }
        providers
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
        if let Some(lease) = self
            .fake_ip_state_lease
            .lock()
            .expect("Fake-IP lease poisoned")
            .as_ref()
        {
            if lease.bound() {
                if let Some((instance, _)) = lease.grant()?.host_scope() {
                    instances.insert(instance);
                }
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
        self.pause_donor_replacement(replaced, false)
    }
    pub(crate) fn pause_donor_replacement(
        &self,
        replaced: &[ComponentInstanceId],
        cancel_queries: bool,
    ) -> io::Result<DnsDonorPause> {
        let mut guard = DnsDonorPause {
            _queries: Vec::new(),
            _providers: Vec::new(),
            _fake_ip: None,
            cancel_sources: Vec::new(),
            queries_retired: Default::default(),
            #[cfg(feature = "doh")]
            owners: Vec::new(),
            replaced: replaced
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        };
        if replaced.is_empty()
            || !self
                .native_donor_instances()?
                .iter()
                .any(|actual| replaced.contains(actual))
        {
            return Ok(guard);
        }
        guard._fake_ip = self.pause_fake_ip_journal(replaced)?;
        let mut selected_tasks: Vec<crate::OwnedDnsQueryTasks> = Vec::new();
        #[cfg(feature = "doh")]
        for owner in self.transport_owners.actual_doh_owners() {
            if owner
                .retained_instances()?
                .iter()
                .any(|actual| replaced.contains(actual))
            {
                // Canonical cross-purpose pool sharing retains the real source
                // pair, never another equal budget or a fabricated Scope.
                for tasks in owner.source_tasks()? {
                    if !selected_tasks
                        .iter()
                        .any(|old: &crate::OwnedDnsQueryTasks| old.same_owner(&tasks))
                    {
                        selected_tasks.push(tasks);
                    }
                }
                guard.owners.push(owner);
            }
        }
        for provider in self.donor_providers() {
            let tasks = provider.tasks();
            if provider
                .host_scope()
                .is_some_and(|(instance, _)| replaced.contains(&instance))
                || selected_tasks
                    .iter()
                    .any(|actual| actual.same_owner(&tasks))
            {
                if !selected_tasks
                    .iter()
                    .any(|actual| actual.same_owner(&tasks))
                {
                    selected_tasks.push(tasks);
                }
                guard._providers.push(provider);
            }
        }
        for tasks in selected_tasks {
            guard._queries.push(if cancel_queries {
                tasks.pause_queries_for_profile_change()?
            } else {
                tasks.try_pause_queries()?
            });
        }
        if cancel_queries {
            for source in self.donor_sources() {
                if let Ok(provider) = source.provider() {
                    if guard
                        ._providers
                        .iter()
                        .any(|actual| Arc::ptr_eq(actual, &provider))
                    {
                        guard.cancel_sources.push(source);
                    }
                }
            }
        }
        Ok(guard)
    }
}

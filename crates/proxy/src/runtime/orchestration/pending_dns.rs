//! One exact active candidate survives cancellation of a reconciliation poll.
use super::state::OrchestrationState;
use crate::{component::catalog::ComponentPlan, runtime::Proxy};
use std::{collections::BTreeSet, io, sync::Arc};
use zero_engine::EngineRuntimeSnapshot;
pub(super) struct PendingDnsReload {
    snapshot: Arc<EngineRuntimeSnapshot>,
    components: Arc<ComponentPlan>,
    resources: zero_dns::DnsReloadResources,
}
impl OrchestrationState {
    pub(super) fn prepare_dns_for_reload(
        &mut self,
        proxy: &Proxy,
        snapshot: &Arc<EngineRuntimeSnapshot>,
        components: &Arc<ComponentPlan>,
        affected: &BTreeSet<String>,
    ) -> io::Result<()> {
        if let Some(pending) = &self.pending_dns_reload {
            return if Arc::ptr_eq(snapshot, &pending.snapshot)
                && Arc::ptr_eq(components, &pending.components)
            {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "another actual host DNS/component candidate is still pending",
                ))
            };
        }
        let config = snapshot.config();
        let dispatch = config
            .compile_dns_dispatch()
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidInput, source))?;
        let resources = proxy.resolver.prepare_reload_resources_with_dispatch(
            config.runtime.dns.as_ref(),
            dispatch,
            affected,
        )?;
        // No await or publication separates preparing the real backend map
        // from retaining its exact host snapshot/component association.
        self.pending_dns_reload = Some(PendingDnsReload {
            snapshot: snapshot.clone(),
            components: components.clone(),
            resources,
        });
        Ok(())
    }
    pub(super) fn dns_reload_resources(
        &self,
        snapshot: &Arc<EngineRuntimeSnapshot>,
    ) -> io::Result<zero_dns::DnsReloadResources> {
        self.pending_dns_reload
            .as_ref()
            .filter(|pending| Arc::ptr_eq(snapshot, &pending.snapshot))
            .map(|pending| pending.resources.clone())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "exact host DNS candidate is unavailable",
                )
            })
    }
}

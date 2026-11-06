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
        let dispatch = snapshot.dns_dispatch();
        let resources = proxy.resolver.prepare_reload_resources_with_host_scopes(
            config.runtime.dns.as_ref(),
            dispatch,
            affected,
            |instance, scope| {
                components
                    .component(instance.component)
                    .is_some_and(|slot| {
                        slot.instance_id() == instance && slot.matches_live_scope(scope)
                    })
            },
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
    /// The same actual DNS token survives cancellation, just like its cold
    /// backend candidate. This is before Host user quiescence, never a bypass.
    pub(super) fn quiesce_dns_donors_for_reload<'a>(
        &'a mut self,
        proxy: &'a Proxy,
        snapshot: &'a Arc<EngineRuntimeSnapshot>,
        components: &'a Arc<ComponentPlan>,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let replaced = proxy
                .protocols
                .runtime_host()
                .components()
                .replacement_instances(components)
                .map_err(io::Error::other)?;
            let resources = self.dns_reload_resources(snapshot)?;
            // Install in the real existing token before the first await.
            proxy.resolver.prepare_donor_pause(&resources, &replaced)?;
            let end = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            proxy
                .protocols
                .drive_component_supervision(proxy.resolver.retire_paused_donors(&resources, end))
                .await
        })
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

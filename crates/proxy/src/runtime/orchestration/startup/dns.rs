//! Bootstrap from the same captured candidate before Business publication.
use crate::runtime::Proxy;
use std::{collections::BTreeSet, sync::Arc};
use zero_engine::EngineError;
pub(super) async fn prepare(
    proxy: &Proxy,
    snapshot: &Arc<zero_engine::EngineRuntimeSnapshot>,
    components: &crate::component::catalog::ComponentPlan,
) -> Result<(), EngineError> {
    let dispatch = snapshot.dns_dispatch();
    let config = snapshot.config();
    // No published startup path exists yet. A previous independent Service
    // probe may have used this DNS system; equal detour strings do not prove
    // that its retained pool used the about-to-start component generations.
    let affected: BTreeSet<_> = proxy
        .config
        .runtime
        .dns
        .iter()
        .chain(config.runtime.dns.iter())
        .flat_map(|dns| dns.servers.values())
        .filter_map(|server| server.detour().map(str::to_owned))
        .collect();
    let token = proxy.resolver.prepare_reload_resources_with_host_scopes(
        config.runtime.dns.as_ref(),
        dispatch,
        &affected,
        |instance, scope| {
            components
                .component(instance.component)
                .is_some_and(|slot| {
                    slot.instance_id() == instance && slot.matches_live_scope(scope)
                })
        },
    )?;
    proxy
        .resolver
        .retire_reload_resources(
            &token,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await?;
    // Node bootstrap DNS must already use the captured configuration during
    // component start. This publishes the exact retired/confirmed candidate;
    // final component publication still uses that same captured config Arc.
    proxy.resolver.commit_reload_resources(&token)?;
    Ok(())
}

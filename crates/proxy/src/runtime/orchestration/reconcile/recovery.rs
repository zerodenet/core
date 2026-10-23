//! Recovery after actual resource retirement rebuilds closed DNS backends.
use super::*;

impl OrchestrationState {
    pub(super) async fn finish_rejection(
        &mut self,
        proxy: &Proxy,
        rejected: &std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
        message: String,
        recover_dns: bool,
    ) {
        // Only candidate instances registered by this phase are stopped.
        // Recovery uses the last-good factory without this admission fence.
        self.preparing_inbounds.begin_shutdown();
        self.preparing_inbounds = Default::default();
        self.pending_dns_reload = None;
        proxy.resolver.discard_prepared_reload();
        let persist = proxy.pending_reload_persists(rejected);
        let previous = proxy
            .pending_reload_snapshot(rejected)
            .unwrap_or_else(|| self.applied_snapshot.clone());
        let rollback = proxy.engine.restore_staged_snapshot(previous, persist);
        let mut acknowledgement = message;
        if let Err(rollback_error) = rollback {
            warn!(
                core_instance_id = proxy.core_instance_id(),
                config_revision = proxy.config_revision(),
                reason = "reload_rollback_error",
                %rollback_error,
                "failed to restore last known-good config after reload failure"
            );
            acknowledgement.push_str(&format!(
                "; last-known-good config restore failed: {rollback_error}"
            ));
        } else if let Err(error) = proxy
            .reconcile_configured_tun(
                self.applied_snapshot.config().runtime.tun.as_ref(),
                self.applied_snapshot.config().runtime.network.mtu,
            )
            .await
        {
            warn!(
                core_instance_id = proxy.core_instance_id(),
                config_revision = proxy.config_revision(),
                reason = "tun_reload_rollback_error",
                %error,
                "failed to restore last-known-good TUN after reload failure"
            );
            acknowledgement.push_str(&format!("; TUN rollback failed: {error}"));
        }
        let previous_config = self.applied_snapshot.config().clone();
        if let Err(error) = proxy
            .protocols
            .start_components(
                previous_config.clone(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
        {
            acknowledgement.push_str(&format!(
                "; component rollback remains unconfirmed: {error}"
            ));
        }

        let previous_runtime = self.inbound_runtime_factory.clone().for_recovery();
        // Only pre-existing actual owners may grant one recovery re-entry.
        // This temporary finite set holds IDs, never old owners/configuration
        // history. A failed newly created generation stays fatal/retained.
        let mut pending_faults = proxy.inbound_owners.identities();
        loop {
            let result = listeners::reconcile_inbounds(
                &proxy.protocols,
                self.source_dir.as_deref(),
                &previous_runtime,
                &previous_runtime,
                &previous_config,
                listeners::InboundReconcileState {
                    listener_stops: &mut self.listener_stops,
                    active_inbounds: &mut self.active_inbounds,
                    expected_listener_exits: &mut self.expected_listener_exits,
                    listeners: &mut self.listeners,
                },
            )
            .await;
            let Err(error) = result else {
                break;
            };
            if crate::runtime::inbound_owners::released_fault_identity(&error)
                .is_some_and(|identity| pending_faults.remove(&identity))
            {
                // Its original source was actually published; the old owner/tag
                // is now physically absent. Continue to bind last-good afresh.
                continue;
            }
            warn!(%error, reason = "listener_reload_rollback_error", "failed to restore last-known-good listeners after reload failure");
            acknowledgement.push_str(&format!("; listener rollback failed: {error}"));
            break;
        }
        if recover_dns {
            if let Err(error) = restore_dns(proxy, &previous_config).await {
                acknowledgement.push_str(&format!("; DNS recovery remains unconfirmed: {error}"));
            }
        }
        self.publish_endpoint_facts(proxy);
        self.publish_rejected_endpoints(proxy, rejected, &acknowledgement);
        proxy.complete_reload(rejected, Err(acknowledgement));
    }
}

async fn restore_dns(
    proxy: &Proxy,
    config: &std::sync::Arc<zero_config::RuntimeConfig>,
) -> Result<(), String> {
    let dispatch = config
        .compile_dns_dispatch()
        .map_err(|error| error.to_string())?;
    let token = proxy
        .resolver
        .prepare_reload_resources_with_dispatch(
            config.runtime.dns.as_ref(),
            dispatch,
            &std::collections::BTreeSet::new(),
        )
        .map_err(|error| error.to_string())?;
    proxy
        .resolver
        .retire_reload_resources(
            &token,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .map_err(|error| error.to_string())?;
    proxy
        .protocols
        .scan_components(
            config,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .map_err(|error| error.to_string())?;
    proxy.protocols.commit_components(config, || {
        proxy
            .resolver
            .commit_reload_resources(&token)
            .map_err(|error| error.to_string())
    })
}

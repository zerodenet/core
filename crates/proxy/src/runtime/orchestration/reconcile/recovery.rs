//! Recovery after actual resource retirement rebuilds closed DNS backends.
use super::*;
use zero_engine::EngineError;

impl OrchestrationState {
    // Every rejected branch borrows this one bounded heap future. Keeping the
    // complete recovery state machine inline in each branch overflows a real
    // 2 MiB worker before the branch is even selected in debug builds.
    pub(super) fn finish_rejection<'a>(
        &'a mut self,
        proxy: &'a Proxy,
        rejected: &'a std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
        message: String,
        recover_dns: bool,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, ()> {
        Box::pin(async move {
            // Only candidate instances registered by this phase are stopped.
            // Recovery uses the last-good factory without this admission fence.
            self.preparing_inbounds.begin_shutdown();
            self.preparing_inbounds = Default::default();
            // Failed/cancelled binds retain their original native owners. Drain
            // those candidates before rebuilding any last-good dependency or TUN.
            let retired = retire_rejected_bindings(&self.inbound_runtime_factory).await;
            let mut can_restore = retired.is_ok();
            #[cfg(feature = "raw-ip-runtime")]
            let discarded = proxy
                .protocols
                .discard_pending_device_candidates(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await;
            #[cfg(feature = "raw-ip-runtime")]
            if discarded.is_err() {
                can_restore = false;
            }
            self.pending_dns_reload = None;
            proxy.resolver.discard_prepared_reload();
            let previous = if !rejected.configuration_publication().is_committed() {
                proxy.engine.runtime_snapshot()
            } else {
                proxy
                    .pending_reload_snapshot(rejected)
                    .unwrap_or_else(|| self.applied_snapshot.clone())
            };
            let rollback = proxy
                .engine
                .restore_staged_snapshot(previous.clone(), false);
            let mut acknowledgement = message;
            if let Err(error) = retired {
                acknowledgement
                    .push_str(&format!("; candidate cleanup remains unconfirmed: {error}"));
            }
            #[cfg(feature = "raw-ip-runtime")]
            if let Err(error) = discarded {
                acknowledgement.push_str(&format!(
                    "; device candidate originals remain unconfirmed: {error}"
                ));
            }
            if let Err(rollback_error) = rollback {
                can_restore = false;
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
            } else if can_restore {
                if let Err(error) = proxy
                    .reconcile_configured_tun(
                        self.applied_snapshot.config().runtime.tun.as_ref(),
                        self.applied_snapshot.config().runtime.network.mtu,
                    )
                    .await
                {
                    can_restore = false;
                    warn!(
                        core_instance_id = proxy.core_instance_id(),
                        config_revision = proxy.config_revision(),
                        reason = "tun_reload_rollback_error",
                        %error,
                        "failed to restore last-known-good TUN after reload failure"
                    );
                    acknowledgement.push_str(&format!("; TUN rollback failed: {error}"));
                }
            }
            self.applied_snapshot = previous;
            let previous_config = self.applied_snapshot.config().clone();
            if can_restore {
                if let Err(error) = proxy
                    .protocols
                    .prepare_recovered_device_components(previous_config.clone())
                {
                    can_restore = false;
                    acknowledgement.push_str(&format!("; cold component recovery failed: {error}"));
                }
            }
            if can_restore {
                if let Err(error) = proxy
                    .protocols
                    .start_components(
                        previous_config.clone(),
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
                {
                    can_restore = false;
                    acknowledgement.push_str(&format!(
                        "; component rollback remains unconfirmed: {error}"
                    ));
                }
            }

            // Rebuild DNS before peer resolution, retaining its exact token until
            // the same recovered device graph has actually started and scanned.
            let mut dns_recovery = None;
            if recover_dns && can_restore {
                match restore_dns(proxy, &self.applied_snapshot).await {
                    Ok(token) => dns_recovery = Some(token),
                    Err(error) => {
                        can_restore = false;
                        acknowledgement
                            .push_str(&format!("; DNS recovery remains unconfirmed: {error}"));
                    }
                }
            }
            #[cfg(feature = "raw-ip-runtime")]
            let mut recovered_devices = None;
            #[cfg(feature = "raw-ip-runtime")]
            if can_restore {
                let services =
                    proxy.tcp_runtime_services_for_snapshot(self.applied_snapshot.clone());
                let result = async {
                    proxy
                        .protocols
                        .start_device_resources(
                            &previous_config,
                            &services,
                            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                            self.preparing_inbounds.cancellation(),
                        )
                        .await?;
                    self.bind_retained_packet_sources(proxy, &previous_config, &services)?;
                    proxy
                        .protocols
                        .prepare_devices(&previous_config, services)
                        .await
                }
                .await;
                match result {
                    Ok(devices) => recovered_devices = Some(devices),
                    Err(error) => {
                        can_restore = false;
                        acknowledgement.push_str(&format!(
                            "; outbound device rollback remains unconfirmed: {error}"
                        ));
                    }
                }
            }
            #[cfg(feature = "raw-ip-runtime")]
            if let Some(devices) = recovered_devices.take() {
                if let Err(error) = proxy
                    .protocols
                    .stage_device_publication(&previous_config, devices)
                {
                    can_restore = false;
                    acknowledgement.push_str(&format!("; device recovery staging failed: {error}"));
                }
            }
            // Dropping an unpublished candidate requests stop only. Its actual
            // controllers remain in the catalog/device owners for the next drain.
            #[cfg(feature = "raw-ip-runtime")]
            drop(recovered_devices);

            let previous_runtime = self.inbound_runtime_factory.clone().for_recovery();
            // Only pre-existing actual owners may grant one recovery re-entry.
            // This temporary finite set holds IDs, never old owners/configuration
            // history. A failed newly created generation stays fatal/retained.
            let mut pending_faults = proxy.inbound_owners.identities();
            if can_restore {
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
                    can_restore = false;
                    break;
                }
            }
            // Listener recovery must consume this same fresh PendingEndpoints
            // before confirmation clears it. The original recovery capsule's
            // linked binding may be retired and must never be revived instead.
            if can_restore {
                let result = async {
                    proxy
                        .protocols
                        .scan_components(
                            &previous_config,
                            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    proxy.protocols.commit_components(&previous_config, || {
                        dns_recovery.as_ref().map_or(Ok(()), |token| {
                            proxy
                                .resolver
                                .commit_reload_resources(token)
                                .map_err(|error| error.to_string())
                        })
                    })
                }
                .await;
                match result {
                    Err(error) => acknowledgement.push_str(&format!(
                        "; component recovery publication remains unconfirmed: {error}"
                    )),
                    Ok(()) => {
                        #[cfg(feature = "raw-ip-runtime")]
                        if let Err(error) = proxy
                            .protocols
                            .finish_device_publication(&previous_config)
                            .await
                        {
                            acknowledgement
                                .push_str(&format!("; device recovery completion failed: {error}"));
                        }
                    }
                }
            }
            self.publish_endpoint_facts(proxy);
            self.publish_rejected_endpoints(proxy, rejected, &acknowledgement);
            proxy.complete_reload(rejected, Err(acknowledgement));
        })
    }
}

async fn restore_dns(
    proxy: &Proxy,
    snapshot: &std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
) -> Result<zero_dns::DnsReloadResources, String> {
    let config = snapshot.config();
    let dispatch = snapshot.dns_dispatch();
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
    Ok(token)
}

/// Only physically closed, actually published pre-existing faults permit a
/// re-entry. An incomplete native owner (or a new failure) still blocks build.
async fn retire_rejected_bindings(
    runtime: &crate::runtime::route_runtime::InboundListenerRuntimeFactory,
) -> Result<(), EngineError> {
    let mut original_faults = runtime.owners().identities();
    loop {
        match runtime
            .owners()
            .retire_bind_reservations_reported(
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                |tag, terminal| {
                    runtime.publish_inbound_terminal(tag, terminal);
                    Ok(())
                },
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(error)
                if crate::runtime::inbound_owners::released_fault_identity(&error)
                    .is_some_and(|identity| original_faults.remove(&identity)) => {}
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
#[path = "recovery/tests.rs"]
mod tests;

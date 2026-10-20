//! Configuration replacement and rollback; no protocol-private lifecycle.
mod recovery;
use super::{logging::log_reload_reconciled, state::OrchestrationState};
use crate::groups::UrlTestRuntime;
use crate::runtime::route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices};
use crate::runtime::{listeners, Proxy};
use tracing::warn;

impl OrchestrationState {
    pub(super) async fn reconcile_reload(&mut self, proxy: &Proxy) {
        let new_snapshot = proxy.engine.runtime_snapshot();
        // Notifications may outlive the rollback that emitted them. Replaying
        // an already applied snapshot after its acknowledgement would consume
        // the DNS candidate still owned by the acknowledged apply transaction.
        if std::sync::Arc::ptr_eq(&new_snapshot, &self.applied_snapshot)
            && !proxy.pending_reload_matches(&new_snapshot)
        {
            return;
        }
        if let Err(error) = self.validate_endpoint_directions(proxy, &new_snapshot) {
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        if let Err(error) = proxy
            .protocols
            .validate_dial_environment(new_snapshot.config())
        {
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        self.publish_reload_transitions(proxy, &new_snapshot);
        let new_config = new_snapshot.config().clone();
        let components = match proxy.protocols.prepare_components(new_config.clone()) {
            Ok(plan) => plan,
            Err(error) => {
                self.reject_reload(proxy, &new_snapshot, error.to_string())
                    .await;
                return;
            }
        };
        let affected = proxy.protocols.affected_dns_detours(
            &proxy.engine,
            &self.applied_snapshot,
            &new_snapshot,
            &components,
        );
        let dns_reload = self.prepare_dns_for_reload(proxy, &new_snapshot, &components, &affected);
        if let Err(error) = dns_reload {
            warn!(%error, reason = "dns_reload_error", "failed to prepare dns config");
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        if let Err(error) = proxy
            .protocols
            .start_components(
                new_config.clone(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
        {
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        let candidate_tcp_services = proxy.tcp_runtime_services_for_snapshot(new_snapshot.clone());
        let candidate_runtime_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(candidate_tcp_services.clone()),
            proxy.inbound_owners.clone(),
        );
        let candidate_urltest_runtime = UrlTestRuntime::new(candidate_tcp_services.clone());
        #[cfg(feature = "managed-stream-runtime")]
        let prepared_services = match proxy.protocols.prepare_inbound_services(&new_config) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.reject_reload(proxy, &new_snapshot, error.to_string())
                    .await;
                return;
            }
        };
        let rollback_runtime_factory = self.inbound_runtime_factory.clone();
        let source_dir = self.source_dir.clone();
        if let Err(error) = proxy
            .reconcile_configured_tun(
                new_config.runtime.tun.as_ref(),
                new_config.runtime.network.mtu,
            )
            .await
        {
            warn!(
                core_instance_id = proxy.core_instance_id(),
                config_revision = proxy.config_revision(),
                reason = "tun_reconcile_error",
                %error,
                "config reload TUN reconciliation failed; restoring last known-good config"
            );
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        #[cfg(feature = "raw-ip-runtime")]
        let prepared_outbound_devices = match proxy
            .protocols
            .prepare_outbound_devices(&new_config, candidate_tcp_services.clone())
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                warn!(%error, reason = "outbound_device_prepare_error", "failed to prepare outbound devices");
                self.reject_reload(proxy, &new_snapshot, error.to_string())
                    .await;
                return;
            }
        };
        let revoked_directions = new_snapshot.revoked_endpoint_directions(&self.applied_snapshot);
        let inbound_result = listeners::reconcile_inbounds(
            &proxy.protocols,
            source_dir.as_deref(),
            &candidate_runtime_factory,
            &rollback_runtime_factory,
            &new_config,
            listeners::InboundReconcileState {
                listener_stops: &mut self.listener_stops,
                active_inbounds: &mut self.active_inbounds,
                expected_listener_exits: &mut self.expected_listener_exits,
                listeners: &mut self.listeners,
            },
        )
        .await;
        if let Err(error) = inbound_result {
            let message = error.to_string();
            warn!(
                core_instance_id = proxy.core_instance_id(),
                config_revision = proxy.config_revision(),
                reason = "listener_reconcile_error",
                %error,
                "config reload listener reconciliation failed; restoring last known-good config"
            );
            #[cfg(feature = "raw-ip-runtime")]
            drop(prepared_outbound_devices);
            self.reject_reload(proxy, &new_snapshot, message).await;
            return;
        }
        if !proxy.pending_reload_matches(&new_snapshot) {
            let commit = async {
                let token = self
                    .dns_reload_resources(&new_snapshot)
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
                    .commit_components(new_snapshot.config(), || {
                        proxy
                            .resolver
                            .commit_reload_resources(&token)
                            .map_err(|error| error.to_string())
                    })
            }
            .await;
            if let Err(message) = commit {
                #[cfg(feature = "raw-ip-runtime")]
                drop(prepared_outbound_devices);
                self.finish_rejection(proxy, &new_snapshot, message, true)
                    .await;
                return;
            }
        }
        #[cfg(feature = "raw-ip-runtime")]
        for prepared in prepared_outbound_devices {
            prepared.publish().await;
        }
        listeners::reconcile_urltests(
            &candidate_urltest_runtime,
            &self.shutdown_rx,
            &mut self.urltests,
        )
        .await;
        proxy.protocols.on_config_reloaded(&new_config);
        #[cfg(feature = "managed-stream-runtime")]
        self.services
            .replace(
                prepared_services,
                candidate_tcp_services,
                self.shutdown_rx.clone(),
            )
            .await;
        self.inbound_runtime_factory = candidate_runtime_factory;
        self.urltest_runtime = candidate_urltest_runtime;
        self.publish_removed_endpoints(proxy, &new_snapshot);
        self.applied_snapshot = new_snapshot;
        // Only successful DNS/resource confirmation can precede irreversible
        // cancellation of the old business sessions in revoked directions.
        for (binding, directions) in revoked_directions {
            proxy.engine.cancel_endpoint_flows(&binding, directions);
        }
        self.publish_endpoint_facts(proxy);
        log_reload_reconciled(&new_config);
        // For an accepted apply, its persistent task takes over retirement and
        // commit after this real acknowledgement. Resolver retains the token.
        self.pending_dns_reload = None;
        proxy.complete_reload(&self.applied_snapshot, Ok(()));
    }

    async fn reject_reload(
        &mut self,
        proxy: &Proxy,
        rejected: &std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
        message: String,
    ) {
        self.finish_rejection(proxy, rejected, message, false).await;
    }
}

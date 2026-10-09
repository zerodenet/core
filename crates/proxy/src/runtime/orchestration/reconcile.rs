//! Configuration replacement and rollback; no protocol-private lifecycle.
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
        let candidate_tcp_services = proxy.tcp_runtime_services_for_snapshot(new_snapshot.clone());
        let candidate_runtime_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(candidate_tcp_services.clone()),
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
        let dns_reload = new_config
            .compile_dns_dispatch()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
            .and_then(|dispatch| {
                proxy
                    .resolver
                    .prepare_reload_with_dispatch(new_config.runtime.dns.as_ref(), dispatch)
            });
        if let Err(error) = dns_reload {
            warn!(%error, reason = "dns_reload_error", "failed to reload dns config");
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
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
            if let Err(error) = proxy.resolver.commit_prepared_reload() {
                warn!(%error, reason = "dns_commit_error", "failed to commit dns config");
                #[cfg(feature = "raw-ip-runtime")]
                drop(prepared_outbound_devices);
                self.reject_reload(proxy, &new_snapshot, error.to_string())
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
        // Do not terminate old business until every fallible preparation and
        // listener reconcile has succeeded. The published snapshot denies
        // new admission; only then retire sessions in revoked directions.
        for (binding, directions) in revoked_directions {
            proxy.engine.cancel_endpoint_flows(&binding, directions);
        }
        self.publish_endpoint_facts(proxy);
        log_reload_reconciled(&new_config);
        proxy.complete_reload(&self.applied_snapshot, Ok(()));
    }

    async fn reject_reload(
        &mut self,
        proxy: &Proxy,
        rejected: &std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
        message: String,
    ) {
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
        let previous_runtime = self.inbound_runtime_factory.clone();
        if let Err(error) = listeners::reconcile_inbounds(
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
        .await
        {
            warn!(%error, reason = "listener_reload_rollback_error", "failed to restore last-known-good listeners after reload failure");
            acknowledgement.push_str(&format!("; listener rollback failed: {error}"));
        }
        self.publish_endpoint_facts(proxy);
        self.publish_rejected_endpoints(proxy, rejected, &acknowledgement);
        proxy.complete_reload(rejected, Err(acknowledgement));
    }
}

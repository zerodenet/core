use std::sync::Arc;

use zero_config::{ModeConfig, RuntimeConfig};

use super::Engine;
use crate::{EngineError, EnginePlan};

mod notifications;
mod source;

impl Engine {
    /// Prepare an embedded domain plan, commit it through this engine's source
    /// owner, and advance its revision without starting a Proxy.
    pub fn reload_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, true, true)
    }

    /// Rebuild and atomically install a runtime-only configuration overlay.
    ///
    /// This is the boundary for caller-owned ephemeral configuration: the
    /// effective runtime changes, but the operator-owned source file does not.
    /// A process restart therefore reconstructs only the persisted local
    /// deployment configuration.
    pub fn reload_runtime_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, false, true)
    }

    /// Install a persisted candidate without publishing a committed revision.
    /// This synchronous embedded entry point preserves its domain-only scope.
    /// Proxy retains its prepared candidate under the existing RuntimeHost.
    pub fn stage_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        // Embedded callers retain their existing synchronous persistence scope.
        // Proxy uses pure snapshot preparation and its own final Ready gate.
        self.reload_config_inner(new_config, true, false)
    }

    /// Install an ephemeral candidate without publishing a committed revision.
    pub fn stage_runtime_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, false, false)
    }

    /// Restore this engine's last-known-good snapshot after a staged apply
    /// fails. Rebuilding only its config would lose selections and disconnect
    /// the still-running old probe tasks from their policy state.
    pub fn restore_staged_snapshot(
        &self,
        snapshot: Arc<super::EngineRuntimeSnapshot>,
        persist: bool,
    ) -> Result<(), EngineError> {
        if snapshot.config_revision() != self.config_revision() {
            return Err(EngineError::InvalidPlan {
                message: "cannot restore a snapshot across a committed configuration change"
                    .to_owned(),
            });
        }
        // The public embedded restore reinstalls this exact historical domain
        // snapshot, including its original selector state. It is distinct from
        // publishing a hidden host candidate against the still-committed state.
        let config = snapshot.config().clone();
        self.config_source
            .commit_embedded(&config, persist, "engine-only-restore", || {
                self.install_prepared_snapshot(snapshot, false)
            })?;
        self.traffic
            .reconcile(&self.config(), self.config_revision());
        self.notify_reload();
        Ok(())
    }

    fn reload_config_inner(
        &self,
        config: RuntimeConfig,
        persist: bool,
        commit_revision: bool,
    ) -> Result<(), EngineError> {
        self.config_source
            .ensure_available()
            .map_err(std::io::Error::from)?;
        let prepared = zero_runtime::configuration::PreparedConfiguration::prepare_owned(
            config,
            self.config_source.clone(),
        )?;
        let snapshot = self.prepare_configuration_snapshot(prepared)?;
        let config = snapshot.config().clone();
        self.config_source
            .commit_embedded(&config, persist, "engine-only-apply", || {
                self.publish_prepared_snapshot(snapshot)?;
                self.notify_reload();
                if commit_revision {
                    self.commit_config_change();
                }
                Ok::<(), EngineError>(())
            })
    }

    /// Existing embedded staging entry point. Proxy transactions instead retain
    /// the returned inert snapshot in their same persistent host operation.
    pub fn stage_prepared_config(
        &self,
        prepared: zero_runtime::configuration::PreparedConfiguration,
    ) -> Result<(), EngineError> {
        let snapshot = self.prepare_configuration_snapshot(prepared)?;
        let config = snapshot.config().clone();
        self.config_source
            .commit_embedded(&config, false, "engine-only-prepared", || {
                self.publish_prepared_snapshot(snapshot)?;
                self.notify_reload();
                Ok::<(), EngineError>(())
            })
    }

    /// Build only domain values; no live configuration or policy is published.
    pub fn prepare_configuration_snapshot(
        &self,
        prepared: zero_runtime::configuration::PreparedConfiguration,
    ) -> Result<Arc<super::EngineRuntimeSnapshot>, EngineError> {
        let zero_runtime::configuration::PreparedConfiguration {
            config,
            routing,
            source: _,
        } = prepared;
        self.event_log.validate_statistics_limits(
            config.runtime.effective_event_log_stats_max_bytes(),
            config.runtime.effective_event_subscriber_stats_max_bytes(),
        )?;
        self.packet_routes
            .prepare_limit(config.runtime.effective_packet_route_max_routes())?;
        let current = self.runtime_snapshot();
        if current.config.runtime.principal_quota_state_path
            != config.runtime.principal_quota_state_path
        {
            return Err(EngineError::InvalidPlan {
                message: "runtime.principal_quota_state_path cannot change during live reload"
                    .to_owned(),
            });
        }
        let mut plan = EnginePlan::build(&config)?;
        let endpoint_intents = Arc::new(
            super::endpoint::EndpointIntents::for_config(&config, Some(&current.endpoint_intents))
                .map_err(|error| EngineError::InvalidPlan {
                    message: error.message,
                })?,
        );
        plan.reconcile_direct_dial_generations(Some(&current.plan), &self.next_dial_generation);
        plan.reconcile_outbound_static(&config, &current.plan, &current.config);
        let plan = Arc::new(plan);
        let outbound_group_state = crate::groups::OutboundGroupStateStore::for_plan(
            &plan,
            Some((&current.plan, &current.outbound_group_state)),
        );
        Ok(Arc::new(super::EngineRuntimeSnapshot {
            config_revision: Arc::new(std::sync::atomic::AtomicU64::new(current.config_revision())),
            publication: super::ConfigurationPublication::pending(),
            config: Arc::new(config),
            endpoint_intents,
            plan,
            router: routing.router,
            bypass: routing.bypass,
            dns_dispatch: routing.dns_dispatch,
            outbound_group_state,
        }))
    }

    /// Install under the accepted operation's final publication gate. Source
    /// persistence and original resource cleanup have already been confirmed.
    pub fn publish_prepared_snapshot(
        &self,
        snapshot: Arc<super::EngineRuntimeSnapshot>,
    ) -> Result<(), EngineError> {
        self.install_prepared_snapshot(snapshot, true)
    }

    fn install_prepared_snapshot(
        &self,
        snapshot: Arc<super::EngineRuntimeSnapshot>,
        reconcile_live_choices: bool,
    ) -> Result<(), EngineError> {
        let mut current = self
            .runtime_snapshot
            .write()
            .expect("runtime snapshot lock poisoned");
        if snapshot.config_revision() != current.config_revision() {
            return Err(EngineError::InvalidPlan {
                message: "cannot publish a snapshot across a committed configuration change"
                    .to_owned(),
            });
        }
        // Recheck under the actual byte books before mutating any selector,
        // mode, policy, capacity, configuration or publication state. Event
        // producers may have charged new bytes since the pure preparation.
        let packet_limit = self
            .packet_routes
            .prepare_limit(snapshot.config.runtime.effective_packet_route_max_routes())?;
        self.event_log.configure_statistics_limits(
            snapshot
                .config
                .runtime
                .effective_event_log_stats_max_bytes(),
            snapshot
                .config
                .runtime
                .effective_event_subscriber_stats_max_bytes(),
        )?;
        let waiters = if reconcile_live_choices
            && !Arc::ptr_eq(
                &snapshot.outbound_group_state,
                &current.outbound_group_state,
            ) {
            snapshot.outbound_group_state.reconcile_live_selectors(
                &snapshot.plan,
                &current.plan,
                &current.outbound_group_state,
            )
        } else {
            Vec::new()
        };
        *self.mode.lock().unwrap_or_else(|error| error.into_inner()) = snapshot.config.mode.clone();
        self.principal_policies
            .replace_from_config(snapshot.config());
        self.event_log
            .set_capacity(snapshot.config.runtime.event_log_capacity);
        let publication = snapshot.publication.clone();
        packet_limit.commit();
        *current = snapshot;
        drop(packet_limit);
        drop(current);
        publication.publish();
        for waiter in waiters {
            waiter.wake();
        }
        self.passive_relay_health.clear();
        Ok(())
    }

    /// Publish the currently staged runtime snapshot as one committed
    /// configuration generation.
    pub fn commit_config_change(&self) -> u64 {
        let revision = self
            .config_revision
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1);
        let current = self.runtime_snapshot();
        current
            .config_revision
            .store(revision, std::sync::atomic::Ordering::Release);
        self.stats.reconcile_outbounds(&self.config());
        self.traffic.reconcile(&self.config(), revision);
        self.event_log.push_config_changed(revision);
        revision
    }

    pub(crate) fn commit_mode_change(&self, mode: ModeConfig) -> u64 {
        let revision = self
            .config_revision
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1);
        let current = self.runtime_snapshot();
        let mut config = (*current.config).clone();
        config.mode = mode;
        *self
            .runtime_snapshot
            .write()
            .expect("runtime snapshot lock poisoned") = Arc::new(super::EngineRuntimeSnapshot {
            config_revision: Arc::new(std::sync::atomic::AtomicU64::new(revision)),
            publication: current.publication.clone(),
            config: Arc::new(config),
            endpoint_intents: current.endpoint_intents.clone(),
            plan: current.plan.clone(),
            dns_dispatch: current.dns_dispatch.clone(),
            router: current.router.clone(),
            bypass: current.bypass.clone(),
            outbound_group_state: current.outbound_group_state.clone(),
        });
        self.stats.reconcile_outbounds(&self.config());
        self.traffic.reconcile(&self.config(), revision);
        self.event_log.push_config_changed(revision);
        revision
    }
}

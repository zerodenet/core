//! Configuration replacement and rollback; no protocol-private lifecycle.
pub(super) mod budget_barrier;
mod recovery;
use super::{logging::log_reload_reconciled, state::OrchestrationState};
use crate::groups::UrlTestRuntime;
use crate::runtime::route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices};
use crate::runtime::{listeners, Proxy};
use tracing::warn;

impl OrchestrationState {
    pub(super) async fn reconcile_reload(&mut self, proxy: &Proxy) {
        let pending = proxy.pending_configuration_snapshot();
        // Ready has delivered this candidate's ACK, but the same accepted
        // operation still owns application readiness and final publication.
        // A late/coalesced ordinary wake-up cannot replace those resources
        // with the old committed domain snapshot. Only an explicit pending
        // operation (including compensation) may reconcile in this interval.
        if pending.is_none()
            && !self
                .applied_snapshot
                .configuration_publication()
                .is_committed()
        {
            return;
        }
        let new_snapshot = pending.unwrap_or_else(|| proxy.engine.runtime_snapshot());
        // Notifications may outlive the rollback that emitted them. Replaying
        // an already applied snapshot after its acknowledgement would consume
        // the DNS candidate still owned by the acknowledged apply transaction.
        if std::sync::Arc::ptr_eq(&new_snapshot, &self.applied_snapshot)
            && !proxy.pending_reload_matches(&new_snapshot)
        {
            return;
        }
        let compensating = proxy.pending_reload_compensates(&new_snapshot);
        let budget_changed = !compensating
            && budget_barrier::ss_budget_changed(
                self.applied_snapshot.config(),
                new_snapshot.config(),
            );
        #[cfg(feature = "raw-ip-runtime")]
        let tun_restart = {
            let old = self.applied_snapshot.config();
            let next = new_snapshot.config();
            let requested = proxy.has_tun_packet_budget_recovery()
                || budget_barrier::packet_budget_changed(old, next)
                || budget_barrier::carrier_budget_changed(old, next)
                || old.runtime.system_tun_task_limit != next.runtime.system_tun_task_limit
                || old.runtime.tun != next.runtime.tun
                || old.runtime.network.mtu != next.runtime.network.mtu;
            if requested {
                match proxy.validate_tun_packet_budget(next) {
                    Ok(present) => present || next.runtime.tun.is_some(),
                    Err(error) => {
                        self.reject_before_start(proxy, &new_snapshot, error.to_string(), true).await;
                        return;
                    }
                }
            } else { false }
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let tun_restart = false;
        // Only an accepted transaction's exact restore acknowledgement may
        // reconstruct its last-good listeners after shutdown. It is not a new
        // candidate and must not inherit the stopped candidate phase. Ordinary
        // reloads still fail even if a previous compensation reset that phase.
        let preparing = if compensating {
            std::sync::Arc::new(crate::runtime::inbound_owners::PreparingInbounds::default())
        } else {
            if proxy.host.apply().is_closing() {
                self.reject_before_start(
                    proxy,
                    &new_snapshot,
                    "proxy stopped before configuration commit".to_owned(),
                    budget_changed,
                )
                .await;
                return;
            }
            self.preparing_inbounds.clone()
        };
        if let Err(error) = preparing.ensure_open() {
            self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                .await;
            return;
        }
        if let Err(error) = self.validate_endpoint_directions(proxy, &new_snapshot) {
            self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                .await;
            return;
        }
        if let Err(error) = proxy
            .protocols
            .validate_dial_environment(new_snapshot.config())
        {
            self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                .await;
            return;
        }
        #[cfg(feature = "raw-ip-runtime")]
        if compensating {
            if let Err(error) = proxy
                .protocols
                .discard_pending_device_candidates(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
            {
                self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                    .await;
                return;
            }
        }
        // Recheck actual owner debt before stopping any old component. Recovery
        // uses its already accepted immutable recipe, not a new grammar choice.
        if !compensating {
            if let Err(error) = proxy
                .protocols
                .validate_runtime_candidate(new_snapshot.config())
            {
                self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                    .await;
                return;
            }
        }
        self.publish_reload_transitions(proxy, &new_snapshot);
        let new_config = new_snapshot.config().clone();
        #[cfg(feature = "raw-ip-runtime")]
        let restarted = tun_restart.then(|| proxy.protocols
            .prepare_restarting_device_components(new_config.clone(), compensating));
        #[cfg(not(feature = "raw-ip-runtime"))]
        let restarted: Option<Result<std::sync::Arc<crate::component::catalog::ComponentPlan>, crate::component::instance::SharedError>> = None;
        let prepared = if let Some(restarted) = restarted {
            restarted
        } else if compensating {
            proxy
                .protocols
                .prepare_recovered_device_components(new_config.clone())
        } else {
            proxy
                .protocols
                .prepare_device_components(new_config.clone())
        };
        let components = match prepared {
            Ok(plan) => plan,
            Err(error) => {
                self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                    .await;
                return;
            }
        };
        let mut source_dependency_changes = match proxy
            .inbound_owners
            .changed_source_dependency_tags(&new_config, &components)
        {
            Ok(changed) => changed,
            Err(error) => {
                self.reject_before_start(proxy, &new_snapshot, error.to_string(), true)
                    .await;
                return;
            }
        };
        if tun_restart {
            source_dependency_changes.extend(self.applied_snapshot.config().inbounds.iter()
                .map(|input| input.tag.clone()));
        }
        let source_barrier = budget_changed || tun_restart || !source_dependency_changes.is_empty();
        let affected = proxy.protocols.affected_dns_detours(
            &proxy.engine,
            &self.applied_snapshot,
            &new_snapshot,
            &components,
        );
        let dns_reload = self.prepare_dns_for_reload(proxy, &new_snapshot, &components, &affected);
        if let Err(error) = dns_reload {
            warn!(%error, reason = "dns_reload_error", "failed to prepare dns config");
            self.reject_before_start(proxy, &new_snapshot, error.to_string(), budget_changed)
                .await;
            return;
        }
        // A same-endpoint CA/transport/dependency replacement owns the same
        // warm DNS loans as a budget edit. The exact candidate determines the
        // replaced instance intersection; an empty intersection changes nothing.
        if let Err(error) = self
            .quiesce_dns_donors_for_reload(proxy, &new_snapshot, &components)
            .await
        {
            // No Source/Host StopOld has run. Preserve the actual old query,
            // restore only cold admission and discard this exact token/plan.
            self.reject_before_start(proxy, &new_snapshot, error.to_string(), true)
                .await;
            return;
        }
        // Reject actual Business before revoking any cached borrower. This same
        // guard also supplies the sealed actual receiver set for Source preflight.
        let user_pause = if source_barrier {
            match proxy.protocols.try_pause_replacement_users(&components) {
                Ok(pause) => Some(std::sync::Arc::new(pause)),
                Err(error) => {
                    #[cfg(test)]
                    eprintln!("actual replacement user preflight before Source pause: {error:?}");
                    self.reject_before_start(proxy, &new_snapshot, error.to_string(), true)
                        .await;
                    return;
                }
            }
        } else {
            None
        };
        let mut source_retirement = None;
        let _source_pauses = if let Some(user_pause) = &user_pause {
            match budget_barrier::pause_sources(
                proxy,
                self.applied_snapshot.config(),
                new_snapshot.config(),
                &source_dependency_changes,
                &components,
                user_pause.clone(),
                &mut source_retirement,
                tun_restart,
            )
            .await
            {
                Ok(pauses) => Some(pauses),
                Err(error) => {
                    let cold_only = source_retirement.is_none();
                    #[cfg(test)]
                    eprintln!("actual Source idle/receiver retirement barrier, cold_only={cold_only}: {error:?}");
                    drop(source_retirement.take());
                    self.reject_before_start(proxy, &new_snapshot, error.to_string(), cold_only)
                        .await;
                    return;
                }
            }
        } else {
            None
        };
        let candidate_tcp_services = proxy.tcp_runtime_services_for_snapshot(new_snapshot.clone());
        let retained_for_binding = self.active_inbounds.clone();
        let mut prepared_resources = None;
        if !source_barrier {
            match budget_barrier::start_resources(
                proxy,
                &new_config,
                &candidate_tcp_services,
                &preparing,
                &retained_for_binding,
            )
            .await
            {
                Ok(()) => {}
                Err(error) => {
                    self.reject_reload(proxy, &new_snapshot, error.to_string())
                        .await;
                    return;
                }
            }
        }
        let candidate_runtime_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(candidate_tcp_services.clone()),
            proxy.inbound_owners.clone(),
        );
        let candidate_runtime_factory = if tun_restart {
            candidate_runtime_factory.for_source_network_restart()
        } else { candidate_runtime_factory };
        let candidate_runtime_factory = if compensating {
            candidate_runtime_factory.for_recovery()
        } else {
            candidate_runtime_factory.with_preparing(preparing.clone())
        };
        #[cfg(feature = "managed-stream-runtime")]
        let prepared_services = match proxy.protocols.prepare_inbound_services(&new_config) {
            Ok(prepared) => prepared,
            Err(error) => {
                let cold_only = source_retirement.is_none();
                drop(source_retirement.take());
                self.reject_before_start(proxy, &new_snapshot, error.to_string(), cold_only)
                    .await;
                return;
            }
        };
        let rollback_runtime_factory = self.inbound_runtime_factory.clone();
        let source_dir = self.source_dir.clone();
        if !source_barrier {
            match budget_barrier::prepare_devices(proxy, &new_config, &candidate_tcp_services).await
            {
                Ok(devices) => prepared_resources = Some(devices),
                Err(error) => {
                    self.reject_reload(proxy, &new_snapshot, error.to_string())
                        .await;
                    return;
                }
            }
        }
        let revoked_directions = new_snapshot.revoked_endpoint_directions(&self.applied_snapshot);
        let start_config = &new_config;
        let start_services = &candidate_tcp_services;
        let start_preparing = &preparing;
        let start_retained = &retained_for_binding;
        let ss_source_budget_changed =
            budget_barrier::ss_source_budget_changed(self.applied_snapshot.config(), &new_config);
        let mut charged_source_changes = std::collections::BTreeSet::new();
        #[cfg(feature = "raw-ip-runtime")]
        if budget_barrier::packet_budget_changed(self.applied_snapshot.config(), &new_config) {
            for input in &self.applied_snapshot.config().inbounds {
                if proxy
                    .inbound_owners
                    .owner(&input.tag)
                    .is_some_and(|owner| owner.has_packet_source_custody())
                {
                    // A live Source owns its native-read/workspace promises
                    // even while it admits no business. Idle pause is not their
                    // release: require this exact old Source's StopOld receipt.
                    charged_source_changes.insert(input.tag.clone());
                }
            }
        }
        if budget_barrier::carrier_budget_changed(self.applied_snapshot.config(), &new_config) {
            for input in &new_config.inbounds {
                match proxy.protocols.prepare_inbound_bind_in_config(
                    candidate_runtime_factory.listener_config(input),
                    &new_config,
                ) {
                    Ok(Some(prepared))
                        if budget_barrier::source_total_resources(prepared.resources()) =>
                    {
                        charged_source_changes.insert(input.tag.clone());
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let cold_only = source_retirement.is_none();
                        drop(source_retirement.take());
                        self.reject_before_start(
                            proxy,
                            &new_snapshot,
                            error.to_string(),
                            cold_only,
                        )
                        .await;
                        return;
                    }
                }
            }
        }
        let output_resources = &mut prepared_resources;
        let inbound_result = listeners::reconcile_inbounds_with_before_bind(
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
            move |stopped| async move {
                if source_barrier {
                    if !stopped.matches_candidate(start_config) {
                        return Err(zero_engine::EngineError::Io(std::io::Error::other(
                            "original budget StopOld names another actual candidate",
                        )));
                    }
                    for tag in &source_dependency_changes {
                        if stopped.terminal(tag).is_none() && !stopped.owner_was_absent(tag) {
                            return Err(zero_engine::EngineError::Io(std::io::Error::other(
                                "actual dependent Source has no original StopOld receipt",
                            )));
                        }
                    }
                    for input in start_retained {
                        if (ss_source_budget_changed
                            && input.1.protocol.protocol_name() == "shadowsocks"
                            || charged_source_changes.contains(input.0))
                            && stopped.terminal(input.0).is_none()
                            && !stopped.owner_was_absent(input.0)
                        {
                            return Err(zero_engine::EngineError::Io(std::io::Error::other(
                                "original budget source lacks its original StopOld receipt",
                            )));
                        }
                    }
                    drop(source_retirement);
                    #[cfg(feature = "raw-ip-runtime")]
                    let refreshed = if tun_restart {
                        Some(budget_barrier::restart_tun_and_resources(
                            proxy, start_config, start_services, start_preparing, start_retained,
                        ).await?)
                    } else { None };
                    #[cfg(feature = "raw-ip-runtime")]
                    let start_services = refreshed.as_ref().unwrap_or(start_services);
                    if !tun_restart {
                        budget_barrier::start_resources(
                        proxy,
                        start_config,
                        start_services,
                        start_preparing,
                        start_retained,
                    )
                        .await?;
                    }
                    *output_resources = Some(
                        budget_barrier::prepare_devices(proxy, start_config, start_services)
                            .await?,
                    );
                }
                Ok(())
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
            drop(prepared_resources.take());
            self.reject_reload(proxy, &new_snapshot, message).await;
            return;
        }
        let candidate_tcp_services = if tun_restart {
            match candidate_tcp_services.with_prepared_components() {
                Ok(services) => services,
                Err(error) => {
                    self.reject_reload(proxy, &new_snapshot, error.to_string()).await;
                    return;
                }
            }
        } else { candidate_tcp_services };
        let candidate_runtime_factory = if tun_restart {
            match candidate_runtime_factory.with_prepared_components() {
                Ok(factory) => factory,
                Err(error) => {
                    self.reject_reload(proxy, &new_snapshot, error.to_string()).await;
                    return;
                }
            }
        } else { candidate_runtime_factory };
        let candidate_urltest_runtime = UrlTestRuntime::new(candidate_tcp_services.clone());
        if tun_restart {
            let actual = match &candidate_tcp_services.components {
                Ok(actual) => actual.clone(),
                Err(error) => {
                    self.reject_reload(proxy, &new_snapshot, error.to_string()).await;
                    return;
                }
            };
            if let Err(error) = self.refresh_dns_component_candidate(&new_snapshot, &components, actual) {
                self.reject_reload(proxy, &new_snapshot, error.to_string()).await;
                return;
            }
        }
        if let Err(error) = preparing.ensure_open() {
            #[cfg(feature = "raw-ip-runtime")]
            drop(prepared_resources.take());
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Err(error) = proxy.protocols.stage_device_publication(
            &new_config,
            prepared_resources
                .take()
                .expect("successful actual resource start"),
        ) {
            self.reject_reload(proxy, &new_snapshot, error.to_string())
                .await;
            return;
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = prepared_resources.take();
        let deferred_commit = proxy.pending_reload_matches(&new_snapshot);
        if !deferred_commit {
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
                preparing.ensure_open().map_err(|error| error.to_string())?;
                proxy
                    .protocols
                    .scan_components(
                        new_snapshot.config(),
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
                self.finish_rejection(proxy, &new_snapshot, message, true)
                    .await;
                return;
            }
        }
        #[cfg(feature = "raw-ip-runtime")]
        if !deferred_commit {
            if let Err(error) = proxy.protocols.finish_device_publication(&new_config).await {
                self.finish_rejection(proxy, &new_snapshot, error.to_string(), true)
                    .await;
                return;
            }
            if let Err(error) = proxy.confirm_tun_packet_budget(&new_config) {
                self.finish_rejection(proxy, &new_snapshot, error.to_string(), true).await;
                return;
            }
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
        // Resource/service publication above can suspend. Recheck the same
        // phase immediately before its success ACK; stop cannot become success
        // merely because all listeners had reached Ready earlier.
        if let Err(error) = preparing.ensure_open() {
            self.finish_rejection(proxy, &new_snapshot, error.to_string(), true)
                .await;
            return;
        }
        self.inbound_runtime_factory = candidate_runtime_factory.without_preparing();
        if !compensating {
            self.preparing_inbounds = Default::default();
        }
        self.urltest_runtime = candidate_urltest_runtime;
        self.publish_removed_endpoints(proxy, &new_snapshot);
        self.applied_snapshot = new_snapshot;
        // Only successful DNS/resource confirmation can precede irreversible
        // cancellation of the old business sessions in revoked directions.
        if !proxy.pending_reload_matches(&self.applied_snapshot) {
            for (binding, directions) in revoked_directions {
                proxy.engine.cancel_endpoint_flows(&binding, directions);
            }
        }
        // Accepted applies still hold the unpublished same-device guard at
        // Ready. Only their existing post-commit observer may publish the new
        // incarnation census; observing here would mix new listeners with old
        // devices and invent a second running-generation transition.
        if !deferred_commit {
            self.publish_endpoint_facts(proxy);
        }
        log_reload_reconciled(&new_config);
        // For an accepted apply, its persistent task takes over retirement and
        // commit after this real acknowledgement. Resolver retains the token.
        self.pending_dns_reload = None;
        proxy.complete_reload(&self.applied_snapshot, Ok(()));
    }

    fn reject_reload<'a>(
        &'a mut self,
        proxy: &'a Proxy,
        rejected: &'a std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
        message: String,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, ()> {
        self.finish_rejection(proxy, rejected, message, false)
    }
}

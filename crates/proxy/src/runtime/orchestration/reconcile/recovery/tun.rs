//! The owning TUN recovery follows the same real Source/Root transaction.
use super::*;
use std::{collections::BTreeSet, sync::Arc};

pub(super) struct TunRecoveryResources {
    pub(super) dns: zero_dns::DnsReloadResources,
    _sources: budget_barrier::SourceBudgetPauses,
    _first_cold: Arc<crate::component::catalog::ComponentPlan>,
}

impl OrchestrationState {
    pub(super) fn restore_tun_resources<'a>(
        &'a mut self,
        proxy: &'a Proxy,
        snapshot: &'a Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, Result<TunRecoveryResources, String>> {
        Box::pin(async move {
            let config = snapshot.config().clone();
            proxy
                .validate_tun_packet_budget(&config)
                .map_err(|error| error.to_string())?;
            let components = proxy
                .protocols
                .prepare_restarting_device_components(config.clone(), true)
                .map_err(|error| error.to_string())?;
            let dns = restore_dns(proxy, snapshot, Some(&components)).await?;
            let changed: BTreeSet<_> = config
                .inbounds
                .iter()
                .map(|input| input.tag.clone())
                .collect();
            let mut retirement = None;
            let sources = budget_barrier::pause_sources(
                proxy,
                &config,
                &config,
                &changed,
                &components,
                &mut retirement,
                true,
            )
            .await
            .map_err(|error| error.to_string())?;
            let services = proxy.tcp_runtime_services_for_snapshot(snapshot.clone());
            let runtime = InboundListenerRuntimeFactory::new(
                SharedIngressRuntimeServices::new(services.clone()),
                proxy.inbound_owners.clone(),
            )
            .for_recovery()
            .for_source_network_restart();
            let rollback = self.inbound_runtime_factory.clone().for_recovery();
            let retained = self.active_inbounds.clone();
            let preparing = self.preparing_inbounds.clone();
            let mut devices = None;
            let mut pending_faults = proxy.inbound_owners.identities();
            loop {
                let retirement = &mut retirement;
                let output = &mut devices;
                let config = &config;
                let changed = &changed;
                let services = &services;
                let preparing = &preparing;
                let retained = &retained;
                let result = listeners::reconcile_inbounds_with_before_bind(
                    &proxy.protocols,
                    self.source_dir.as_deref(),
                    &runtime,
                    &rollback,
                    config,
                    changed,
                    listeners::InboundReconcileState {
                        listener_stops: &mut self.listener_stops,
                        active_inbounds: &mut self.active_inbounds,
                        expected_listener_exits: &mut self.expected_listener_exits,
                        listeners: &mut self.listeners,
                    },
                    move |stopped| async move {
                        if !stopped.matches_candidate(config)
                            || changed.iter().any(|tag| {
                                stopped.terminal(tag).is_none() && !stopped.owner_was_absent(tag)
                            })
                        {
                            return Err(std::io::Error::other(
                                "System TUN recovery lacks actual Source StopOld receipts",
                            )
                            .into());
                        }
                        drop(retirement.take());
                        let services = budget_barrier::restart_tun_and_resources(
                            proxy, config, services, preparing, retained,
                        )
                        .await?;
                        *output =
                            Some(budget_barrier::prepare_devices(proxy, config, &services).await?);
                        Ok(())
                    },
                )
                .await;
                match result {
                    Ok(()) => break,
                    Err(error)
                        if crate::runtime::inbound_owners::released_fault_identity(&error)
                            .is_some_and(|identity| pending_faults.remove(&identity)) =>
                    {
                        continue
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            proxy
                .protocols
                .stage_device_publication(
                    &config,
                    devices.ok_or_else(|| {
                        "System TUN recovery lost its actual device guard".to_owned()
                    })?,
                )
                .map_err(|error| error.to_string())?;
            self.inbound_runtime_factory = runtime
                .with_prepared_components()
                .map_err(|error| error.to_string())?
                .without_preparing();
            self.urltest_runtime = UrlTestRuntime::new(
                services
                    .with_prepared_components()
                    .map_err(|error| error.to_string())?,
            );
            self.device_generation = proxy.egress_interface.generation();
            Ok(TunRecoveryResources {
                dns,
                _sources: sources,
                _first_cold: components,
            })
        })
    }
}

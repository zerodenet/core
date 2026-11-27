//! Component, DNS backend, and dependent-Source preparation on network change.
use std::time::Duration;

use tokio::time::Instant;
use zero_engine::EngineError;

use super::super::state::OrchestrationState;
use crate::runtime::{
    listeners,
    route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices},
    Proxy,
};

struct PreparedDnsGuard<'a> {
    dns: &'a zero_dns::DnsSystem,
    armed: bool,
}

impl<'a> PreparedDnsGuard<'a> {
    fn new(dns: &'a zero_dns::DnsSystem) -> Self {
        Self { dns, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PreparedDnsGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.dns.discard_prepared_reload();
        }
    }
}

impl OrchestrationState {
    pub(super) async fn prepare_network_devices(
        &mut self,
        proxy: &Proxy,
        snapshot: &std::sync::Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> Result<crate::adapters::devices::PreparedDevices, EngineError> {
        let config = snapshot.resource_config();
        let deadline = Instant::now() + Duration::from_secs(5);
        proxy
            .protocols
            .discard_device_components(config, deadline)
            .await?;
        let components = proxy
            .protocols
            .prepare_device_components(config.clone())
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        let changed_sources = proxy
            .inbound_owners
            .changed_source_dependency_tags(config, &components)?;

        if self.pending_dns_reload.is_none() {
            proxy.resolver.discard_prepared_reload();
        }
        // Network recovery preserves the configuration document: reuse pools
        // whose canonical host Scope remains live, and rebuild only pools
        // whose bound Scope is replaced in `components` or already closing.
        let dns_token = proxy.resolver.prepare_reload_resources_with_host_scopes(
            config.runtime.dns.as_ref(),
            snapshot.dns_dispatch(),
            &std::collections::BTreeSet::new(),
            |instance, scope| {
                components
                    .component(instance.component)
                    .is_some_and(|slot| {
                        slot.instance_id() == instance && slot.matches_live_scope(scope)
                    })
            },
        )?;
        let mut dns_guard = PreparedDnsGuard::new(&proxy.resolver);
        proxy.resolver.prepare_fake_ip_journal_limit(
            &dns_token,
            config.runtime.effective_file_read_max_bytes(),
        )?;
        let replaced = proxy
            .protocols
            .runtime_host()
            .components()
            .replacement_instances(&components)
            .map_err(std::io::Error::other)?;
        proxy.resolver.prepare_donor_pause(&dns_token, &replaced)?;
        proxy
            .protocols
            .drive_component_supervision(proxy.resolver.retire_paused_donors(&dns_token, deadline))
            .await?;

        if changed_sources.is_empty() {
            let start_deadline = Instant::now() + Duration::from_secs(5);
            proxy
                .protocols
                .start_components(config.clone(), start_deadline)
                .await?;
            proxy
                .resolver
                .retire_reload_resources(&dns_token, Instant::now() + Duration::from_secs(5))
                .await?;
            proxy.resolver.commit_reload_resources(&dns_token)?;
            dns_guard.disarm();

            // Network replacement keeps the configuration Arc, so ordinary
            // execution still selects the published plan until commit.
            let services = proxy
                .tcp_runtime_services_for_snapshot(snapshot.clone())
                .with_prepared_components()?;
            proxy
                .protocols
                .start_device_resources(
                    config,
                    &services,
                    Instant::now() + Duration::from_secs(5),
                    self.preparing_inbounds.cancellation(),
                )
                .await?;
            Self::bind_retained_packet_sources(proxy, config, &services, &self.active_inbounds)?;
            return proxy
                .protocols
                .prepare_devices(config, services, self.preparing_inbounds.cancellation())
                .await;
        }

        let mut source_retirement = None;
        let _source_pauses = super::super::reconcile::budget_barrier::pause_sources(
            proxy,
            config,
            config,
            &changed_sources,
            &components,
            &mut source_retirement,
            false,
        )
        .await?;
        let candidate_services = proxy.tcp_runtime_services_for_snapshot(snapshot.clone());
        let candidate_factory = InboundListenerRuntimeFactory::new(
            SharedIngressRuntimeServices::new(candidate_services.clone()),
            proxy.inbound_owners.clone(),
        )
        .with_preparing(self.preparing_inbounds.clone());
        let rollback_factory = self.inbound_runtime_factory.clone();
        let source_dir = self.source_dir.clone();
        let retained_for_binding = self.active_inbounds.clone();
        let preparing = self.preparing_inbounds.clone();
        let mut prepared_devices = None;
        let output_devices = &mut prepared_devices;
        let changed_ref = &changed_sources;
        let dns_token_ref = &dns_token;
        let dns_guard_ref = &mut dns_guard;
        listeners::reconcile_inbounds_with_before_bind(
            &proxy.protocols,
            source_dir.as_deref(),
            &candidate_factory,
            &rollback_factory,
            config,
            changed_ref,
            listeners::InboundReconcileState {
                listener_stops: &mut self.listener_stops,
                active_inbounds: &mut self.active_inbounds,
                expected_listener_exits: &mut self.expected_listener_exits,
                listeners: &mut self.listeners,
            },
            move |stopped| async move {
                for tag in changed_ref {
                    if stopped.terminal(tag).is_none() && !stopped.owner_was_absent(tag) {
                        return Err(EngineError::Io(std::io::Error::other(
                            "network dependent Source lacks its original StopOld receipt",
                        )));
                    }
                }
                drop(source_retirement);
                let step_deadline = Instant::now() + Duration::from_secs(5);
                proxy
                    .protocols
                    .start_components(config.clone(), step_deadline)
                    .await?;
                proxy
                    .resolver
                    .retire_reload_resources(dns_token_ref, Instant::now() + Duration::from_secs(5))
                    .await?;
                proxy.resolver.commit_reload_resources(dns_token_ref)?;
                dns_guard_ref.disarm();
                let services = candidate_services.with_prepared_components()?;
                proxy
                    .protocols
                    .start_device_resources(
                        config,
                        &services,
                        Instant::now() + Duration::from_secs(5),
                        preparing.cancellation(),
                    )
                    .await?;
                Self::bind_retained_packet_sources(
                    proxy,
                    config,
                    &services,
                    &retained_for_binding,
                )?;
                *output_devices = Some(
                    proxy
                        .protocols
                        .prepare_devices(config, services, preparing.cancellation())
                        .await?,
                );
                Ok(())
            },
        )
        .await?;
        prepared_devices.ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "network reconciliation did not produce prepared outbound devices",
            ))
        })
    }
}

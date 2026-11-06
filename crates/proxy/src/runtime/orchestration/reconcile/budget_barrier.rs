//! SS security changes fence original users before the existing StopOld phase.
use super::*;
use std::{collections::HashMap, sync::Arc};
use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;
use zero_platform_tokio::contract::BoxFuture;

pub(in crate::runtime) fn ss_budget_changed(old: &RuntimeConfig, next: &RuntimeConfig) -> bool {
    if old.runtime.effective_process_max_jobs() != next.runtime.effective_process_max_jobs()
        || old.runtime.effective_process_max_jobs_per_instance() != next.runtime.effective_process_max_jobs_per_instance() {
        return true;
    }
    let configured = old
        .inbounds
        .iter()
        .chain(&next.inbounds)
        .any(|input| input.protocol.protocol_name() == "shadowsocks")
        || old
            .outbounds
            .iter()
            .chain(&next.outbounds)
            .any(|input| input.protocol.protocol_name() == "shadowsocks");
    configured
        && match (
            zero_runtime::configuration::SsSecurityBudgetLimits::from_runtime(&old.runtime),
            zero_runtime::configuration::SsSecurityBudgetLimits::from_runtime(&next.runtime),
        ) {
            (Ok(old), Ok(next)) => old != next,
            _ => true,
        }
}

pub(super) async fn pause_sources(
    proxy: &Proxy,
    previous: &RuntimeConfig,
) -> Result<Vec<crate::runtime::inbound_owners::AcquisitionPause>, EngineError> {
    let mut pauses = Vec::new();
    for input in &previous.inbounds {
        if input.protocol.protocol_name() != "shadowsocks" {
            continue;
        }
        if let Some(original) = proxy.inbound_owners.owner(&input.tag) {
            if let Some(pause) = original
                .pause_acquisition_if_idle(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await?
            {
                pauses.push(pause);
            }
        }
    }
    Ok(pauses)
}

#[cfg(feature = "raw-ip-runtime")]
pub(super) type ReloadDevices = crate::adapters::devices::PreparedDevices;
#[cfg(not(feature = "raw-ip-runtime"))]
pub(super) type ReloadDevices = ();

/// The existing exact plans/controllers remain retained by their real hosts.
/// This future adds no task or host and returns the same device publication value.
pub(super) fn start_resources<'a>(
    proxy: &'a Proxy,
    config: &'a Arc<RuntimeConfig>,
    services: &'a crate::protocol_registry::TcpRuntimeServices,
    preparing: &'a Arc<crate::runtime::inbound_owners::PreparingInbounds>,
    retained: &'a HashMap<String, InboundConfig>,
) -> BoxFuture<'a, Result<(), EngineError>> {
    Box::pin(async move {
        proxy
            .protocols
            .start_components(
                config.clone(),
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
            .inspect_err(|source|{tracing::warn!(?source,"same catalog StopOld/ordinary start retained original failure");#[cfg(test)]eprintln!("actual catalog StopOld original failure: {source:?}");})?;
        #[cfg(feature = "raw-ip-runtime")]
        {
            proxy
                .protocols
                .start_device_resources(
                    config,
                    services,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                    preparing.cancellation(),
                )
                .await
                .inspect_err(|source|{tracing::warn!(?source,"same device resolve/deferred start retained original failure");#[cfg(test)]eprintln!("actual deferred device original failure: {source:?}");})?;
            let execution = services.execution();
            let plan = execution.components.as_deref().map_err(|error| {
                std::io::Error::other(crate::component::instance::retained(error.clone()))
            })?;
            for input in &config.inbounds {
                if retained.get(&input.tag) != Some(input) {
                    continue;
                }
                if let Some(original) = proxy.inbound_owners.packet_source_origin(input)? {
                    proxy
                        .protocols
                        .bind_actual_packet_source(config, plan, &input.tag, original)?;
                }
            }
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = (services, preparing, retained);
        Ok(())
    })
}

pub(super) fn prepare_devices<'a>(
    proxy: &'a Proxy,
    config: &'a Arc<RuntimeConfig>,
    services: &'a crate::protocol_registry::TcpRuntimeServices,
) -> BoxFuture<'a, Result<ReloadDevices, EngineError>> {
    Box::pin(async move {
        proxy
            .reconcile_configured_tun(config.runtime.tun.as_ref(), config.runtime.network.mtu)
            .await?;
        #[cfg(feature = "raw-ip-runtime")]
        {
            proxy
                .protocols
                .prepare_devices(config, services.clone())
                .await
                .inspect_err(|source| {
                    #[cfg(test)]
                    eprintln!("actual post-start device installation retained original failure: {source:#?}");
                    tracing::warn!(?source, "same post-start device original installation failed");
                })
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            let _ = services;
            Ok(())
        }
    })
}

impl OrchestrationState {
    /// The exact candidate is still cold: current slots/Sources were neither
    /// revoked nor replaced. Do not run last-good recovery through live users.
    pub(super) fn reject_before_start<'a>(
        &'a mut self,
        proxy: &'a Proxy,
        rejected: &'a Arc<zero_engine::EngineRuntimeSnapshot>,
        message: String,
        cold_only: bool,
    ) -> BoxFuture<'a, ()> {
        if !cold_only {
            return self.reject_reload(proxy, rejected, message);
        }
        Box::pin(async move {
            if self.dns_reload_resources(rejected).is_ok() {
                self.pending_dns_reload = None;
                proxy.resolver.discard_prepared_reload();
            }
            let discarded = proxy
                .protocols
                .discard_cold_components(
                    rejected.config(),
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .await;
            let mut message = message;
            if let Err(error) = discarded {
                message.push_str(&format!(
                    "; exact cold candidate cleanup remains unconfirmed: {error}"
                ));
            }
            // Acknowledged apply stages a hidden snapshot, not an Engine domain
            // mutation. Only restore if an uncommitted same-revision snapshot
            // was actually installed by the original caller.
            let current = proxy.engine.runtime_snapshot();
            if !Arc::ptr_eq(&current, &self.applied_snapshot)
                && current.config_revision() == self.applied_snapshot.config_revision()
            {
                if let Err(error) = proxy
                    .engine
                    .restore_staged_snapshot(self.applied_snapshot.clone(), false)
                {
                    message.push_str(&format!("; original domain restore failed: {error}"));
                }
            }
            self.publish_rejected_endpoints(proxy, rejected, &message);
            proxy.complete_reload(rejected, Err(message));
        })
    }
}

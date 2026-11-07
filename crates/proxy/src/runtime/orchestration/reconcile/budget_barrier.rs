//! SS security changes fence original users before the existing StopOld phase.
use super::*;
use std::{collections::HashMap, sync::Arc};
use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;
use zero_platform_tokio::contract::BoxFuture;

pub(in crate::runtime) fn carrier_budget_changed(
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    old.runtime.effective_carrier_max_total_originals()
        != next.runtime.effective_carrier_max_total_originals()
}
/// Exactly the Source native classes now bound to the same Host total root.
/// A packet device or role alias is not silently charged as a listener.
pub(in crate::runtime) fn source_total_resources(
    resources: &[zero_platform_tokio::contract::component::ResourceDeclaration],
) -> bool {
    use zero_platform_tokio::contract::component::ResourceDeclaration as R;
    resources.iter().any(|resource| {
        matches!(
            resource,
            R::TcpListener { .. }
                | R::ListenerHandoff { .. }
                | R::DatagramListener { .. }
                | R::MetadataDatagram { .. }
                | R::NativeIcmp { .. }
        )
    })
}
/// Actual metadata recipes retain different omitted defaults for DNS and
/// shared endpoints. Compare both concrete defaults; explicit=default has no
/// effect on that consumer while it can change the other consumer.
pub(in crate::runtime) fn metadata_budget_changed(
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    let a = &old.runtime;
    let b = &next.runtime;
    [1, 2].into_iter().any(|d| {
        a.effective_metadata_max_probe_sockets(d) != b.effective_metadata_max_probe_sockets(d)
    }) || [4, 8].into_iter().any(|d| {
        a.effective_metadata_max_probe_attempts(d) != b.effective_metadata_max_probe_attempts(d)
    }) || [1, 2]
        .into_iter()
        .any(|d| a.effective_metadata_max_send_calls(d) != b.effective_metadata_max_send_calls(d))
        || a.effective_quic_source_max_connections() != b.effective_quic_source_max_connections()
        || a.effective_quic_shared_endpoint_max_users()
            != b.effective_quic_shared_endpoint_max_users()
}
pub(in crate::runtime) fn metadata_source_resources(
    resources: &[zero_platform_tokio::contract::component::ResourceDeclaration],
) -> bool {
    resources.iter().any(|resource| {
        matches!(
            resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::MetadataDatagram { .. }
        )
    })
}
pub(in crate::runtime) fn inbound_work_source_changed(
    input: &InboundConfig,
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    crate::adapters::inbound_work_budgets::inbound_work_source_changed(input, old, next)
}
pub(in crate::runtime) fn inbound_work_budget_changed(
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    crate::adapters::inbound_work_budgets::inbound_work_budget_changed(old, next)
}
pub(in crate::runtime) fn inbound_work_resources(
    resources: &[zero_platform_tokio::contract::component::ResourceDeclaration],
) -> bool {
    use zero_platform_tokio::contract::component::ResourceDeclaration as R;
    resources.iter().any(|r| {
        matches!(
            r,
            R::TcpListener {
                accepted: Some(_),
                ..
            } | R::DatagramListener {
                accepted: Some(_),
                ..
            } | R::DatagramAssociations { .. }
        )
    })
}
pub(in crate::runtime) fn ss_budget_changed(old: &RuntimeConfig, next: &RuntimeConfig) -> bool {
    if carrier_budget_changed(old, next)
        || inbound_work_budget_changed(old, next)
        || old.inbounds.iter().chain(&next.inbounds).any(|input| {
            crate::adapters::metadata::metadata_source_budget_changed(input, old, next)
        })
    {
        return true;
    }
    if ss_source_budget_changed(old, next) {
        return true;
    }
    #[cfg(feature = "raw-ip-runtime")]
    if packet_budget_changed(old, next) {
        return true;
    }
    false
}
// Physical SS Source replacement is required only for these actual source
// proposals, not for a cap on providers to which that Source merely lends work.
pub(in crate::runtime) fn ss_source_budget_changed(
    old: &RuntimeConfig,
    next: &RuntimeConfig,
) -> bool {
    if old.runtime.effective_process_max_jobs() != next.runtime.effective_process_max_jobs()
        || old.runtime.effective_process_max_jobs_per_instance()
            != next.runtime.effective_process_max_jobs_per_instance()
    {
        return true;
    }
    crate::adapters::security_budgets::ss_source_resources_changed(old, next)
}

#[cfg(feature = "raw-ip-runtime")]
pub(in crate::runtime) fn packet_budget_changed(old: &RuntimeConfig, next: &RuntimeConfig) -> bool {
    old.runtime.effective_packet_return_max_frames()
        != next.runtime.effective_packet_return_max_frames()
        || old.runtime.effective_packet_return_max_bytes()
            != next.runtime.effective_packet_return_max_bytes()
        || old.runtime.effective_packet_return_total_max_frames()
            != next.runtime.effective_packet_return_total_max_frames()
        || old.runtime.effective_packet_return_total_max_bytes()
            != next.runtime.effective_packet_return_total_max_bytes()
        || old.runtime.effective_packet_transmit_total_max_frames()
            != next.runtime.effective_packet_transmit_total_max_frames()
        || old.runtime.effective_packet_transmit_total_max_bytes()
            != next.runtime.effective_packet_transmit_total_max_bytes()
        || old.runtime.effective_packet_native_read_total_max_frames()
            != next.runtime.effective_packet_native_read_total_max_frames()
        || old.runtime.effective_packet_native_read_total_max_bytes()
            != next.runtime.effective_packet_native_read_total_max_bytes()
}
pub(super) struct SourceBudgetPauses {
    // Same old roots remain fenced until the actual publication finishes.
    // This is admission metadata, never Native/Scope/IO close evidence.
    _users: Arc<crate::component::catalog::ReplacementUsersPause>,
    _sources: Vec<crate::runtime::inbound_owners::AcquisitionPause>,
    #[cfg(feature = "raw-ip-runtime")]
    _direct: Option<crate::runtime::packet_route::host::NativeDevicePaused>,
    #[cfg(feature = "wireguard")]
    _standalone: Vec<crate::runtime::packet_route::host::NativeDevicePaused>,
}
pub(super) async fn pause_sources<'a>(
    proxy: &'a Proxy,
    previous: &RuntimeConfig,
    next: &RuntimeConfig,
    changed_sources: &std::collections::BTreeSet<String>,
    candidate: &Arc<crate::component::catalog::ComponentPlan>,
    retirement: &mut Option<crate::component::catalog::PacketSourceRetirement<'a>>,
    network_restart: bool,
) -> Result<SourceBudgetPauses, EngineError> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    #[cfg(feature = "raw-ip-runtime")]
    let packet_changed = packet_budget_changed(previous, next) || network_restart;
    #[cfg(not(feature = "raw-ip-runtime"))]
    let _ = next;
    let carrier_changed = carrier_budget_changed(previous, next) || network_restart;
    let mut pauses = Vec::new();
    #[cfg(feature = "raw-ip-runtime")]
    let mut mains = Vec::new();
    for input in &previous.inbounds {
        let Some(original) = proxy.inbound_owners.owner(&input.tag) else {
            continue;
        };
        let needed = carrier_changed
            || input.protocol.protocol_name() == "shadowsocks"
            || changed_sources.contains(&input.tag);
        #[cfg(feature = "raw-ip-runtime")]
        let needed = needed || packet_changed && original.has_packet_source_pause();
        if !needed {
            continue;
        }
        #[cfg(feature = "raw-ip-runtime")]
        if original.has_packet_source_pause() {
            if let Some(main) = original.pause_packet_main_if_idle(deadline).await? {
                mains.push((original, main));
            }
            continue;
        }
        if let Some(pause) = original.pause_acquisition_if_idle(deadline).await? {
            pauses.push(pause);
        }
    }
    #[cfg(feature = "raw-ip-runtime")]
    if packet_changed || carrier_changed {
        if let Some(original) = proxy
            .inbound_owners
            .owner(crate::runtime::inbound_owners::system_tun::SYSTEM_TUN_OWNER)
        {
            if let Some(main) = original.pause_packet_main_if_idle(deadline).await? {
                mains.push((original, main));
            }
        }
    }
    // Only actual Source Main may close its idle taskless Packet-session
    // cache. Doing this after the catalog Users check would self-deadlock:
    // its exact BoxPacketSession still owns a charged Packet Lease until here.
    // No component/cached carrier receiver has been revoked in this phase.
    let user_pause = Arc::new(
        proxy
            .protocols
            .try_pause_replacement_users(candidate)
            .inspect_err(|source| {
                #[cfg(test)]
                eprintln!("actual replacement user preflight after Source Main pause: {source:?}");
                tracing::debug!(
                    ?source,
                    "actual component users remain charged after Source Main pause"
                );
            })
            .map_err(std::io::Error::other)?,
    );
    #[cfg(feature = "raw-ip-runtime")]
    let direct = if packet_changed || carrier_changed {
        proxy
            .protocols
            .fence_direct_packet_acquisition(deadline)
            .await?
    } else {
        None
    };
    #[cfg(feature = "wireguard")]
    let standalone = if packet_changed || carrier_changed {
        proxy
            .protocols
            .fence_standalone_packet_acquisition(deadline)
            .await?
    } else {
        Vec::new()
    };
    #[cfg(feature = "wireguard")]
    for (device, origin) in proxy.protocols.actual_packet_flow_sources(previous)? {
        let affected = packet_changed
            || carrier_changed
            || mains.iter().any(|(_, main)| {
                main.instance() == origin.instance()
                    && origin
                        .driver()
                        .is_some_and(|driver| main.scope().same_scope(&driver.scope_identity()))
            });
        if affected && !device.replacement_users_retained(&user_pause, &origin) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "actual Packet Source retains live business or unclassified original users",
            )
            .into());
        }
    }
    // Every actual business Source is now fenced and proved idle; only exact
    // cached receivers in this candidate's real StopOld set may be revoked.
    // Persist the replacement before callbacks; cancellation retains all debt.
    *retirement = proxy
        .protocols
        .begin_packet_source_retirement(candidate, user_pause.clone())
        .map_err(std::io::Error::other)?;
    #[cfg(feature = "raw-ip-runtime")]
    for (original, main) in mains {
        pauses.push(original.finish_packet_pause(main).await?);
    }
    #[cfg(feature = "raw-ip-runtime")]
    let direct = match direct {
        Some(fenced) => Some(
            fenced
                .finish(deadline)
                .await
                .map_err(std::io::Error::other)?,
        ),
        None => None,
    };
    #[cfg(feature = "wireguard")]
    let standalone = {
        let mut actual = Vec::new();
        for fenced in standalone {
            actual.push(
                fenced
                    .finish(deadline)
                    .await
                    .map_err(std::io::Error::other)?,
            );
        }
        actual
    };
    for pause in &pauses {
        pause.validate()?;
    }
    #[cfg(feature = "wireguard")]
    for pause in &standalone {
        pause.validate().map_err(std::io::Error::other)?;
    }
    #[cfg(feature = "raw-ip-runtime")]
    if let Some(direct) = &direct {
        direct.validate().map_err(std::io::Error::other)?;
    }
    Ok(SourceBudgetPauses {
        _users: user_pause,
        _sources: pauses,
        #[cfg(feature = "raw-ip-runtime")]
        _direct: direct,
        #[cfg(feature = "wireguard")]
        _standalone: standalone,
    })
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
            .inspect_err(|source| {
                tracing::warn!(
                    ?source,
                    "same catalog StopOld/ordinary start retained original failure"
                );
                #[cfg(test)]
                eprintln!("actual catalog StopOld original failure: {source:?}");
            })?;
        start_deferred_resources(proxy, config, services, preparing, retained).await
    })
}
fn start_deferred_resources<'a>(
    proxy: &'a Proxy,
    config: &'a Arc<RuntimeConfig>,
    services: &'a crate::protocol_registry::TcpRuntimeServices,
    preparing: &'a Arc<crate::runtime::inbound_owners::PreparingInbounds>,
    retained: &'a HashMap<String, InboundConfig>,
) -> BoxFuture<'a, Result<(), EngineError>> {
    Box::pin(async move {
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
                .inspect_err(|source| {
                    tracing::warn!(
                        ?source,
                        "same device resolve/deferred start retained original failure"
                    );
                    #[cfg(test)]
                    eprintln!("actual deferred device original failure: {source:?}");
                })?;
            let execution = services.execution();
            let plan = execution.components.as_deref().map_err(|error| {
                std::io::Error::other(crate::component::instance::retained(error.clone()))
            })?;
            for input in &config.inbounds {
                if retained.get(&input.tag) != Some(input) {
                    continue;
                }
                if let Some(original) = proxy.inbound_owners.packet_source_origin(input)? {
                    let memory=proxy.inbound_owners.packet_source_memory(input)?.ok_or_else(||std::io::Error::other("retained actual Packet Source lost its original memory view/layout"))?;
                    proxy
                        .protocols
                        .bind_actual_packet_source(config, plan, &input.tag, original, memory)?;
                }
            }
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = (services, preparing, retained);
        Ok(())
    })
}

#[cfg(feature = "raw-ip-runtime")]
pub(super) fn restart_tun_and_resources<'a>(
    proxy: &'a Proxy,
    config: &'a Arc<RuntimeConfig>,
    services: &'a crate::protocol_registry::TcpRuntimeServices,
    preparing: &'a Arc<crate::runtime::inbound_owners::PreparingInbounds>,
    retained: &'a HashMap<String, InboundConfig>,
) -> BoxFuture<'a, Result<crate::protocol_registry::TcpRuntimeServices, EngineError>> {
    Box::pin(async move {
        proxy.stop_tun_for_packet_budget(config).await?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut prepared = proxy
            .protocols
            .prepare_resource_start(config.clone(), deadline)
            .await?;
        preparing.ensure_open()?;
        // Only this point has actual old Source and component original receipts
        // and the target policy on the same roots, with BuildNew still sealed.
        proxy
            .reconcile_configured_tun_for_config(config.clone())
            .await?;
        proxy
            .protocols
            .refresh_prepared_device_components(&mut prepared, deadline)
            .await?;
        let services = services.with_prepared_components()?;
        preparing.ensure_open()?;
        prepared
            .start(deadline)
            .await
            .map_err(std::io::Error::other)?;
        start_deferred_resources(proxy, config, &services, preparing, retained).await?;
        Ok(services)
    })
}

pub(super) fn prepare_devices<'a>(
    proxy: &'a Proxy,
    config: &'a Arc<RuntimeConfig>,
    services: &'a crate::protocol_registry::TcpRuntimeServices,
) -> BoxFuture<'a, Result<ReloadDevices, EngineError>> {
    Box::pin(async move {
        proxy
            .reconcile_configured_tun_for_config(config.clone())
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

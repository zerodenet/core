use zero_config::RuntimeConfig;
use zero_engine::RouteMode;
use zero_engine::{EngineError, ResolvedLeafOutbound};

use super::service::{endpoint, ServiceNode};
use super::{PreparedTcpCandidateExecution, ProtocolInventory};
use crate::component::catalog::ComponentPlan;
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::dns::DnsQueryRole;

impl ProtocolInventory {
    pub(in crate::inventory) fn service_node(
        &self,
        ctx: OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
        components: &ComponentPlan,
    ) -> Result<Option<ServiceNode>, TcpOutboundFailure> {
        let failure = |error| TcpOutboundFailure {
            stage: "prepare_service",
            error: zero_engine::EngineError::Io(std::io::Error::other(error)),
            upstream_endpoint: None,
            network: None,
        };
        match leaf {
            ResolvedLeafOutbound::Direct {
                tag, dial_policy, ..
            } => {
                let slot = components.direct(*tag).map_err(failure)?;
                Ok(Some(ServiceNode {
                    slot,
                    tag: tag.map(str::to_owned),
                    endpoint: None,
                    wire_endpoint: None,
                    role: DnsQueryRole::Direct,
                    dial: dial_policy.clone(),
                }))
            }
            ResolvedLeafOutbound::Block { .. } => Ok(None),
            ResolvedLeafOutbound::Proxy { .. } => {
                let Some(candidate) =
                    self.native_tcp_candidate(ctx, leaf, Some(components), RouteMode::Flow)
                else {
                    return Ok(None);
                };
                let candidate = candidate?;
                let PreparedTcpCandidateExecution::Native(slot) = candidate.execution else {
                    return Ok(None);
                };
                let slot = slot.slot().map_err(failure)?;
                let Some((host, port)) = candidate.endpoint else {
                    return Ok((slot.accepted_carrier_provider().is_some()
                        || components.has_device_carrier_provider(slot.role_id()))
                    .then(|| ServiceNode {
                        dial: slot.dial.clone(),
                        slot,
                        tag: candidate.tag,
                        endpoint: None,
                        wire_endpoint: None,
                        role: DnsQueryRole::Node,
                    }));
                };
                Ok(Some(ServiceNode {
                    dial: slot.dial.clone(),
                    slot,
                    tag: candidate.tag,
                    endpoint: Some(endpoint(&host, port)),
                    wire_endpoint: Some(Destination::new(Address::Domain(host), port)),
                    role: DnsQueryRole::Node,
                }))
            }
        }
    }

    /// Project an engine leaf at the claim boundary. Consumers receive only the
    /// actual native owner, without inspecting protocol/config leaf variants.
    pub(in crate::inventory) fn native_tcp_owner(
        &self,
        ctx: crate::protocol_registry::OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
        components: &crate::component::catalog::ComponentPlan,
    ) -> Option<std::sync::Arc<crate::component::instance::ComponentSlot>> {
        match leaf {
            ResolvedLeafOutbound::Direct { tag, .. } => components.direct(*tag).ok(),
            ResolvedLeafOutbound::Block { .. } => None,
            ResolvedLeafOutbound::Proxy { .. } => {
                let candidate = self
                    .native_tcp_candidate(ctx, leaf, Some(components), RouteMode::Flow)?
                    .ok()?;
                match candidate.execution {
                    super::PreparedTcpCandidateExecution::Native(slot) => slot.slot().ok(),
                    _ => None,
                }
            }
        }
    }

    pub(in crate::inventory) fn native_tcp_candidate(
        &self,
        ctx: crate::protocol_registry::OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
        components: Option<&crate::component::catalog::ComponentPlan>,
        mode: RouteMode,
    ) -> Option<Result<super::PreparedTcpCandidate, crate::transport::TcpOutboundFailure>> {
        if let ResolvedLeafOutbound::Direct {
            tag, dial_policy, ..
        } = leaf
        {
            // Native Direct Packet does not provide Flow conversion. Ordinary
            // TCP and pool prefixes use the same already-compiled Direct role.
            if mode == RouteMode::Packet {
                return Some(Err(crate::runtime::path::missing_tcp_route_capability(
                    mode,
                )));
            }
            // All executable Direct callers select this same actual host
            // generation. No claim may construct an unowned socket fallback.
            let selected = components
                .ok_or_else(|| {
                    std::io::Error::other("Direct Stream requires its actual component plan")
                })
                .and_then(|plan| plan.direct(*tag).map_err(std::io::Error::other));
            let slot = match selected {
                Ok(slot) => slot,
                Err(source) => {
                    return Some(Err(TcpOutboundFailure {
                        stage: "prepare_component",
                        error: source.into(),
                        upstream_endpoint: None,
                        network: None,
                    }));
                }
            };
            if slot.dial != *dial_policy {
                return Some(Err(TcpOutboundFailure {
                    stage: "prepare_component",
                    error: std::io::Error::other("selected Direct dial generation changed").into(),
                    upstream_endpoint: None,
                    network: None,
                }));
            }
            return Some(Ok(super::PreparedTcpCandidate {
                health_tag: None,
                tag: Some(tag.unwrap_or("direct").into()),
                protocol: "direct".into(),
                endpoint: None,
                execution: super::PreparedTcpCandidateExecution::Native(
                    super::tcp::StreamRoleRef::new(&slot),
                ),
            }));
        }
        let ResolvedLeafOutbound::Proxy { identity } = leaf else {
            return None;
        };
        let outbound = ctx.config().outbounds.get(identity.config_index())?;
        let slot = components?.configured(&outbound.tag)?;
        if !slot
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Stream)
        {
            return None;
        }
        let failure = |error| crate::transport::TcpOutboundFailure {
            stage: "outbound_leaf_runtime",
            error,
            upstream_endpoint: None,
            network: None,
        };
        if let Some((reason, endpoint)) =
            zero_engine::EndpointAdmission::from_config(ctx.config()).outbound_denial(&outbound.tag)
        {
            return Some(Err(failure(EngineError::InvalidPlan {
                message: format!("{reason}: {endpoint}"),
            })));
        }
        if mode == RouteMode::Packet && !components?.has_device_carrier_provider(slot.role_id()) {
            return Some(Err(crate::runtime::path::missing_tcp_route_capability(
                mode,
            )));
        }
        // Device Stream opens the same original stack under the actual caller;
        // strict Packet no longer constructs a private Flow converter.
        let endpoint = match outbound.protocol.endpoint() {
            Some((server, port)) => Some((server.into(), port)),
            None if slot.accepted_carrier_provider().is_some() => None,
            None if [zero_platform_tokio::contract::component::RoleKind::Packet,
                     zero_platform_tokio::contract::component::RoleKind::Stream,
                     zero_platform_tokio::contract::component::RoleKind::Datagram]
                .iter().all(|role| slot.recipe.spec().roles.contains(role))
                && components?.device_component(slot.id).is_ok() => None,
            None => return Some(Err(failure(EngineError::InvalidPlan {
                message: "registered Stream has neither a node endpoint nor a canonical device dependency".into(),
            }))),
        };
        Some(Ok(super::PreparedTcpCandidate {
            health_tag: endpoint.as_ref().map(|_| outbound.tag.clone()),
            tag: Some(outbound.tag.clone()),
            protocol: outbound.protocol.protocol_name().into(),
            endpoint,
            execution: super::PreparedTcpCandidateExecution::Native(
                super::tcp::StreamRoleRef::new(&slot),
            ),
        }))
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn outbound_device_health(
        &self,
        config: &RuntimeConfig,
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        self.devices.health(config)
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn device_health_row_count(&self, config: &RuntimeConfig) -> std::io::Result<usize> {
        self.devices.health_row_count(config)
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn fill_device_health_rows(
        &self,
        config: &RuntimeConfig,
        rows: &mut Vec<crate::adapters::devices::HealthRow>,
    ) {
        self.devices.fill_health_rows(config, rows);
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn prepare_devices(
        &self,
        config: &RuntimeConfig,
        services: crate::protocol_registry::TcpRuntimeServices,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<crate::adapters::devices::PreparedDevices, EngineError> {
        use std::sync::Arc;

        let admission = zero_engine::EndpointAdmission::from_snapshot(services.snapshot());
        let execution = services.execution();
        let components = execution
            .components
            .as_deref()
            .map_err(|source| EngineError::Io(std::io::Error::other(source.clone())))?;
        let packet_paths = self.prepare_device_packet_paths(
            config,
            &admission,
            zero_core::component::NetworkGeneration(services.upstream().egress_generation()),
            Some(components),
        )?;
        let context = crate::adapters::devices::OutboundDevicePreparationContext {
            direct_packet_device: config.runtime.network.direct_packet_device.clone(),
            mtu: config.runtime.network.mtu,
            stack_options: crate::runtime::raw_ip::RawIpStackOptions::from_config_with_packet_root(
                &config.runtime.network.udp_receive_queue,
                self.devices.packet_return_totals.clone(),
            )
            .with_packet_native_read_limits(
                config
                    .runtime
                    .effective_packet_native_read_total_max_frames(),
                config
                    .runtime
                    .effective_packet_native_read_total_max_bytes(),
            )
            .with_packet_transmit_limits(
                config.runtime.effective_packet_transmit_total_max_frames(),
                config.runtime.effective_packet_transmit_total_max_bytes(),
            )
            .with_packet_driver_metadata_limits(
                config
                    .runtime
                    .effective_packet_driver_metadata_total_max_allocations(),
                config
                    .runtime
                    .effective_packet_driver_metadata_total_max_bytes(),
            )
            .with_packet_return_rows(&config.runtime)
            .with_packet_return_limits(
                config.runtime.effective_packet_return_max_frames(),
                config.runtime.effective_packet_return_max_bytes(),
            )
            .with_packet_return_totals(
                self.devices.packet_return_totals.clone(),
                config.runtime.effective_packet_return_total_max_frames(),
                config.runtime.effective_packet_return_total_max_bytes(),
            ),
            network_generation: zero_core::component::NetworkGeneration(
                services.upstream().egress_generation(),
            ),
            packet_paths: Arc::new(packet_paths),
        };
        // Outer handshakes begin here, after the actual component originals
        // have been installed. Interrupted preparation leaves those originals
        // with their slots and the pool's retained retirement queue.
        let preparation = Box::pin(self.devices.prepare(config, context, admission));
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(crate::runtime::inbound_owners::stopped_preparation()),
            result = preparation => result,
        }
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn request_device_shutdown(&self) {
        self.devices.request_shutdown();
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn close_devices(&self) -> Result<(), EngineError> {
        self.devices.close().await
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) fn prepare_inbound_services(
        &self,
        config: &std::sync::Arc<RuntimeConfig>,
    ) -> Result<Vec<crate::runtime::inbound_service::NativeServiceOperation>, EngineError> {
        self.registry.prepare_inbound_services(config)
    }

    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        self.registry.on_config_reloaded(config);
    }
}

use std::path::Path;

use zero_config::RuntimeConfig;
#[cfg(feature = "raw-ip-runtime")]
use zero_engine::OutboundIdentity;
use zero_engine::RouteMode;
use zero_engine::{EngineError, ResolvedLeafOutbound};

use super::service::{endpoint, ServiceNode};
use super::{PreparedTcpCandidateExecution, ProtocolInventory};
use crate::component::catalog::ComponentPlan;
use crate::protocol_registry::OutboundAdapterContext;
use crate::protocol_registry::{ClaimedOutboundLeaf, OutboundLeafRuntime};
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::transport::TcpOutboundFailure;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::dns::DnsQueryRole;

#[derive(Clone)]
pub(crate) struct ClaimedInventoryLeaf<'a> {
    claimed: ClaimedOutboundLeaf<'a>,
}

impl<'a> ClaimedInventoryLeaf<'a> {
    fn new(claimed: ClaimedOutboundLeaf<'a>) -> Self {
        Self { claimed }
    }

    pub(crate) fn runtime(&self) -> OutboundLeafRuntime {
        self.claimed.runtime.clone()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_claimed(self) -> ClaimedOutboundLeaf<'a> {
        self.claimed
    }

    pub(crate) fn prepare_tcp_connect_for_route(
        &self,
        source_dir: Option<&Path>,
        mode: RouteMode,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, crate::transport::TcpOutboundFailure> {
        self.claimed.prepare_tcp_connect_for_route(source_dir, mode)
    }

    pub(crate) fn prepare_tcp_relay_hop(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<(String, u16, Box<dyn PreparedTcpRelayOperation>), EngineError> {
        self.claimed.prepare_tcp_relay_hop(source_dir)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_flow_for_route(
        &self,
        source_dir: Option<&Path>,
        mode: RouteMode,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, crate::runtime::udp_dispatch::FlowFailure>
    {
        self.claimed.prepare_udp_flow_for_route(source_dir, mode)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn native_packet_returns_correlated(&self) -> bool {
        self.claimed.native_packet_returns_correlated()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_packet_route(
        &self,
    ) -> Option<Box<dyn crate::runtime::packet_route::PreparedPacketRouteOperation>> {
        self.claimed.prepare_packet_route()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_translated_packet_route(
        &self,
    ) -> Option<Box<dyn crate::runtime::packet_route::PreparedPacketRouteOperation>> {
        self.claimed.prepare_translated_packet_route()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare_datagram_exchange(
        &self,
    ) -> Option<Box<dyn crate::runtime::packet_route::PreparedDatagramExchangeOperation>> {
        self.claimed.prepare_datagram_exchange()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn data_plane_sinks(&self) -> crate::runtime::network_graph::PlaneSet {
        self.claimed.data_plane_sinks()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_packet_path(
        &self,
        source_dir: Option<&std::path::Path>,
    ) -> Option<
        Box<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    > {
        self.claimed.prepare_udp_packet_path(source_dir)
    }
}

mod relay;
pub(crate) use relay::ClaimedRelayChain;

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
                let Some(candidate) = self.native_tcp_candidate(ctx, leaf, Some(components)) else {
                    return Ok(None);
                };
                let candidate = candidate?;
                let PreparedTcpCandidateExecution::Native(slot) = candidate.execution else {
                    return Ok(None);
                };
                let Some((host, port)) = candidate.endpoint else {
                    return Ok(None);
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
                    .native_tcp_candidate(ctx, leaf, Some(components))?
                    .ok()?;
                match candidate.execution {
                    super::PreparedTcpCandidateExecution::Native(slot) => Some(slot),
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
    ) -> Option<Result<super::PreparedTcpCandidate, crate::transport::TcpOutboundFailure>> {
        let ResolvedLeafOutbound::Proxy { identity } = leaf else {
            return None;
        };
        let outbound = ctx.config().outbounds.get(identity.config_index())?;
        let slot = components?.configured(&outbound.tag)?;
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
        let Some((server, port)) = outbound.protocol.endpoint() else {
            return Some(Err(failure(EngineError::InvalidPlan {
                message: "native network Stream is missing its declared endpoint".into(),
            })));
        };
        Some(Ok(super::PreparedTcpCandidate {
            health_tag: Some(outbound.tag.clone()),
            tag: Some(outbound.tag.clone()),
            protocol: outbound.protocol.protocol_name().into(),
            endpoint: Some((server.into(), port)),
            execution: super::PreparedTcpCandidateExecution::Native(slot),
        }))
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn claim_config_outbound<'a>(
        &self,
        config: &'a RuntimeConfig,
        index: usize,
    ) -> Result<ClaimedInventoryLeaf<'a>, EngineError> {
        self.claim_outbound_leaf(
            config,
            ResolvedLeafOutbound::Proxy {
                identity: OutboundIdentity::from_config_index(index),
            },
        )
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn claim_config_relay_chain<'a>(
        &self,
        config: &'a RuntimeConfig,
        indices: &[usize],
    ) -> Result<ClaimedRelayChain<'a>, EngineError> {
        let chain = indices
            .iter()
            .copied()
            .map(|index| ResolvedLeafOutbound::Proxy {
                identity: OutboundIdentity::from_config_index(index),
            });
        self.claim_relay_chain(config, chain, |error| error, |error| error)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn outbound_device_health(
        &self,
        config: &RuntimeConfig,
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        self.registry.outbound_device_health(config)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn prepare_outbound_devices(
        &self,
        config: &RuntimeConfig,
        services: crate::protocol_registry::TcpRuntimeServices,
    ) -> Result<Vec<Box<dyn crate::protocol_registry::PreparedOutboundDeviceState>>, EngineError>
    {
        use std::sync::Arc;

        let admission = zero_engine::EndpointAdmission::from_snapshot(services.snapshot());
        let packet_paths = self.prepare_device_packet_paths(
            config,
            &admission,
            zero_core::component::NetworkGeneration(services.upstream().egress_generation()),
        )?;
        let context = crate::protocol_registry::OutboundDevicePreparationContext {
            traffic: services.engine().clone(),
            direct_packet_device: config.runtime.network.direct_packet_device.clone(),
            mtu: config.runtime.network.mtu,
            stack_options: crate::runtime::raw_ip::RawIpStackOptions::from_config(
                &config.runtime.network.udp_receive_queue,
            ),
            endpoint_bindings: Arc::new(config.endpoint_bindings()),
            disabled_outbounds: Arc::new(
                config
                    .outbounds
                    .iter()
                    .filter(|outbound| admission.outbound_denial(&outbound.tag).is_some())
                    .map(|outbound| outbound.tag.clone())
                    .collect(),
            ),
            upstream: services.upstream(),
            packet_path_services:
                crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(
                    &services.execution(),
                ),
            packet_paths: Arc::new(packet_paths),
        };
        self.registry
            .prepare_outbound_devices(config, context, admission)
            .await
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn shutdown_outbound_devices(
        &self,
    ) -> Vec<crate::protocol_registry::OutboundDeviceCompletion> {
        self.registry.shutdown_outbound_devices()
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) fn prepare_inbound_services(
        &self,
        config: &RuntimeConfig,
    ) -> Result<
        Vec<Box<dyn crate::runtime::inbound_service::PreparedInboundServiceOperation>>,
        EngineError,
    > {
        self.registry.prepare_inbound_services(config)
    }

    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        self.registry.on_config_reloaded(config);
    }

    pub(crate) fn claim_outbound_leaf<'a>(
        &self,
        config: &'a RuntimeConfig,
        leaf: ResolvedLeafOutbound<'a>,
    ) -> Result<ClaimedInventoryLeaf<'a>, EngineError> {
        if let ResolvedLeafOutbound::Proxy { identity } = &leaf {
            if let Some(outbound) = config.outbounds.get(identity.config_index()) {
                if let Some((reason, endpoint_id)) =
                    zero_engine::EndpointAdmission::from_config(config)
                        .outbound_denial(&outbound.tag)
                {
                    return Err(EngineError::InvalidPlan {
                        message: format!("{reason}: {endpoint_id}"),
                    });
                }
            }
        }
        let claimed = self.registry.claim_outbound_leaf(config, leaf)?;
        Ok(ClaimedInventoryLeaf::new(claimed))
    }

    pub(in crate::inventory) fn claim_relay_chain<'a, E, F, G>(
        &self,
        config: &'a RuntimeConfig,
        chain: impl IntoIterator<Item = ResolvedLeafOutbound<'a>>,
        map_first_error: F,
        map_relay_error: G,
    ) -> Result<ClaimedRelayChain<'a>, E>
    where
        F: FnOnce(EngineError) -> E,
        G: Fn(EngineError) -> E,
    {
        let mut chain = chain.into_iter();
        let first = chain.next().expect("relay chain must have at least 2 hops");
        let second = chain.next().expect("relay chain must have at least 2 hops");

        let first = self
            .claim_outbound_leaf(config, first)
            .map_err(map_first_error)?;
        let map_relay_error = &map_relay_error;
        let relay_hops = std::iter::once(second)
            .chain(chain)
            .map(|leaf| {
                self.claim_outbound_leaf(config, leaf)
                    .map_err(map_relay_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ClaimedRelayChain::new(first, relay_hops))
    }
}

//! A standard Stream node never requires a compatibility TCP claimer.
use super::{
    PreparedTcpCandidate, PreparedTcpCandidateExecution, PreparedTcpRelayChain,
    PreparedTcpRelayExecution, PreparedTcpRelayHop,
};
use crate::{
    inventory::ProtocolInventory, protocol_registry::OutboundAdapterContext,
    transport::TcpOutboundFailure,
};
use zero_engine::{EngineError, ResolvedLeafOutbound, RouteMode};

impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_tcp_relay_chain<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        leaves: &[ResolvedLeafOutbound<'a>],
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> Result<PreparedTcpRelayChain, TcpOutboundFailure> {
        self.prepare_tcp_relay_chain_inner(
            ctx,
            leaves,
            components,
            #[cfg(feature = "udp-runtime")]
            None,
        )
    }
    #[cfg(feature = "udp-runtime")]
    pub(in crate::inventory) fn prepare_device_tcp_relay_chain<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        leaves: &[ResolvedLeafOutbound<'a>],
        components: Option<&crate::component::catalog::ComponentPlan>,
        sources: &[Option<crate::protocol_registry::ColdPacketPathProjection>],
    ) -> Result<PreparedTcpRelayChain, TcpOutboundFailure> {
        if sources.len() != leaves.len() {
            return Err(failure(
                "relay_prepare",
                EngineError::Io(std::io::Error::other("cold formatter leaf count changed")),
            ));
        }
        self.prepare_tcp_relay_chain_inner(ctx, leaves, components, Some(sources))
    }
    fn prepare_tcp_relay_chain_inner<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        leaves: &[ResolvedLeafOutbound<'a>],
        components: Option<&crate::component::catalog::ComponentPlan>,
        #[cfg(feature = "udp-runtime")] sources: Option<
            &[Option<crate::protocol_registry::ColdPacketPathProjection>],
        >,
    ) -> Result<PreparedTcpRelayChain, TcpOutboundFailure> {
        let first_leaf = leaves
            .first()
            .expect("validated relay has at least two nodes");
        // An unmigrated pool may retain its entire shorter prefix. Keep that
        // prefix with its existing owner until real carrier borrowing qualifies;
        // only the ordinary Stream suffix after it may use request-bound roles.
        let last_legacy = leaves
            .iter()
            .rposition(|leaf| self.native_tcp_candidate(ctx, leaf, components).is_none());
        let native_at = |index: usize, leaf: &ResolvedLeafOutbound<'a>| {
            let selected = self.native_tcp_candidate(ctx, leaf, components);
            #[cfg(feature = "udp-runtime")]
            if sources.is_some() {
                return selected;
            }
            if last_legacy.is_some_and(|last| index < last)
                && matches!(selected, Some(Ok(_)))
                && self.claim_outbound_leaf(ctx.config(), leaf.clone()).is_ok()
            {
                None
            } else {
                selected
            }
        };
        #[cfg(feature = "udp-runtime")]
        let first_datagram;
        let first = if let Some(native) = native_at(0, first_leaf) {
            let candidate = native?;
            #[cfg(feature = "udp-runtime")]
            {
                first_datagram = match sources {
                    Some(values) => self
                        .prepared_source_packet_path(components, values[0].as_ref())
                        .map_err(|error| failure("relay_prepare", error))?,
                    None => self.optional_tcp_packet_path(ctx, first_leaf, components),
                };
            }
            candidate
        } else {
            #[cfg(feature = "udp-runtime")]
            if sources.is_some() {
                return Err(failure(
                    "relay_prepare",
                    EngineError::Io(std::io::Error::other(
                        "Source prefix lacks its actual prepared Stream role",
                    )),
                ));
            }
            let claimed = self
                .claim_outbound_leaf(ctx.config(), first_leaf.clone())
                .map_err(|error| failure("outbound_leaf_runtime", error))?;
            #[cfg(feature = "udp-runtime")]
            {
                first_datagram = claimed.udp_packet_path();
            }
            self.prepare_claimed_tcp_candidate(ctx, &claimed, RouteMode::Flow)?
        };
        let mut chain = PreparedTcpRelayChain {
            first,
            #[cfg(feature="udp-runtime")]
            datagram_prefixes:vec![first_datagram.and_then(crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier::new)],
            relay_hops:Vec::with_capacity(leaves.len()-1),
        };
        for (index, leaf) in leaves.iter().enumerate().skip(1) {
            #[cfg(feature = "udp-runtime")]
            let packet;
            let hop = if let Some(native) = native_at(index, leaf) {
                let candidate = native.map_err(|mut error| {
                    error.stage = "relay_prepare";
                    error
                })?;
                #[cfg(feature = "udp-runtime")]
                {
                    packet = if index + 1 < leaves.len() {
                        match sources {
                            Some(values) => self
                                .prepared_source_packet_path(components, values[index].as_ref())
                                .map_err(|error| failure("relay_prepare", error))?,
                            None => self.optional_tcp_packet_path(ctx, leaf, components),
                        }
                    } else {
                        None
                    };
                }
                native_hop(candidate)
            } else {
                #[cfg(feature = "udp-runtime")]
                if sources.is_some() {
                    return Err(failure(
                        "relay_prepare",
                        EngineError::Io(std::io::Error::other(
                            "Source relay lacks its actual prepared Stream role",
                        )),
                    ));
                }
                let claimed = self
                    .claim_outbound_leaf(ctx.config(), leaf.clone())
                    .map_err(|error| failure("relay_prepare", error))?;
                #[cfg(feature = "udp-runtime")]
                {
                    packet = if index + 1 < leaves.len() {
                        claimed.udp_packet_path()
                    } else {
                        None
                    };
                }
                self.prepare_claimed_tcp_relay_hop(ctx, &claimed)
                    .map_err(|error| failure("relay_prepare", error))?
            };
            #[cfg(feature = "udp-runtime")]
            if index + 1 < leaves.len() {
                append_datagram(
                    &mut chain,
                    &hop,
                    packet,
                    sources.map(|values| {
                        values[index]
                            .as_ref()
                            .and_then(|value| value.formatter.clone())
                    }),
                );
            }
            chain.relay_hops.push(hop);
        }
        Ok(chain)
    }

    #[cfg(feature = "udp-runtime")]
    pub(in crate::inventory) fn prepared_source_packet_path(
        &self,
        components: Option<&crate::component::catalog::ComponentPlan>,
        projection: Option<&crate::protocol_registry::ColdPacketPathProjection>,
    ) -> Result<Option<std::sync::Arc<dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation>>, EngineError>{
        let Some(projection) = projection else {
            return Ok(None);
        };
        let plan = components.ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "Source packet metadata lacks its actual candidate plan",
            ))
        })?;
        let tag = projection.descriptor.tag.as_deref().ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "Source packet metadata lacks its configured owner tag",
            ))
        })?;
        let slot = plan.configured(tag).ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "Source packet metadata owner is absent from its actual plan",
            ))
        })?;
        Ok(Some(crate::component::stream_bridge::datagram::packet_path::StandardPacketPath::prepare_projection(&slot, projection)))
    }

    /// Optional, unmigrated Datagram-carrier projection only. Its absence
    /// cannot require a standard Stream protocol to implement a legacy SPI.
    #[cfg(feature = "udp-runtime")]
    pub(in crate::inventory) fn optional_tcp_packet_path(
        &self,
        ctx: OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> Option<
        std::sync::Arc<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    > {
        if let Some(operation) = self
            .claim_outbound_leaf(ctx.config(), leaf.clone())
            .ok()
            .and_then(|claim| claim.udp_packet_path())
        {
            return Some(operation);
        }
        let ResolvedLeafOutbound::Proxy { identity } = leaf else {
            return None;
        };
        let input = ctx.config().outbounds.get(identity.config_index())?;
        let slot = components?.configured(input.tag())?;
        if !slot
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram)
        {
            return None;
        }
        let (server, port) = input.protocol.endpoint()?;
        Some(
            crate::component::stream_bridge::datagram::packet_path::StandardPacketPath::prepare(
                &slot,
                input.tag(),
                server,
                port,
            ),
        )
    }
}

fn native_hop(candidate: PreparedTcpCandidate) -> PreparedTcpRelayHop {
    let PreparedTcpCandidateExecution::Native(slot) = candidate.execution else {
        unreachable!("native candidate selects an actual Stream slot")
    };
    let (server, port) = candidate
        .endpoint
        .expect("native Stream has a validated endpoint");
    PreparedTcpRelayHop {
        tag: candidate.tag.expect("native Stream has a configured tag"),
        protocol: candidate.protocol,
        server,
        port,
        execution: PreparedTcpRelayExecution::Native(super::native::StreamRoleRef::new(&slot)),
    }
}

#[cfg(feature = "udp-runtime")]
fn append_datagram(
    chain: &mut PreparedTcpRelayChain,
    hop: &PreparedTcpRelayHop,
    operation: Option<
        std::sync::Arc<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    >,
    source: Option<Option<crate::runtime::udp_flow::packet_path::UdpDatagramSource>>,
) {
    let prefix = super::PreparedTcpRelayPrefix {
        first: chain.first.clone(),
        datagram_prefixes: chain.datagram_prefixes.clone(),
        relay_hops: chain.relay_hops.clone(),
        final_tag: hop.tag.clone(),
        final_protocol: hop.protocol.clone(),
        final_server: hop.server.clone(),
        final_port: hop.port,
    };
    let previous = chain.datagram_prefixes.last().cloned().flatten();
    chain
        .datagram_prefixes
        .push(operation.and_then(|operation| {
        match source {
            Some(source)=>crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier::extend_with_source(previous,operation,prefix,source),
            None=>crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier::extend(previous,operation,prefix),
        }
    }));
}
fn failure(stage: &'static str, error: EngineError) -> TcpOutboundFailure {
    TcpOutboundFailure {
        stage,
        error,
        upstream_endpoint: None,
        network: None,
    }
}

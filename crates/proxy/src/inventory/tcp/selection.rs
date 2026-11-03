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
            if last_legacy.is_some_and(|last| index < last) && matches!(selected, Some(Ok(_))) {
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
                first_datagram = self.optional_tcp_packet_path(ctx, first_leaf);
            }
            candidate
        } else {
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
                        self.optional_tcp_packet_path(ctx, leaf)
                    } else {
                        None
                    };
                }
                native_hop(candidate)
            } else {
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
                append_datagram(&mut chain, &hop, packet);
            }
            chain.relay_hops.push(hop);
        }
        Ok(chain)
    }

    /// Optional, unmigrated Datagram-carrier projection only. Its absence
    /// cannot require a standard Stream protocol to implement a legacy SPI.
    #[cfg(feature = "udp-runtime")]
    fn optional_tcp_packet_path(
        &self,
        ctx: OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
    ) -> Option<
        std::sync::Arc<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation,
        >,
    > {
        self.claim_outbound_leaf(ctx.config(), leaf.clone())
            .ok()?
            .udp_packet_path()
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
        crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier::extend(
            previous, operation, prefix,
        )
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

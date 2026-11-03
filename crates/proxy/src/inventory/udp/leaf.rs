use super::super::ProtocolInventory;
use crate::inventory::ClaimedInventoryLeaf;
use crate::protocol_registry::{OutboundAdapterContext, UdpAdapterContext};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use zero_engine::{ResolvedLeafOutbound, RouteMode};

pub(crate) enum PreparedUdpLeafCandidate<'a> {
    Block {
        tag: String,
    },
    #[cfg(feature = "upstream-association-runtime")]
    Datagram(super::DatagramAssociationTarget),
    #[cfg(feature = "managed-stream-runtime")]
    DatagramFlow(super::DatagramAssociationTarget),
    Flow(Box<dyn PreparedUdpFlowOperation + 'a>),
}

impl ProtocolInventory {
    pub(super) fn prepare_udp_candidate<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        candidate: &ResolvedLeafOutbound<'a>,
        mode: RouteMode,
    ) -> Result<super::leaf::PreparedUdpLeafCandidate<'a>, FlowFailure> {
        #[cfg(feature = "upstream-association-runtime")]
        if mode != RouteMode::Packet {
            if let Some(native) = super::association::prepare(&ctx, candidate) {
                return native.map(super::leaf::PreparedUdpLeafCandidate::Datagram);
            }
        }
        #[cfg(feature = "managed-stream-runtime")]
        if mode != RouteMode::Packet {
            if let Some(native) = super::association::prepare_flow(&ctx, candidate) {
                return native.map(PreparedUdpLeafCandidate::DatagramFlow);
            }
        }
        let claimed = self
            .claim_outbound_leaf(ctx.config(), candidate.clone())
            .map_err(|error| FlowFailure {
                stage: "outbound_leaf_runtime",
                error,
                upstream: None,
            })?;
        self.prepare_claimed_udp_leaf_candidate(ctx, claimed, mode)
    }

    pub(in crate::inventory) fn prepare_claimed_udp_leaf_candidate<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        claimed: ClaimedInventoryLeaf<'a>,
        mode: RouteMode,
    ) -> Result<PreparedUdpLeafCandidate<'a>, FlowFailure> {
        let runtime = claimed.runtime();
        if !ctx.udp_enabled_for_outbound(runtime.udp_policy_tag.as_deref()) {
            return Err(FlowFailure {
                stage: "udp_policy",
                error: zero_engine::EngineError::Io(std::io::Error::other(
                    "udp disabled for outbound",
                )),
                upstream: runtime.endpoint.map(|endpoint| endpoint.upstream()),
            });
        }
        if matches!(runtime.tcp_path, TcpPathCategory::Block) {
            return Ok(PreparedUdpLeafCandidate::Block {
                tag: runtime.kernel_tag.unwrap_or_else(|| "block".to_owned()),
            });
        }

        let operation = claimed.into_udp_flow_for_route(mode)?;
        Ok(PreparedUdpLeafCandidate::Flow(operation))
    }
}

impl ProtocolInventory {
    /// Project raw relay leaves once at the owning inventory preparation boundary.
    /// The continuation only receives existing prepared carriers and final claim.
    pub(super) fn prepare_standard_first_udp_relay<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        session: &'a zero_core::Session,
        chain: &[ResolvedLeafOutbound<'a>],
        payload: &'a [u8],
    ) -> Result<crate::runtime::udp_dispatch::relay::PreparedUdpRelayChain<'a>, FlowFailure> {
        for leaf in chain {
            if let ResolvedLeafOutbound::Proxy { identity } = leaf {
                let input = ctx
                    .config()
                    .outbounds
                    .get(identity.config_index())
                    .ok_or_else(|| FlowFailure {
                        stage: "relay_prepare",
                        error: std::io::Error::other("relay outbound retired").into(),
                        upstream: None,
                    })?;
                if !ctx.udp_enabled_for_outbound(Some(input.tag())) {
                    return Err(FlowFailure {
                        stage: "udp_policy",
                        error: std::io::Error::other("udp disabled for relay chain outbound")
                            .into(),
                        upstream: None,
                    });
                }
            }
        }
        let execution = ctx.packet_path_execution_services().tcp();
        let prepared = self
            .prepare_tcp_relay_chain(
                OutboundAdapterContext::new(ctx.config()),
                chain,
                execution.components.as_deref().ok(),
            )
            .map_err(|failure| FlowFailure {
                stage: failure.stage,
                error: failure.error,
                upstream: failure.upstream_endpoint.map(|endpoint| *endpoint),
            })?;
        let last = chain.last().expect("validated relay has final hop");
        let claimed = self
            .claim_outbound_leaf(ctx.config(), last.clone())
            .map_err(|error| FlowFailure {
                stage: "relay_prepare",
                error,
                upstream: None,
            })?;
        self.prepare_udp_relay_continuation(session, prepared, claimed, payload)
    }
}

use super::super::ProtocolInventory;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::FlowFailure;
use zero_engine::{ResolvedLeafOutbound, RouteMode};

pub(crate) enum PreparedUdpLeafCandidate {
    Direct(super::DirectDatagramTarget),
    Block {
        tag: String,
    },
    #[cfg(feature = "upstream-association-runtime")]
    Datagram(super::DatagramAssociationTarget),
    #[cfg(feature = "managed-stream-runtime")]
    DatagramFlow(super::DatagramAssociationTarget),
}

impl ProtocolInventory {
    pub(super) fn prepare_udp_candidate<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        candidate: &ResolvedLeafOutbound<'a>,
        mode: RouteMode,
    ) -> Result<super::leaf::PreparedUdpLeafCandidate, FlowFailure> {
        if let ResolvedLeafOutbound::Direct {
            tag,
            dial_policy,
            dial_generation,
        } = candidate
        {
            return super::DirectDatagramTarget::prepare(
                &ctx,
                *tag,
                dial_policy,
                *dial_generation,
                mode,
            )
            .map(PreparedUdpLeafCandidate::Direct);
        }
        #[cfg(feature = "upstream-association-runtime")]
        if mode != RouteMode::Packet {
            if let Some(native) = super::association::prepare(&ctx, candidate) {
                return native.map(super::leaf::PreparedUdpLeafCandidate::Datagram);
            }
        }
        #[cfg(feature = "managed-stream-runtime")]
        {
            if let Some(native) = super::association::prepare_flow(&ctx, candidate, mode) {
                return native.map(PreparedUdpLeafCandidate::DatagramFlow);
            }
        }
        if let ResolvedLeafOutbound::Block { tag } = candidate {
            if !ctx.udp_enabled_for_outbound(None) {
                return Err(FlowFailure {
                    stage: "udp_policy",
                    error: std::io::Error::other("udp disabled for outbound").into(),
                    upstream: None,
                });
            }
            return Ok(PreparedUdpLeafCandidate::Block {
                tag: tag.unwrap_or("block").into(),
            });
        }
        Err(crate::runtime::path::missing_udp_route_capability(mode))
    }
}

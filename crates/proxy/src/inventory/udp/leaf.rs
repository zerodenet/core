use super::super::ProtocolInventory;
use crate::inventory::ClaimedInventoryLeaf;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::path::TcpPathCategory;
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use zero_engine::RouteMode;

pub(crate) enum PreparedUdpLeafCandidate<'a> {
    Block { tag: String },
    Flow(Box<dyn PreparedUdpFlowOperation + 'a>),
}

impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_claimed_udp_leaf_candidate<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        claimed: &ClaimedInventoryLeaf<'a>,
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

        let operation = claimed.prepare_udp_flow_for_route(ctx.source_dir(), mode)?;
        Ok(PreparedUdpLeafCandidate::Flow(operation))
    }
}

use zero_engine::{ResolvedLeafOutbound, RouteMode};

use super::super::ProtocolInventory;
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

#[derive(Clone)]
pub(crate) struct PreparedTcpCandidate {
    pub(crate) health_tag: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) protocol: String,
    pub(crate) endpoint: Option<(String, u16)>,
    pub(crate) execution: PreparedTcpCandidateExecution,
}

#[derive(Clone)]
pub(crate) enum PreparedTcpCandidateExecution {
    // Only the actual execution acquires a strong current-generation slot.
    Native(super::native::StreamRoleRef),
    Block { tag: String },
}

impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_tcp_candidate(
        &self,
        ctx: OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
        components: Option<&crate::component::catalog::ComponentPlan>,
        mode: RouteMode,
    ) -> Result<PreparedTcpCandidate, TcpOutboundFailure> {
        if let ResolvedLeafOutbound::Block { tag } = leaf {
            return Ok(PreparedTcpCandidate {
                health_tag: None,
                tag: tag.map(str::to_owned),
                protocol: "block".into(),
                endpoint: None,
                execution: PreparedTcpCandidateExecution::Block {
                    tag: tag.unwrap_or("block").into(),
                },
            });
        }
        self.native_tcp_candidate(ctx, leaf, components, mode)
            .unwrap_or_else(|| Err(crate::runtime::path::missing_tcp_route_capability(mode)))
    }
}

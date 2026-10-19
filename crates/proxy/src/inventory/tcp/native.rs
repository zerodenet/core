//! Materialize an already selected chain of real, built Stream components.
use super::super::ProtocolInventory;
use super::PreparedTcpCandidateExecution;
use crate::{
    component::{catalog::ComponentPlan, instance::ComponentSlot},
    protocol_registry::OutboundAdapterContext,
    transport::TcpOutboundFailure,
};
use std::sync::Arc;
use zero_engine::ResolvedLeafOutbound;

pub(crate) struct PreparedNativeTcpRelayChain {
    pub(crate) nodes: Vec<PreparedNativeTcpNode>,
}
pub(crate) struct PreparedNativeTcpNode {
    pub(crate) slot: Arc<ComponentSlot>,
    pub(crate) tag: String,
    pub(crate) protocol: String,
    pub(crate) endpoint: (String, u16),
    pub(crate) dial: zero_traits::DialPolicy,
}
impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_native_tcp_relay_chain(
        &self,
        ctx: OutboundAdapterContext<'_>,
        chain: &[ResolvedLeafOutbound<'_>],
        components: Option<&ComponentPlan>,
    ) -> Option<Result<PreparedNativeTcpRelayChain, TcpOutboundFailure>> {
        let components = components?;
        let mut nodes = Vec::with_capacity(chain.len());
        for (index, leaf) in chain.iter().enumerate() {
            let candidate = match self.native_tcp_candidate(ctx, leaf, Some(components))? {
                Ok(candidate) => candidate,
                Err(mut error) => {
                    if index > 0 {
                        error.stage = "relay_prepare";
                    }
                    return Some(Err(error));
                }
            };
            let PreparedTcpCandidateExecution::Native(slot) = candidate.execution else {
                return None;
            };
            let ResolvedLeafOutbound::Proxy { identity } = leaf else {
                return None;
            };
            let outbound = ctx.config().outbounds.get(identity.config_index())?;
            nodes.push(PreparedNativeTcpNode {
                slot,
                tag: outbound.tag.clone(),
                protocol: candidate.protocol,
                endpoint: candidate
                    .endpoint
                    .expect("native node has a validated endpoint"),
                dial: outbound.dial.to_policy(),
            });
        }
        Some(Ok(PreparedNativeTcpRelayChain { nodes }))
    }
}

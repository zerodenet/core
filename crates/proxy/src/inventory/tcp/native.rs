//! Materialize a selected chain of already prepared, neutral Stream candidates.
use super::super::ProtocolInventory;
use super::{PreparedTcpCandidate, PreparedTcpCandidateExecution};
use crate::{component::instance::ComponentSlot, transport::TcpOutboundFailure};
use std::sync::Arc;

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
        candidates: impl IntoIterator<Item = Option<Result<PreparedTcpCandidate, TcpOutboundFailure>>>,
    ) -> Option<Result<PreparedNativeTcpRelayChain, TcpOutboundFailure>> {
        let candidates = candidates.into_iter();
        let mut nodes = Vec::with_capacity(candidates.size_hint().0);
        for (index, candidate) in candidates.enumerate() {
            let candidate = match candidate? {
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
            let dial = slot.dial.clone();
            nodes.push(PreparedNativeTcpNode {
                slot,
                tag: candidate.tag.expect("native node has a configured tag"),
                protocol: candidate.protocol,
                endpoint: candidate
                    .endpoint
                    .expect("native node has a validated endpoint"),
                dial,
            });
        }
        Some(Ok(PreparedNativeTcpRelayChain { nodes }))
    }
}

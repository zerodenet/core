//! Materialize a selected chain of already prepared, neutral Stream candidates.
use super::super::ProtocolInventory;
use super::{PreparedTcpCandidate, PreparedTcpCandidateExecution};
use crate::{component::instance::ComponentSlot, transport::TcpOutboundFailure};
use std::sync::Arc;

/// A prepared or cached candidate cannot keep a retired factory or pool alive.
#[derive(Clone)]
pub(crate) struct StreamRoleRef {
    instance: zero_core::component::ComponentInstanceId,
    slot: std::sync::Weak<ComponentSlot>,
}
impl StreamRoleRef {
    pub(crate) fn new(slot: &Arc<ComponentSlot>) -> Self {
        Self {
            instance: slot.instance_id(),
            slot: Arc::downgrade(slot),
        }
    }
    pub(crate) fn instance(&self) -> zero_core::component::ComponentInstanceId {
        self.instance
    }
    pub(crate) fn slot(
        &self,
    ) -> Result<Arc<ComponentSlot>, zero_platform_tokio::contract::ChannelError> {
        let slot = self
            .slot
            .upgrade()
            .filter(|slot| slot.instance_id() == self.instance)
            .ok_or_else(|| {
                let mut error = crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Revoked,
                    "selected Stream owner is gone",
                );
                error.component = Some(self.instance.component);
                error
            })?;
        Ok(slot)
    }
    pub(crate) fn matches(&self, slot: &Arc<ComponentSlot>) -> bool {
        slot.instance_id() == self.instance && self.slot.ptr_eq(&Arc::downgrade(slot))
    }
}

pub(crate) struct PreparedNativeTcpRelayChain {
    pub(crate) nodes: Vec<PreparedNativeTcpNode>,
}
pub(crate) struct PreparedNativeTcpNode {
    pub(crate) slot: StreamRoleRef,
    pub(crate) tag: String,
    pub(crate) protocol: String,
    pub(crate) endpoint: Option<(String, u16)>,
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
            // Only copy neutral dial facts here. The prepared chain retains
            // weak generation identities; dispatch reacquires all actual owners
            // before it may open its first carrier.
            let dial = match slot.slot() {
                Ok(owner) => owner.dial.clone(),
                Err(source) => {
                    return Some(Err(TcpOutboundFailure {
                        stage: if index == 0 {
                            "outbound_leaf_runtime"
                        } else {
                            "relay_prepare"
                        },
                        error: std::io::Error::other(source).into(),
                        upstream_endpoint: candidate.endpoint.map(Box::new),
                        network: None,
                    }))
                }
            };
            nodes.push(PreparedNativeTcpNode {
                slot,
                tag: candidate.tag.expect("native node has a configured tag"),
                protocol: candidate.protocol,
                endpoint: candidate.endpoint,
                dial,
            });
        }
        Some(Ok(PreparedNativeTcpRelayChain { nodes }))
    }
}

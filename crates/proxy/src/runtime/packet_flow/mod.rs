//! Shared transferred-user and original caller/receiver close ledger for the
//! actual standard device roles. It owns no protocol framing, keys or pools.
// Stand-alone test bridges exercise the stack directly; production roles own
// their actual caller, FlowUser and original provider instead of these adapters.
#[cfg(test)]
#[path = "tests/bridge/contract.rs"]
mod contract;
#[cfg(test)]
#[path = "tests/bridge/tcp.rs"]
mod tcp;
#[cfg(test)]
#[path = "tests/bridge/udp.rs"]
mod udp;
mod users;
pub(crate) use users::{
    FlowLogicalClose, FlowReceiverBinding, FlowUser, FlowUsers, PacketSourceClose,
};

#[cfg(test)]
pub(crate) use contract::{
    PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowEndpoint, PacketFlowSource,
};

#[cfg(test)]
pub(crate) mod tests;

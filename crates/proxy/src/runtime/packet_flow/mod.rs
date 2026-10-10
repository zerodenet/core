//! Candidate-local Flow -> Packet execution over a shared bidirectional IP stack.
//! No protocol framing, keys, sockets or peer selection policy live here.
mod contract;
mod tcp;
mod udp;

pub(crate) use contract::{
    PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowEndpoint, PacketFlowSource,
};

#[cfg(test)]
pub(crate) mod tests;

use std::net::SocketAddr;
use zero_core::component::NetworkGeneration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundDatagramRequest {
    pub peer: SocketAddr,
    pub expected_network_generation: NetworkGeneration,
}

/// The original socket and exact host selection, never a cloned descriptor.
/// Its receiver must install the socket before inspecting whether the selection
/// has become stale, then retire it through that same owner's native close ledger.
#[derive(Debug)]
pub struct GrantedOutboundDatagram {
    pub socket: crate::TokioDatagramSocket,
    pub selection: crate::EgressSelection,
}

//! Cold protocol-neutral facts. No process arguments or network authority.
use zero_core::component::NetworkGeneration;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessEndpointSpec {
    pub stream: bool,
    pub datagram: bool,
    pub ipv6: bool,
    pub network_generation: NetworkGeneration,
}

mod endpoint;
mod inbound;
mod key;
mod network;
mod outbound;

pub use endpoint::{parse_endpoint, Endpoint, EndpointError};
pub use inbound::{
    validate_inbound, InboundInput, InboundPeerInput, InboundValidationError, ValidatedInbound,
    ValidatedInboundPeer,
};
pub use key::{parse_key, public_peer_id, Key, KeyError};
pub use network::{parse_network, IpNetwork, NetworkError};
pub use outbound::{
    validate_outbound, OutboundInput, PeerInput, ValidatedOutbound, ValidatedPeer, ValidationError,
    DEFAULT_MTU, MAX_MTU, MIN_IPV4_MTU, MIN_IPV6_MTU,
};

#[cfg(feature = "runtime")]
pub(crate) use key::validated_public_peer_id;

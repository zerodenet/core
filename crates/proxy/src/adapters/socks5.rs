use ::socks5::transport::{Socks5OutboundOptionsRef, Socks5TransportLeaf};
use zero_config::OutboundProtocolConfig;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;

mod component;
#[cfg(feature = "socks5")]
pub(crate) mod inbound;
pub(crate) use component::{component_options, LEGACY_CONNECT_STAGE};
#[cfg(feature = "socks5")]
pub(crate) mod udp;

#[cfg(feature = "socks5")]
#[derive(Debug)]
pub(crate) struct Socks5Adapter;

fn transport_leaf(tag: &str, protocol: &OutboundProtocolConfig) -> Option<Socks5TransportLeaf> {
    let OutboundProtocolConfig::Socks5 {
        server,
        port,
        username,
        password,
    } = protocol
    else {
        return None;
    };
    Some(Socks5TransportLeaf::from_options_refs(
        tag,
        server,
        *port,
        Socks5OutboundOptionsRef {
            username: username.as_deref(),
            password: password.as_deref(),
        },
    ))
}

#[cfg(feature = "socks5")]
impl NamedProtocolAdapter for Socks5Adapter {
    const PROTOCOL_NAME: &'static str = "socks5";
    const FEATURE_NAME: &'static str = "socks5";
}

#[cfg(feature = "socks5")]
impl ProtocolMetadata for Socks5Adapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::socks5::Socks5Protocol.descriptor()
    }
}

#[cfg(test)]
#[path = "../component/tests/socks.rs"]
mod component_tests;

#[cfg(test)]
#[path = "../component/tests/relay.rs"]
pub(crate) mod relay_tests;

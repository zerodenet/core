use ::socks5::transport::{Socks5OutboundOptionsRef, Socks5TransportLeaf};
use zero_config::OutboundProtocolConfig;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput};
use crate::runtime::path::TcpPathCategory;

mod component;
#[cfg(feature = "socks5")]
pub(crate) mod inbound;
pub(crate) use component::{component_options, LEGACY_CONNECT_STAGE};
#[cfg(feature = "socks5")]
mod tcp;
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
impl Socks5Adapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy { outbound, .. } = input else {
            return None;
        };
        let leaf = transport_leaf(outbound.tag(), &outbound.protocol)?;
        let tcp = std::sync::Arc::new(
            crate::runtime::tcp_dispatch::operation::SocketTcpOperation {
                handshake: leaf.clone(),
            },
        );
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Tunnel,
            tcp: Some(Ok(tcp.clone())),
            tcp_relay: Some(Ok(tcp)),
            udp: None,
            udp_relay: None,
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: self.udp_packet_path_operation(leaf),
        })
    }
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

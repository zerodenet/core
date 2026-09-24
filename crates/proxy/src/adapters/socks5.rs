use ::socks5::transport::{
    Socks5InboundAcceptor, Socks5InboundUserRef, Socks5OutboundOptionsRef, Socks5TransportLeaf,
};
use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig};
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, TcpOutboundCapability,
    UdpFlowCapability, UdpPacketPathCapability, UpstreamUdpHandlerProvider,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::udp_flow::registered::UpstreamAssociationHandler;

#[cfg(feature = "socks5")]
pub(super) mod inbound;
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
        let tcp = self.claim_tcp_outbound_leaf_impl(leaf.clone());
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Tunnel,
            tcp: Some(tcp),
            udp: Some(self.claim_udp_flow_leaf_impl(leaf.clone())),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: self.claim_udp_packet_path_leaf_impl(leaf),
        })
    }
}

#[cfg(feature = "socks5")]
impl UdpPacketPathCapability for Socks5Adapter {}

#[cfg(feature = "socks5")]
impl UdpFlowCapability for Socks5Adapter {}

#[cfg(feature = "socks5")]
impl UpstreamUdpHandlerProvider for Socks5Adapter {
    fn upstream_association_handler(&self) -> Box<dyn UpstreamAssociationHandler> {
        udp::upstream_association_handler()
    }
}

#[cfg(feature = "socks5")]
impl InboundListenerCapability for Socks5Adapter {
    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let acceptor = match &inbound.protocol {
            InboundProtocolConfig::Socks5 { users } => {
                Socks5InboundAcceptor::from_options_refs(users.iter().map(|user| {
                    Socks5InboundUserRef {
                        username: user.username.as_str(),
                        password: user.password.as_str(),
                        principal_key: user.principal_key.as_deref(),
                        up_bps: user.up_bps,
                        down_bps: user.down_bps,
                    }
                }))
            }
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "socks5 inbound listener received non-socks5 inbound config",
                )));
            }
        };
        Ok(inbound::prepare(acceptor))
    }
}

#[cfg(feature = "socks5")]
impl TcpOutboundCapability for Socks5Adapter {}

#[cfg(feature = "socks5")]
impl ProtocolMetadata for Socks5Adapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::socks5::Socks5Protocol.descriptor()
    }
}

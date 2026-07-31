use ::mieru::transport::{
    MieruInboundListenerRequest, MieruInboundUserRef, MieruOutboundOptionsRef, MieruTransportLeaf,
};

use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig};
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    InboundListenerCapability, ManagedUdpHandlerProvider, OutboundLeafClaim, OutboundLeafInput,
    TcpOutboundCapability, UdpFlowCapability, UdpPacketPathCapability,
};
use crate::runtime::path::TcpPathCategory;
use crate::runtime::udp_flow::managed::ManagedStreamHandlerPair;

#[cfg(feature = "mieru")]
mod inbound;
#[cfg(feature = "mieru")]
mod tcp;
#[cfg(feature = "mieru")]
pub(crate) mod udp;

#[cfg(feature = "mieru")]
#[derive(Debug)]
pub(crate) struct MieruAdapter;

fn transport_leaf(tag: &str, protocol: &OutboundProtocolConfig) -> Option<MieruTransportLeaf> {
    let OutboundProtocolConfig::Mieru {
        server,
        port,
        username,
        password,
    } = protocol
    else {
        return None;
    };
    Some(MieruTransportLeaf::from_options_refs(
        tag,
        server,
        *port,
        MieruOutboundOptionsRef {
            username: username.as_deref().unwrap_or(password),
            password,
        },
    ))
}

#[cfg(feature = "mieru")]
impl NamedProtocolAdapter for MieruAdapter {
    const PROTOCOL_NAME: &'static str = "mieru";
    const FEATURE_NAME: &'static str = "mieru";
}

#[cfg(feature = "mieru")]
impl MieruAdapter {
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
            tcp_path: TcpPathCategory::Session,
            tcp,
            udp: Some(self.claim_udp_flow_leaf_impl(leaf)),
            packet_path: None,
        })
    }
}

#[cfg(feature = "mieru")]
impl UdpFlowCapability for MieruAdapter {}

#[cfg(feature = "mieru")]
impl ManagedUdpHandlerProvider for MieruAdapter {
    fn managed_stream_udp_handlers(&self) -> Option<ManagedStreamHandlerPair> {
        Some(udp::managed_stream_handler())
    }
}

#[cfg(feature = "mieru")]
impl UdpPacketPathCapability for MieruAdapter {}

#[cfg(feature = "mieru")]
impl InboundListenerCapability for MieruAdapter {
    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        let profile = match &inbound.protocol {
            InboundProtocolConfig::Mieru { users } => {
                MieruInboundListenerRequest::from_options_refs(users.iter().map(|user| {
                    MieruInboundUserRef {
                        username: user.username.as_str(),
                        password: user.password.as_str(),
                        principal_key: user.principal_key.as_deref(),
                    }
                }))
            }
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "mieru inbound listener received non-mieru inbound config",
                )));
            }
        };
        Ok(inbound::prepare(profile))
    }
}

#[cfg(feature = "mieru")]
impl TcpOutboundCapability for MieruAdapter {}

#[cfg(feature = "mieru")]
impl ProtocolMetadata for MieruAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::mieru::MieruProtocol.descriptor()
    }
}

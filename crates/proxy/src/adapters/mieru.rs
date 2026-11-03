use ::mieru::transport::{MieruOutboundOptionsRef, MieruTransportLeaf};

use zero_config::OutboundProtocolConfig;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput};
use crate::runtime::path::TcpPathCategory;

pub(crate) mod component;
#[cfg(feature = "mieru")]
pub(crate) mod inbound;
#[cfg(feature = "mieru")]
mod tcp;
#[cfg(feature = "mieru")]
pub(crate) mod udp;

#[cfg(feature = "mieru")]
#[derive(Debug, Default)]
pub(crate) struct MieruAdapter {
    pool: std::sync::Arc<::mieru::client::ClientPool>,
}

fn transport_leaf(tag: &str, protocol: &OutboundProtocolConfig) -> Option<MieruTransportLeaf> {
    let OutboundProtocolConfig::Mieru {
        server,
        port,
        username,
        password,
        transport,
        options,
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
            options,
            udp: matches!(transport, zero_config::MieruTransport::Udp),
        },
    ))
}

#[cfg(feature = "mieru")]
impl NamedProtocolAdapter for MieruAdapter {
    const PROTOCOL_NAME: &'static str = "mieru";
    const FEATURE_NAME: &'static str = "mieru";
    fn on_config_reloaded(&self, _config: &zero_config::RuntimeConfig) {
        self.pool.clear_legacy();
    }
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
        let leaf = transport_leaf(outbound.tag(), &outbound.protocol)?.with_pool(self.pool.clone());
        let tcp = std::sync::Arc::new(
            crate::runtime::tcp_dispatch::operation::SocketTcpOperation {
                handshake: leaf.clone(),
            },
        );
        let (udp, udp_relay) = self.udp_operations(leaf);
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Session,
            tcp: Some(Ok(tcp.clone())),
            tcp_relay: Some(Ok(tcp)),
            udp: Some(udp),
            udp_relay: Some(udp_relay),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: None,
        })
    }
}

#[cfg(feature = "mieru")]
impl ProtocolMetadata for MieruAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::mieru::MieruProtocol.descriptor()
    }
}

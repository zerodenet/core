use zero_config::InboundConfig;
use zero_engine::EngineError;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{InboundListenerCapability, TcpOutboundCapability};

#[cfg(feature = "http")]
pub(super) mod inbound;

#[cfg(feature = "http")]
#[derive(Debug)]
pub(crate) struct HttpConnectAdapter;

#[cfg(feature = "http")]
impl NamedProtocolAdapter for HttpConnectAdapter {
    const PROTOCOL_NAME: &'static str = "http";
    const FEATURE_NAME: &'static str = "http";
    const HAS_OUTBOUND: bool = false;
}

#[cfg(feature = "http")]
impl InboundListenerCapability for HttpConnectAdapter {
    fn prepare_inbound_bind(
        &self,
        inbound: InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Option<crate::protocol_registry::PreparedInboundBind>, EngineError> {
        crate::protocol_registry::prepare_tcp_inbound_bind(self, inbound, source_dir)
    }

    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        self.prepare_inbound_listener_impl(inbound)
    }
}

#[cfg(feature = "http")]
impl TcpOutboundCapability for HttpConnectAdapter {}

#[cfg(feature = "http")]
impl ProtocolMetadata for HttpConnectAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::http::HttpConnectProtocol.descriptor()
    }
}

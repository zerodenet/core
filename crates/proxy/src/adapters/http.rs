use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;

#[cfg(feature = "http")]
pub(super) mod inbound;
#[cfg(feature = "http")]
pub(crate) use inbound::component_options_in_runtime;
#[cfg(all(test, feature = "http"))]
pub(crate) use inbound::component_options;

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
impl ProtocolMetadata for HttpConnectAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::http::HttpConnectProtocol.descriptor()
    }
}

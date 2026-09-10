use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use zero_engine::EngineError;

use crate::protocol_registry::UdpNetworkServices;
use crate::runtime::udp_flow::packet_path::{
    PacketPathCarrier, PacketPathCarrierDescriptor, UdpDatagramSource,
};

pub(crate) type PacketPathCarrierFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Arc<dyn PacketPathCarrier>, EngineError>> + Send + 'a>>;

#[derive(Clone)]
pub(crate) struct PreparedDatagramRelayCarrier {
    descriptor: PacketPathCarrierDescriptor,
    operation: Arc<dyn PreparedUdpPacketPathOperation>,
}

impl PreparedDatagramRelayCarrier {
    pub(crate) fn new(operation: Box<dyn PreparedUdpPacketPathOperation>) -> Option<Self> {
        let descriptor = operation.carrier_descriptor()?;
        Some(Self {
            descriptor,
            operation: Arc::from(operation),
        })
    }

    pub(crate) fn identity(&self) -> &str {
        &self.descriptor.cache_key
    }

    pub(crate) async fn build(
        &self,
        services: UdpNetworkServices,
    ) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
        self.operation.build_carrier(services).await
    }
}

impl std::fmt::Debug for PreparedDatagramRelayCarrier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedDatagramRelayCarrier")
            .field("identity", &self.descriptor.cache_key)
            .field("server", &self.descriptor.server)
            .field("port", &self.descriptor.port)
            .finish_non_exhaustive()
    }
}

pub(crate) trait PreparedUdpPacketPathOperation: Send + Sync + 'static {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        None
    }

    fn datagram_source(&self) -> Option<UdpDatagramSource> {
        None
    }

    fn build_carrier<'a>(&'a self, _services: UdpNetworkServices) -> PacketPathCarrierFuture<'a> {
        Box::pin(async {
            Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "UDP packet-path carrier build is unsupported",
            )))
        })
    }
}

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use zero_engine::EngineError;

use crate::inventory::PreparedTcpRelayPrefix;
use crate::protocol_registry::PacketPathExecutionServices;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_flow::packet_path::{
    PacketPathCarrier, PacketPathCarrierDescriptor, UdpDatagramSource,
};

pub(crate) type PacketPathCarrierFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Arc<dyn PacketPathCarrier>, EngineError>> + Send + 'a>>;

#[derive(Clone)]
enum PreparedDatagramRelayLayer {
    Base(Arc<dyn PreparedUdpPacketPathOperation>),
    Codec(UdpDatagramSource),
    Associated {
        operation: Arc<dyn PreparedUdpPacketPathOperation>,
        tcp_prefix: PreparedTcpRelayPrefix,
    },
}

#[derive(Clone)]
pub(crate) struct PreparedDatagramRelayCarrier {
    descriptor: PacketPathCarrierDescriptor,
    layers: Vec<PreparedDatagramRelayLayer>,
}

impl PreparedDatagramRelayCarrier {
    pub(crate) fn new(operation: Box<dyn PreparedUdpPacketPathOperation>) -> Option<Self> {
        let descriptor = operation.carrier_descriptor()?;
        Some(Self {
            descriptor,
            layers: vec![PreparedDatagramRelayLayer::Base(Arc::from(operation))],
        })
    }

    pub(crate) fn identity(&self) -> &str {
        &self.descriptor.cache_key
    }

    pub(crate) fn extend(
        current: Option<Self>,
        operation: Box<dyn PreparedUdpPacketPathOperation>,
        tcp_prefix: PreparedTcpRelayPrefix,
    ) -> Option<Self> {
        if let Some(source) = operation.datagram_source() {
            let mut current = current?;
            let next = source.descriptor();
            current.descriptor = chained_descriptor(&current.descriptor, next);
            current
                .layers
                .push(PreparedDatagramRelayLayer::Codec(source));
            return Some(current);
        }
        if !operation.supports_associated_relay_carrier() {
            return None;
        }

        let next = operation.carrier_descriptor()?;
        let prefix_key = tcp_prefix_key(&tcp_prefix);
        let mut layers = current.map_or_else(Vec::new, |current| current.layers);
        layers.push(PreparedDatagramRelayLayer::Associated {
            operation: Arc::from(operation),
            tcp_prefix,
        });
        Some(Self {
            descriptor: PacketPathCarrierDescriptor {
                cache_key: format!(
                    "associated|{}:{prefix_key}|{}:{}",
                    prefix_key.len(),
                    next.cache_key.len(),
                    next.cache_key
                ),
                server: next.server,
                port: next.port,
            },
            layers,
        })
    }

    pub(crate) async fn build(
        &self,
        services: PacketPathExecutionServices,
    ) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
        let start = self
            .layers
            .iter()
            .rposition(|layer| !matches!(layer, PreparedDatagramRelayLayer::Codec(_)))
            .expect("prepared datagram relay carrier has a build layer");
        let mut path = match &self.layers[start] {
            PreparedDatagramRelayLayer::Base(operation) => {
                operation.build_carrier(services.clone()).await?
            }
            PreparedDatagramRelayLayer::Associated {
                operation,
                tcp_prefix,
            } => {
                let carrier = services.prepare_lazy_tcp_relay_prefix(tcp_prefix.clone());
                operation
                    .build_associated_relay_carrier(services.clone(), carrier)
                    .await?
            }
            PreparedDatagramRelayLayer::Codec(_) => unreachable!(),
        };
        for layer in &self.layers[start + 1..] {
            let PreparedDatagramRelayLayer::Codec(source) = layer else {
                unreachable!("last associated packet-path layer must be the build root")
            };
            path = crate::runtime::udp_flow::packet_path_chain::carriers::encoded::wrap(
                path,
                source.clone(),
            );
        }
        Ok(path)
    }
}

impl PreparedUdpPacketPathOperation for PreparedDatagramRelayCarrier {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        Some(self.descriptor.clone())
    }
    fn build_carrier<'a>(
        &'a self,
        services: PacketPathExecutionServices,
    ) -> PacketPathCarrierFuture<'a> {
        Box::pin(self.build(services))
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

    fn supports_associated_relay_carrier(&self) -> bool {
        false
    }

    fn build_carrier<'a>(
        &'a self,
        _services: PacketPathExecutionServices,
    ) -> PacketPathCarrierFuture<'a> {
        Box::pin(async { Err(unsupported("UDP packet-path carrier build is unsupported")) })
    }

    fn build_associated_relay_carrier<'a>(
        &'a self,
        _services: PacketPathExecutionServices,
        _carrier: LazyTcpRelayCarrier<'a>,
    ) -> PacketPathCarrierFuture<'a> {
        Box::pin(async {
            Err(unsupported(
                "associated UDP packet-path carrier build is unsupported",
            ))
        })
    }
}

fn chained_descriptor(
    prefix: &PacketPathCarrierDescriptor,
    next: &crate::runtime::udp_flow::packet_path::UdpDatagramDescriptor,
) -> PacketPathCarrierDescriptor {
    PacketPathCarrierDescriptor {
        cache_key: format!(
            "chain|{}:{}|{}:{}|{}:{}:{}|{}:{}",
            prefix.cache_key.len(),
            prefix.cache_key,
            next.cache_key.len(),
            next.cache_key,
            next.server.len(),
            next.server,
            next.port,
            next.tag.len(),
            next.tag
        ),
        server: next.server.clone(),
        port: next.port,
    }
}

fn tcp_prefix_key(prefix: &PreparedTcpRelayPrefix) -> String {
    let mut key = format!("{}:{}", prefix.first.protocol.len(), prefix.first.protocol);
    if let Some(tag) = &prefix.first.tag {
        key.push_str(&format!("|{}:{tag}", tag.len()));
    }
    if let Some((server, port)) = &prefix.first.endpoint {
        key.push_str(&format!("|{}:{server}:{port}", server.len()));
    }
    for hop in &prefix.relay_hops {
        key.push_str(&format!(
            "|{}:{}|{}:{}|{}:{}:{}",
            hop.tag.len(),
            hop.tag,
            hop.protocol.len(),
            hop.protocol,
            hop.server.len(),
            hop.server,
            hop.port
        ));
    }
    if let Some(Some(datagram)) = prefix.datagram_prefixes.last() {
        key.push_str(&format!(
            "|packet:{}:{}",
            datagram.identity().len(),
            datagram.identity()
        ));
    }
    key.push_str(&format!(
        "|{}:{}|{}:{}|{}:{}:{}",
        prefix.final_tag.len(),
        prefix.final_tag,
        prefix.final_protocol.len(),
        prefix.final_protocol,
        prefix.final_server.len(),
        prefix.final_server,
        prefix.final_port
    ));
    key
}

fn unsupported(message: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        message,
    ))
}

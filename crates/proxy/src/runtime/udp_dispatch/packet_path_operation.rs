mod build;
use build::observed_services;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
    )
)]
mod owned;
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
    /// Binding of the actual last build root, independent of trailing codecs.
    /// In particular, an Associated root uses its final dial and ordered exact
    /// prefix roles, not the original group's first-hop empty binding.
    pub(crate) fn source_binding(
        &self,
        plan: &crate::component::catalog::ComponentPlan,
        network_generation: zero_core::component::NetworkGeneration,
    ) -> Result<zero_platform_tokio::contract::network::UpstreamBinding, EngineError> {
        let root = self
            .layers
            .iter()
            .rfind(|layer| !matches!(layer, PreparedDatagramRelayLayer::Codec(_)))
            .ok_or_else(|| unsupported("prepared Source carrier has no actual build root"))?;
        match root {
            PreparedDatagramRelayLayer::Associated {
                operation,
                tcp_prefix,
            } => {
                let descriptor = operation
                    .carrier_descriptor()
                    .ok_or_else(|| unsupported("Source associated root has no descriptor"))?;
                crate::component::stream_bridge::datagram::packet_path::relay::associated_binding(
                    plan,
                    tcp_prefix,
                    &descriptor,
                    network_generation,
                )
            }
            PreparedDatagramRelayLayer::Base(operation) => {
                let descriptor = operation
                    .carrier_descriptor()
                    .ok_or_else(|| unsupported("Source base root has no descriptor"))?;
                let tag = descriptor
                    .tag
                    .as_deref()
                    .ok_or_else(|| unsupported("Source base root has no actual configured tag"))?;
                let slot = plan.configured(tag).ok_or_else(|| {
                    unsupported("Source base root owner is absent from its actual plan")
                })?;
                Ok(zero_platform_tokio::contract::network::UpstreamBinding {
                    prefix: Vec::new(),
                    dial: slot.dial.clone(),
                    network_generation,
                })
            }
            PreparedDatagramRelayLayer::Codec(_) => unreachable!("actual build root"),
        }
    }

    pub(crate) fn retained_admission(&self) -> Result<(), EngineError> {
        for layer in &self.layers {
            match layer {
                PreparedDatagramRelayLayer::Base(operation) => operation.retained_admission()?,
                PreparedDatagramRelayLayer::Associated {
                    operation,
                    tcp_prefix,
                } => {
                    operation.retained_admission()?;
                    crate::runtime::tcp_dispatch::relay::retained::check_owners(tcp_prefix)?;
                }
                PreparedDatagramRelayLayer::Codec(_) => {
                    return Err(unsupported("retained datagram codec owner is not migrated"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn new(operation: Arc<dyn PreparedUdpPacketPathOperation>) -> Option<Self> {
        let descriptor = operation.carrier_descriptor()?;
        Some(Self {
            descriptor,
            layers: vec![PreparedDatagramRelayLayer::Base(operation)],
        })
    }

    pub(crate) fn identity(&self) -> &str {
        &self.descriptor.cache_key
    }

    pub(crate) fn extend(
        current: Option<Self>,
        operation: Arc<dyn PreparedUdpPacketPathOperation>,
        tcp_prefix: PreparedTcpRelayPrefix,
    ) -> Option<Self> {
        let source = operation.datagram_source();
        Self::extend_with_source(current, operation, tcp_prefix, source)
    }

    /// Consume the exact Source cold-pass codec. Never call datagram_source a
    /// second time: that would create another actual sender/session family.
    pub(crate) fn extend_with_source(
        current: Option<Self>,
        operation: Arc<dyn PreparedUdpPacketPathOperation>,
        tcp_prefix: PreparedTcpRelayPrefix,
        source: Option<UdpDatagramSource>,
    ) -> Option<Self> {
        if let Some(source) = source {
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
        let prefix_key = owned::tcp_prefix_key(&tcp_prefix);
        let mut layers = current.map_or_else(Vec::new, |current| current.layers);
        layers.push(PreparedDatagramRelayLayer::Associated {
            operation,
            tcp_prefix,
        });
        Some(Self {
            descriptor: PacketPathCarrierDescriptor {
                tag: next.tag.clone(),
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
}

impl PreparedUdpPacketPathOperation for PreparedDatagramRelayCarrier {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        Some(self.descriptor.clone())
    }
    fn build_owned_carrier<'a>(
        &'a self,
        services: PacketPathExecutionServices,
        owner: Arc<super::packet_owner::PacketCarrierOwnerSlot>,
        demand: super::packet_owner::PacketCarrierDemand,
    ) -> PacketPathCarrierFuture<'a> {
        Box::pin(owned::build(self, services, owner, demand))
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
    fn retained_admission(&self) -> Result<(), EngineError> {
        Err(unsupported("retained datagram owner is not migrated"))
    }

    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        None
    }

    fn datagram_source(&self) -> Option<UdpDatagramSource> {
        None
    }

    fn supports_associated_relay_carrier(&self) -> bool {
        false
    }

    /// Explicit opt-in: an unknown legacy carrier never acquires a grant by
    /// falling back to its unowned builder.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    fn build_owned_carrier<'a>(
        &'a self,
        _services: PacketPathExecutionServices,
        _owner: Arc<super::packet_owner::PacketCarrierOwnerSlot>,
        _demand: super::packet_owner::PacketCarrierDemand,
    ) -> PacketPathCarrierFuture<'a> {
        Box::pin(async { Err(unsupported("owned UDP packet-path carrier is not migrated")) })
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
        tag: Some(next.tag.clone()),
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

pub(crate) fn unsupported(message: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        message,
    ))
}

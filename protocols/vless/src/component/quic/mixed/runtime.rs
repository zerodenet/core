//! Real same-Slot halves: one registrar/Host book and one shared Entry table.
mod lifecycle;
use super::super::*;
use super::options::{Half, TcpOptions};
use super::resources::BoundHalf;
use zero_platform_tokio::{
    contract::{
        component::BuildContext, lifecycle::ResourceLifecycle, network::UpstreamTransport,
        CancellationToken,
    },
    controlled_carrier::{CarrierGrants, CarrierOwner},
    controlled_datagram::metadata_pool::MetadataPoolOwner,
};
use zero_traits::packet_metadata::PacketMetadataArc;
pub(super) struct TcpGeneration {
    pub instance: zero_core::component::ComponentInstanceId,
    pub options: Arc<TcpOptions>,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
    pub upstream: UpstreamTransport,
    pub carrier: CarrierGrants,
    pub cancel: CancellationToken,
    pub cleanup: CancellationToken,
    pub buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    pub masks: zero_transport::finalmask::Profile,
    pub memory: PacketDriverMetadataView,
    pub entries: PacketMetadataArc<Mutex<entry::Table>>,
    pub resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
}
pub(super) enum Runtime {
    Quic {
        generation: PacketMetadataArc<entry::Generation>,
        carrier: CarrierOwner,
        metadata_ordinal: usize,
        carrier_ordinal: usize,
        failures: super::super::factory::failures::OriginalSources,
    },
    Tcp {
        generation: PacketMetadataArc<TcpGeneration>,
        carrier: CarrierOwner,
        carrier_ordinal: usize,
        failures: super::super::factory::failures::OriginalSources,
    },
}
impl Runtime {
    pub fn build(
        half: &Half,
        binding: BoundHalf,
        context: &BuildContext,
        upstream: UpstreamTransport,
        entries: &PacketMetadataArc<Mutex<entry::Table>>,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<Self, ChannelError> {
        let buffers = context
            .resources
            .carrier_buffer_budget()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        let memory = context.resources.packet_driver_metadata()?;
        let instance = context.instance();
        let failures = super::super::factory::failures::OriginalSources::prepare(&memory)?;
        // A cached original's physical cleanup token is independent of its
        // first logical caller. BoundAuthority still fences Host/related IO.
        let cleanup = CancellationToken::new();
        match (half, binding) {
            (
                Half::Quic(options),
                BoundHalf::Quic {
                    metadata,
                    carrier: carrier_ordinal,
                },
            ) => {
                if buffers.limits() != options.buffers {
                    return Err(error(Stage::Prepare, Reason::StalePlan));
                }
                let masks = options
                    .prepare_masks(buffers.clone(), memory.clone())
                    .map_err(capacity)?;
                let carrier = CarrierOwner::new(instance, options.origins, &cleanup);
                let metadata_owner =
                    MetadataPoolOwner::new(instance, options.origins, options.limits, &cleanup)?;
                let paid = memory
                    .reserve(PacketMetadataArc::<entry::Generation>::layout().map_err(capacity)?)
                    .map_err(capacity)?;
                let generation = PacketMetadataArc::from_prepaid(
                    entry::Generation {
                        instance,
                        options: options.clone(),
                        resources: context.resources.clone(),
                        tasks: context.tasks.clone(),
                        upstream,
                        metadata: metadata_owner,
                        carrier: carrier.grants(),
                        cancel: context.cancel.child_token(),
                        cleanup,
                        buffers,
                        masks,
                        metadata_memory: memory,
                        resolver,
                        entries: entries.clone(),
                    },
                    paid,
                )
                .map_err(|(source, _, _)| capacity(source))?;
                Ok(Self::Quic {
                    generation,
                    carrier,
                    metadata_ordinal: metadata,
                    carrier_ordinal,
                    failures,
                })
            }
            (
                Half::Tcp(options),
                BoundHalf::Tcp {
                    carrier: carrier_ordinal,
                },
            ) => {
                if buffers.limits() != options.buffers {
                    return Err(error(Stage::Prepare, Reason::StalePlan));
                }
                let masks = options
                    .prepare_masks(buffers.clone(), memory.clone())
                    .map_err(capacity)?;
                let carrier = CarrierOwner::new(instance, options.origins, &cleanup);
                let paid = memory
                    .reserve(PacketMetadataArc::<TcpGeneration>::layout().map_err(capacity)?)
                    .map_err(capacity)?;
                let generation = PacketMetadataArc::from_prepaid(
                    TcpGeneration {
                        instance,
                        options: options.clone(),
                        resources: context.resources.clone(),
                        tasks: context.tasks.clone(),
                        upstream,
                        carrier: carrier.grants(),
                        cancel: context.cancel.child_token(),
                        cleanup,
                        buffers,
                        masks,
                        memory,
                        resolver,
                        entries: entries.clone(),
                    },
                    paid,
                )
                .map_err(|(source, _, _)| capacity(source))?;
                Ok(Self::Tcp {
                    generation,
                    carrier,
                    carrier_ordinal,
                    failures,
                })
            }
            _ => Err(error(Stage::Prepare, Reason::StalePlan)),
        }
    }
    pub fn material(&self) -> Option<&Arc<crate::component::material::Material>> {
        match self {
            Self::Quic { generation, .. } => generation.options.protocol.material.as_ref(),
            Self::Tcp { generation, .. } => generation.options.protocol.material.as_ref(),
        }
    }
    /// Called only after BOTH halves register. Identical actual material Arc
    /// is started once by the outer owner; different recipes use direct handles.
    pub async fn start_material(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        let Some(material) = self.material() else {
            return Ok(());
        };
        match self {
            Self::Quic { generation, .. } => {
                material.record_bound(generation.instance, generation.resolver.as_ref())?;
                material
                    .start(
                        generation.resources.as_ref(),
                        generation.instance,
                        &generation.cancel,
                        deadline,
                    )
                    .await
            }
            Self::Tcp { generation, .. } => {
                material.record_bound(generation.instance, generation.resolver.as_ref())?;
                material
                    .start(
                        generation.resources.as_ref(),
                        generation.instance,
                        &generation.cancel,
                        deadline,
                    )
                    .await
            }
        }
    }
    /// Install every actual registration before any fallible material stage.
    /// The outer owner does this for BOTH halves before starting either one.
    pub fn register(&self) -> Result<(), ChannelError> {
        match self {
            Self::Quic {
                generation,
                carrier,
                metadata_ordinal,
                carrier_ordinal,
                ..
            } => {
                generation.resources.register_metadata_pool(
                    *metadata_ordinal,
                    generation.metadata.registration(),
                    &generation.tasks,
                )?;
                generation
                    .resources
                    .register_controlled_carrier(
                        *carrier_ordinal,
                        carrier.registration(),
                        &generation.tasks,
                    )
                    .map(|_| ())
            }
            Self::Tcp {
                generation,
                carrier,
                carrier_ordinal,
                ..
            } => generation
                .resources
                .register_controlled_carrier(
                    *carrier_ordinal,
                    carrier.registration(),
                    &generation.tasks,
                )
                .map(|_| ()),
        }
    }
    pub async fn start_originals(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        match self {
            Self::Quic {
                generation,
                carrier,
                ..
            } => {
                generation.metadata.start()?;
                carrier.start(deadline).await
            }
            Self::Tcp { carrier, .. } => carrier.start(deadline).await,
        }
    }
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}

impl TcpGeneration {
    pub fn next_entry(&self, previous: u64) -> Option<(u64, PacketMetadataArc<entry::Entry>)> {
        entry::Table::next_entry(&self.entries, previous)
    }
}

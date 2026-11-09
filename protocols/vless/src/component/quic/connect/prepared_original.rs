//! Same original reservation/observer publication, separated from starting IO.
//! Mixed halves install BOTH actual Root controls before starting either Aux.
use super::*;
use crate::component::quic::entry::Position;
use zero_platform_tokio::{
    controlled_carrier::CarrierGrant, controlled_datagram::metadata_pool::MetadataPoolPreparation,
};
enum Kind {
    Native(MetadataPoolPreparation),
    Borrowed(CarrierGrant),
}
pub(in crate::component::quic) struct PreparedOriginal {
    pub(super) generation: PacketMetadataArc<Generation>,
    entry: PacketMetadataArc<Entry>,
    pub(super) demand: PacketMetadataArc<demand::Demand>,
    kind: Kind,
    before: before::BeforeActor,
}
impl PreparedOriginal {
    pub(in crate::component::quic) async fn prepare(
        generation: &PacketMetadataArc<Generation>,
        position: Position,
        context: &OpenContext,
    ) -> Result<Self, ChannelError> {
        let demand = demand::Demand::new(generation, context).await?;
        Self::new(generation, position, demand, context).await
    }

    pub(super) async fn new(
        generation: &PacketMetadataArc<Generation>,
        position: Position,
        demand: PacketMetadataArc<demand::Demand>,
        context: &OpenContext,
    ) -> Result<Self, ChannelError> {
        let drivers = generation.options.drivers();
        // No original/control/driver is created before its actual Host memory
        // promises. Refusal leaves no first native IO and no detached worker.
        let owners_header = generation.memory(
            PacketMetadataArc::<Owners>::layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let diagnostics = entry::diagnostics::Diagnostics::prepare(&generation.metadata_memory)?;
        let received_memory = generation.memory(
            PacketMetadataLayout::array::<zero_transport::quic::owned::DriverDelivery>(drivers)
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let logical_memory =
            generation.memory(PacketMetadataLayout::boxed::<entry::LogicalControl>())?;
        let owners = PacketMetadataArc::from_prepaid(
            Owners::new(
                generation.tasks.clone(),
                diagnostics,
                received_memory,
                logical_memory,
                drivers,
            ),
            owners_header,
        )
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        if generation.options.h3.is_some() {
            let _ = owners.protocols.set(
                zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren::new(
                    generation.metadata_memory.clone(),
                    generation.options.origins.get(),
                ),
            );
            owners.state.lock().await.h3 = Some(
                zero_transport::split_http::PreparedXhttp3::new(
                    generation.buffers.clone(),
                    generation.metadata_memory.clone(),
                    generation.options.users,
                )
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
            );
        }
        let control_memory = generation.memory(PacketMetadataLayout::boxed::<ProtocolControl>())?;
        let control =
            BoxChannelControl::from_prepaid(ProtocolControl(owners.clone()), control_memory)
                .map_err(|(source, _, _)| {
                    error(Stage::Prepare, Reason::Capacity).with_source(source)
                })?;
        let call = demand.call.clone();
        let upstream = demand.upstream.clone();
        let binding = upstream.binding().clone();

        let (entry, kind, before) = if binding.prefix.is_empty() {
            let (preparation, retirement) = generation
                .metadata
                .grants()
                .prepare_protocol_original(&generation.tasks, control)
                .map_err(|rejected| {
                    owners.set_actor(None);
                    rejected.error
                })?;
            let origin = CarrierOrigin::from_metadata(preparation.origin());
            let entry = generation
                .make_entry(owners.clone(), retirement.clone(), Some(origin))
                .map_err(|source| {
                    owners.set_actor(None);
                    retirement.request_retirement();
                    source
                })?;
            let entry = position.publish(entry);
            let before = before::BeforeActor(Some(entry.clone()));
            if let Some(observer) = context.upstream.protocol_original_observer() {
                if let Err(source) = preparation
                    .protocol_original(
                        &context.upstream,
                        observer.clone(),
                        &generation.options.protocol.endpoint,
                        &retirement,
                    )
                    .and_then(|original| observer.retain_protocol_original(original))
                {
                    owners.set_actor(None);
                    retirement.request_retirement();
                    return Err(source);
                }
            }
            (entry, Kind::Native(preparation), before)
        } else {
            let (grant, retirement) = generation
                .carrier
                .reserve_protocol_original(&generation.tasks, control)
                .map_err(|rejected| {
                    owners.set_actor(None);
                    rejected.error
                })?;
            let entry = generation
                .make_entry(owners.clone(), retirement.clone(), None)
                .map_err(|source| {
                    owners.set_actor(None);
                    retirement.request_retirement();
                    source
                })?;
            let entry = position.publish(entry);
            let before = before::BeforeActor(Some(entry.clone()));
            if let Some(observer) = context.upstream.protocol_original_observer() {
                if let Err(source) = grant
                    .protocol_original(
                        &context.upstream,
                        observer.clone(),
                        &generation.options.protocol.endpoint,
                        &call,
                        &generation.tasks,
                        &retirement,
                    )
                    .and_then(|original| observer.retain_protocol_original(original))
                {
                    owners.set_actor(None);
                    retirement.request_retirement();
                    return Err(source);
                }
            }
            (entry, Kind::Borrowed(grant), before)
        };
        Ok(Self {
            generation: generation.clone(),
            entry,
            demand,
            kind,
            before,
        })
    }
    pub(super) fn start(
        self,
        index: usize,
    ) -> Result<
        (
            PacketMetadataArc<Entry>,
            oneshot::Receiver<Result<Ready, ChannelError>>,
        ),
        ChannelError,
    > {
        let Self {
            generation,
            entry,
            demand,
            kind,
            mut before,
        } = self;
        let (ready, receiver) = oneshot::channel();
        let upstream = demand.upstream.clone();
        let call = demand.call.clone();
        // The existing per-original RegistrationGuard now owns any synchronous
        // uncertain registration. No synthetic task or caller ACK is created.
        before.disarm();
        match kind {
            Kind::Native(preparation) => native::spawn(
                generation,
                entry.clone(),
                preparation,
                upstream,
                call,
                demand,
                index,
                ready,
            )?,
            Kind::Borrowed(grant) => borrowed::spawn(
                generation,
                entry.clone(),
                grant,
                upstream,
                call,
                demand,
                index,
                ready,
            )?,
        }
        Ok((entry, receiver))
    }
}

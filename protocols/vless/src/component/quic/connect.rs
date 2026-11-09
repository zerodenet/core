//! Real original reservation, actual establishing Aux, then first native IO.
use super::entry::{Entry, Generation, Owners, ProtocolControl};
use super::*;
use tokio::sync::oneshot;
use zero_platform_tokio::{
    contract::{
        context::{with_deadline, OpenContext},
        lifecycle::BoxChannelControl,
        network::{CarrierContext, UpstreamTransport},
    },
    controlled_carrier::{CarrierOrigin, CarrierProtocolRetirement},
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};
mod before;
mod borrowed;
mod demand;
mod initialized;
mod native;
mod ready;
pub(super) use initialized::{Initialized, Pending, Ready};
impl Generation {
    pub async fn acquire(
        generation: &PacketMetadataArc<Self>,
        context: &OpenContext,
    ) -> Result<Initialized, ChannelError> {
        if context.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        let demand = demand::Demand::new(generation, context).await?;
        let mut index = 0;
        loop {
            let position = match Generation::reserve(generation) {
                Ok(position) => position,
                Err(source)
                    if source.reason == Reason::Capacity && generation.options.h3.is_some() =>
                {
                    let mut after = 0;
                    let mut closing = None;
                    while let Some((ordinal, entry)) = generation.next_entry(after) {
                        after = ordinal;
                        if entry.retirement.retirement_requested() && !entry.retirement.is_settled()
                        {
                            closing = Some(entry);
                            break;
                        }
                    }
                    let Some(entry) = closing else {
                        return Err(source);
                    };
                    with_deadline(context.deadline, entry.retirement.wait_settled_original())
                        .await
                        .map_err(|source| {
                            error(Stage::Close, Reason::Timeout).with_source(source)
                        })??;
                    generation.reap();
                    continue;
                }
                Err(source) => return Err(source),
            };
            let drivers = generation.options.drivers();
            // No original/control/driver is created before its actual Host memory
            // promises. Refusal leaves no first native IO and no detached worker.
            let owners_header =
                generation.memory(PacketMetadataArc::<Owners>::layout().map_err(|source| {
                    error(Stage::Prepare, Reason::Capacity).with_source(source)
                })?)?;
            let diagnostics =
                entry::diagnostics::Diagnostics::prepare(&generation.metadata_memory)?;
            let received_memory = generation.memory(
                PacketMetadataLayout::array::<zero_transport::quic::owned::DriverDelivery>(drivers)
                    .map_err(|source| {
                        error(Stage::Prepare, Reason::Capacity).with_source(source)
                    })?,
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
            .map_err(|(source, _, _)| {
                error(Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
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
                    .map_err(|source| {
                        error(Stage::Prepare, Reason::Capacity).with_source(source)
                    })?,
                );
            }
            let control_memory =
                generation.memory(PacketMetadataLayout::boxed::<ProtocolControl>())?;
            let control =
                BoxChannelControl::from_prepaid(ProtocolControl(owners.clone()), control_memory)
                    .map_err(|(source, _, _)| {
                        error(Stage::Prepare, Reason::Capacity).with_source(source)
                    })?;
            let call = demand.call.clone();
            let upstream = demand.upstream.clone();
            let binding = upstream.binding().clone();
            let (ready, receiver) = oneshot::channel();
            let entry = if binding.prefix.is_empty() {
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
                let mut unstarted = before::BeforeActor(Some(entry.clone()));
                if let Some(observer) = context.upstream.protocol_original_observer() {
                    let retained = preparation
                        .protocol_original(
                            &context.upstream,
                            observer.clone(),
                            &generation.options.protocol.endpoint,
                            &retirement,
                        )
                        .and_then(|original| observer.retain_protocol_original(original));
                    if let Err(source) = retained {
                        owners.set_actor(None);
                        retirement.request_retirement();
                        return Err(source);
                    }
                }
                unstarted.disarm();
                native::spawn(
                    generation.clone(),
                    entry.clone(),
                    preparation,
                    upstream,
                    call,
                    demand.clone(),
                    index,
                    ready,
                )?;
                entry
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
                let mut unstarted = before::BeforeActor(Some(entry.clone()));
                if let Some(observer) = context.upstream.protocol_original_observer() {
                    let retained = grant
                        .protocol_original(
                            &context.upstream,
                            observer.clone(),
                            &generation.options.protocol.endpoint,
                            &call,
                            &generation.tasks,
                            &retirement,
                        )
                        .and_then(|original| observer.retain_protocol_original(original));
                    if let Err(source) = retained {
                        owners.set_actor(None);
                        retirement.request_retirement();
                        return Err(source);
                    }
                }
                unstarted.disarm();
                borrowed::spawn(
                    generation.clone(),
                    entry.clone(),
                    grant,
                    upstream,
                    call,
                    demand.clone(),
                    index,
                    ready,
                )?;
                entry
            };
            let pending = Pending(Some(entry.clone()));
            let wait = async {
                receiver
                    .await
                    .map_err(|source| error(Stage::Prepare, Reason::Internal).with_source(source))?
            };
            let result = tokio::select! {biased;
                result=with_deadline(context.deadline,wait)=>result.map_err(|source|error(Stage::Prepare,Reason::Timeout).with_source(source))?,
                _=context.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled)),
                _=generation.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Revoked)),
            };
            let io = match result {
                Ok(io) => io,
                Err(source) => {
                    let retry = entry.owners.state.lock().await.retry_connection;
                    if retry
                        && index + 1 < demand.len()
                        && !context.cancel.is_cancelled()
                        && !generation.cancel.is_cancelled()
                    {
                        entry.retirement.request_retirement();
                        // This same Root positively closes native/loan and waits
                        // every real Aux/driver Host ACK before the next original.
                        with_deadline(context.deadline, entry.retirement.wait_settled_original())
                            .await
                            .map_err(|source| {
                                error(Stage::Close, Reason::Timeout).with_source(source)
                            })??;
                        drop(pending);
                        drop(entry);
                        generation.reap();
                        index += 1;
                        continue;
                    }
                    return Err(source);
                }
            };
            // Only a logical bi-stream is encrypted here. H3 framing belongs
            // to the physical group; VLESS encryption is applied to the later
            // returned logical XHTTP duplex, exactly once.
            let io = match io {
                Ready::H3(client) => Ready::H3(client),
                Ready::Stream(io) => {
                    let encrypted = generation.options.plan.encrypt(io);
                    let io = with_deadline(context.deadline, encrypted)
                        .await
                        .map_err(|source| {
                            error(Stage::Handshake, Reason::Timeout).with_source(source)
                        })?
                        .map_err(|source| {
                            error(Stage::Handshake, Reason::Io).with_source(source)
                        })?;
                    Ready::Stream(io)
                }
            };
            // Disarm only after the returned View takes the same entry. That View
            // requests this exact retirement on all failed/cancelled submissions.
            return Ok(Initialized {
                entry,
                io: Some(io),
                pending,
            });
        }
    }
    pub(super) fn memory(
        &self,
        layout: PacketMetadataLayout,
    ) -> Result<zero_traits::packet_metadata::PacketMetadataCredit, ChannelError> {
        self.metadata_memory
            .reserve(layout)
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))
    }
    fn make_entry(
        &self,
        owners: PacketMetadataArc<Owners>,
        retirement: CarrierProtocolRetirement,
        origin: Option<CarrierOrigin>,
    ) -> Result<PacketMetadataArc<Entry>, ChannelError> {
        let header = self.memory(
            PacketMetadataArc::<Entry>::layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let original = std::sync::OnceLock::new();
        if let Some(origin) = origin {
            let _ = original.set(origin);
        }
        PacketMetadataArc::from_prepaid(
            Entry {
                owners,
                retirement,
                origin: original,
            },
            header,
        )
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))
    }
}

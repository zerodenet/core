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
mod native;
pub(super) struct Pending(pub Option<PacketMetadataArc<Entry>>);
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(entry) = &self.0 {
            entry.retirement.request_retirement();
        }
    }
}
pub(super) struct Initialized {
    pub entry: PacketMetadataArc<Entry>,
    pub io: Option<zero_platform_tokio::TcpRelayStream>,
    pending: Pending,
}
impl Initialized {
    pub fn into_relay(mut self) -> zero_platform_tokio::TcpRelayStream {
        let io = self.io.take().expect("one original initialized stream");
        let control = zero_traits::AsyncSocket::transport_bypass_control(&io);
        let view = entry::View {
            io: Some(io),
            original: self.entry.clone(),
        };
        let relay = match control {
            Some(control) => {
                zero_platform_tokio::TcpRelayStream::with_transport_bypass_control(view, control)
            }
            None => zero_platform_tokio::TcpRelayStream::new(view),
        };
        self.pending.0.take();
        relay
    }
}
impl Generation {
    pub async fn acquire(
        generation: &PacketMetadataArc<Self>,
        context: &OpenContext,
    ) -> Result<Initialized, ChannelError> {
        let demand = demand::Demand::new(generation, context).await?;
        let mut index = 0;
        loop {
            let position = Generation::reserve(generation)?;
            let drivers = 2 + zero_transport::finalmask::owned::driver_count(
                generation.options.plan.owned_quic_masks(),
            );
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
            let mut io = match result {
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
            // Protocol encryption remains the original immutable accepted client,
            // above this same unique owned QUIC stream, before VLESS submission.
            let encrypted = generation.options.plan.encrypt(io);
            io = with_deadline(context.deadline, encrypted)
                .await
                .map_err(|source| error(Stage::Handshake, Reason::Timeout).with_source(source))?
                .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?;
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

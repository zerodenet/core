//! Install this exact Root controller before either half starts its real Aux.
use super::super::runtime::TcpGeneration;
use super::*;
use crate::component::quic::{
    connect::before::BeforeActor,
    entry::{Entry, Owners, Position, ProtocolControl},
};
use zero_platform_tokio::{
    contract::{
        context::OpenContext,
        lifecycle::BoxChannelControl,
        network::{CarrierContext, UpstreamTransport},
    },
    controlled_carrier::{CarrierGrant, CarrierPreparation, CarrierUserView},
};
use zero_traits::packet_metadata::PacketMetadataLayout;
pub(in crate::component::quic::mixed) struct PreparedTcpOriginal {
    pub generation: PacketMetadataArc<TcpGeneration>,
    pub entry: PacketMetadataArc<Entry>,
    pub grant: CarrierGrant,
    pub view: CarrierUserView,
    pub preparation: CarrierPreparation,
    pub upstream: UpstreamTransport,
    pub call: CarrierContext,
    pub context: OpenContext,
    pub before: BeforeActor,
}
impl PreparedTcpOriginal {
    pub(in crate::component::quic::mixed) async fn new(
        generation: &PacketMetadataArc<TcpGeneration>,
        position: Position,
        context: &OpenContext,
    ) -> Result<Self, ChannelError> {
        if generation.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        let call = CarrierContext {
            session: None,
            deadline: context.deadline,
            cancel: generation.cleanup.child_token(),
        };
        let selecting = match context.upstream.observed_pending_carrier() {
            Some(waiting) => generation.upstream.select_background_carrier_observed(
                Some(context.upstream.binding().clone()),
                call.clone(),
                waiting,
            ),
            None => generation
                .upstream
                .select_background_carrier(Some(context.upstream.binding().clone()), call.clone()),
        };
        let upstream =
            zero_platform_tokio::contract::context::with_deadline(context.deadline, selecting)
                .await
                .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))??;
        let reserve = |layout| generation.memory.reserve(layout).map_err(capacity);
        let header = reserve(PacketMetadataArc::<Owners>::layout().map_err(capacity)?)?;
        let diagnostics = entry::diagnostics::Diagnostics::prepare(&generation.memory)?;
        let received = reserve(
            PacketMetadataLayout::array::<zero_transport::quic::owned::DriverDelivery>(0)
                .map_err(capacity)?,
        )?;
        let logical = reserve(PacketMetadataLayout::boxed::<entry::LogicalControl>())?;
        let owners = PacketMetadataArc::from_prepaid(
            Owners::new(generation.tasks.clone(), diagnostics, received, logical, 0),
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        let _ = owners.protocols.set(
            zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren::new(
                generation.memory.clone(),
                generation.options.origins.get(),
            ),
        );
        owners.state.lock().await.tcp = Some(
            zero_transport::split_http::PreparedXhttpTcp::new(
                generation.buffers.clone(),
                generation.memory.clone(),
                generation.options.version,
            )
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        );
        let masks = generation
            .masks
            .tcp()
            .prepare_owned(generation.buffers.clone(), generation.memory.clone())
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        owners.state.lock().await.tcp_masks = Some(masks);
        // Promise the Entry and actual control header before Root reservation;
        // a low metadata failure cannot leave an unpublishable pending original.
        let entry_header = reserve(PacketMetadataArc::<Entry>::layout().map_err(capacity)?)?;
        let control = BoxChannelControl::from_prepaid(
            ProtocolControl(owners.clone()),
            reserve(PacketMetadataLayout::boxed::<ProtocolControl>())?,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        let (mut grant, retirement) = generation
            .carrier
            .reserve_stream_protocol_original(&generation.tasks, control)
            .map_err(|rejected| {
                owners.set_actor(None);
                rejected.error
            })?;
        let (view, preparation) = grant.claim_preparing_driver_view().map_err(|source| {
            owners.set_actor(None);
            retirement.request_retirement();
            source
        })?;
        let entry = PacketMetadataArc::from_prepaid(
            Entry {
                owners: owners.clone(),
                retirement: retirement.clone(),
                origin: std::sync::OnceLock::new(),
            },
            entry_header,
        )
        .unwrap_or_else(|_| panic!("Entry header already promised"));
        let entry = position.publish(entry);
        let before = BeforeActor(Some(entry.clone()));
        if let Some(observer) = context.upstream.protocol_original_observer() {
            let original = grant.protocol_stream_original(
                &context.upstream,
                observer.clone(),
                &generation.options.protocol.endpoint,
                &call,
                &generation.tasks,
                &retirement,
            )?;
            observer.retain_protocol_original(original)?;
        }
        Ok(Self {
            generation: generation.clone(),
            entry,
            grant,
            view,
            preparation,
            upstream,
            call,
            context: context.clone(),
            before,
        })
    }
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}

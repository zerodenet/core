//! Only this actual establishing Aux/driver may receive the Source children.
use super::factory::Shared;
use super::*;
use zero_platform_tokio::{
    contract::{
        network::{PendingCarrierObserver, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        scope::TaskJoinWitness,
    },
    controlled_carrier::ProtocolOriginal,
};
use zero_traits::packet_metadata::PacketMetadataWeak;
struct Pending {
    shared: PacketMetadataWeak<Shared>,
    entry: PacketMetadataWeak<entry::Entry>,
    caller: TaskJoinWitness,
    anchor: std::sync::OnceLock<PendingCarrierObserver>,
}
pub(super) fn prepare(
    shared: &PacketMetadataArc<Shared>,
    entry: &PacketMetadataArc<entry::Entry>,
    caller: TaskJoinWitness,
    binding: &UpstreamBinding,
) -> Result<UpstreamTransport, ChannelError> {
    let credit = shared
        .memory
        .reserve(UpstreamTransport::opener_metadata_layout::<Pending>(binding).map_err(capacity)?)
        .map_err(capacity)?;
    let (upstream, lease) = UpstreamTransport::from_prepaid_opener(
        binding.clone(),
        Pending {
            shared: PacketMetadataArc::downgrade(shared),
            entry: PacketMetadataArc::downgrade(entry),
            caller,
            anchor: std::sync::OnceLock::new(),
        },
        credit,
    )
    .map_err(|(source, _, _, _)| capacity(source))?;
    let _ = lease
        .actual()
        .anchor
        .set(upstream.pending_carrier_observer());
    drop(lease);
    Ok(upstream)
}
impl Pending {
    fn current(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<(PacketMetadataArc<Shared>, PacketMetadataArc<entry::Entry>), ChannelError> {
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let entry = self
            .entry
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let actual = entry
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Option::as_ref)
            .cloned();
        let actual_driver = entry
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Option::as_ref)
            .cloned();
        if !self
            .anchor
            .get()
            .is_some_and(|anchor| anchor.binding() == binding)
            || shared.cancel.is_cancelled()
            || entry.stop.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().same_scope(&entry.scope)
            || !self
                .caller
                .native_task_id()
                .is_some_and(|id| Some(id) == tokio::task::try_id())
            || !(actual
                .as_ref()
                .is_some_and(|actual| actual.same_registration(&self.caller))
                || actual_driver
                    .as_ref()
                    .is_some_and(|actual| actual.same_registration(&self.caller)))
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        Ok((shared, entry))
    }
}
impl UpstreamOpener for Pending {
    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.anchor.get().cloned()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.current(binding).ok().map(|_| 2)
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let (shared, entry) = self.current(binding)?;
        if !self
            .anchor
            .get()
            .is_some_and(|anchor| original.matches_observer(anchor))
            || original.instance() != shared.dependency
            || original.accepted().is_none()
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        shared
            .upstream
            .validate_pending_protocol_original(binding, &original)?;
        entry
            .children
            .retain_for_caller(original, self.caller.clone())
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::stream::BoxStream, ChannelError>,
    > {
        Box::pin(async { Err(error(Stage::Prepare, Reason::Unsupported)) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { Err(error(Stage::Prepare, Reason::Unsupported)) })
    }
}

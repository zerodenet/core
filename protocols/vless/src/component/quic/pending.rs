//! Child original custody of the actual H3 driver doing group replacement.
//! A caller Query is never impersonated by a later cached driver poll.
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
    generation: PacketMetadataWeak<entry::Generation>,
    parent: PacketMetadataWeak<entry::Entry>,
    driver: TaskJoinWitness,
    anchor: std::sync::OnceLock<PendingCarrierObserver>,
}
pub(super) fn for_current(
    generation: &PacketMetadataArc<entry::Generation>,
    binding: &UpstreamBinding,
) -> Result<Option<UpstreamTransport>, ChannelError> {
    let native = tokio::task::try_id();
    let mut after = 0;
    while let Some((ordinal, parent)) = generation.next_entry(after) {
        after = ordinal;
        let Some(receiver) = parent.retirement.original_receiver() else {
            continue;
        };
        let driver = receiver.protocol_driver();
        if driver.is_joined() || driver.native_task_id().is_none_or(|id| Some(id) != native) {
            continue;
        }
        if generation.cancel.is_cancelled()
            || parent.owners.stop.is_cancelled()
            || parent.owners.protocols.get().is_none()
            || receiver.instance() != generation.instance
            || !driver
                .scope_identity()
                .same_scope(&generation.tasks.scope_identity())
        {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let credit = generation.memory(
            UpstreamTransport::opener_metadata_layout::<Pending>(binding)
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let pending = Pending {
            generation: PacketMetadataArc::downgrade(generation),
            parent: PacketMetadataArc::downgrade(&parent),
            driver: driver.clone(),
            anchor: std::sync::OnceLock::new(),
        };
        let (transport, lease) =
            UpstreamTransport::from_prepaid_opener(binding.clone(), pending, credit).map_err(
                |(source, _, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source),
            )?;
        let _ = lease
            .actual()
            .anchor
            .set(transport.pending_carrier_observer());
        drop(lease);
        return Ok(Some(transport));
    }
    Ok(None)
}
impl Pending {
    fn current(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<
        (
            PacketMetadataArc<entry::Generation>,
            PacketMetadataArc<entry::Entry>,
        ),
        ChannelError,
    > {
        let generation = self
            .generation
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let parent = self
            .parent
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let receiver = parent
            .retirement
            .original_receiver()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        if !self
            .anchor
            .get()
            .is_some_and(|anchor| anchor.binding() == binding)
            || generation.cancel.is_cancelled()
            || parent.owners.stop.is_cancelled()
            || !receiver.protocol_driver().same_registration(&self.driver)
            || receiver.instance() != generation.instance
            || self.driver.is_joined()
            || !self
                .driver
                .native_task_id()
                .is_some_and(|id| Some(id) == tokio::task::try_id())
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        Ok((generation, parent))
    }
}
impl UpstreamOpener for Pending {
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.anchor.get().cloned()
    }
    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        Some(self.current(binding).ok()?.0.options.origins.get())
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let (generation, parent) = self.current(binding)?;
        if !self
            .anchor
            .get()
            .is_some_and(|anchor| original.matches_observer(anchor))
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        generation
            .upstream
            .validate_pending_protocol_original(binding, &original)?;
        parent
            .owners
            .protocols
            .get()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
            .retain(original)
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

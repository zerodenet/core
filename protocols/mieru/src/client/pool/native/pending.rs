//! Only the actual pending record and a weak waiting cleanup callback.
use crate::client::pool::managed::{CarrierLease, PendingCarriers};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        stream::BoxStream,
        BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::CarrierAdoption,
};

pub(super) fn transport(
    lease: &CarrierLease,
    binding: UpstreamBinding,
    waiting: PendingCarrierObserver,
) -> UpstreamTransport {
    // This fixed-profile constructor is the explicit independent library
    // path. Installed Proxy pools use with_dynamic_profiles -> for_setup and
    // their genuine registered setup/driver; it cannot mint that receiver.
    let receiver = Arc::new(Pending {
        originals: lease.pending_carriers(),
        waiting: Some(waiting),
        caller: None,
        provider: None,
        anchor: std::sync::OnceLock::new(),
    });
    let transport = UpstreamTransport::new(binding, receiver.clone());
    let _ = receiver.anchor.set(transport.pending_carrier_observer());
    transport
}
struct Pending {
    originals: PendingCarriers,
    waiting: Option<PendingCarrierObserver>,
    caller: Option<super::setup::Caller>,
    provider: Option<UpstreamTransport>,
    anchor: std::sync::OnceLock<PendingCarrierObserver>,
}
pub(super) fn for_setup(
    originals: PendingCarriers,
    binding: UpstreamBinding,
    waiting: Option<PendingCarrierObserver>,
    caller: super::setup::Caller,
    provider: UpstreamTransport,
) -> Result<UpstreamTransport, ChannelError> {
    originals.prepare_protocols()?;
    let memory = originals
        .protocols()
        .map(|children| children.metadata_view().clone());
    let receiver = Pending {
        originals,
        waiting,
        caller: Some(caller),
        provider: Some(provider),
        anchor: std::sync::OnceLock::new(),
    };
    if let Some(memory) = memory {
        let credit = memory
            .reserve(
                UpstreamTransport::opener_metadata_layout::<Pending>(&binding)
                    .map_err(|source| denied(Reason::Capacity).with_source(source))?,
            )
            .map_err(|source| denied(Reason::Capacity).with_source(source))?;
        let (transport, actual) = UpstreamTransport::from_prepaid_opener(binding, receiver, credit)
            .map_err(|(source, _, _, _)| denied(Reason::Capacity).with_source(source))?;
        assert!(
            actual
                .actual()
                .anchor
                .set(transport.pending_carrier_observer())
                .is_ok(),
            "one actual Mieru pending receiver"
        );
        drop(actual);
        Ok(transport)
    } else {
        let receiver = Arc::new(receiver);
        let transport = UpstreamTransport::new(binding, receiver.clone());
        let _ = receiver.anchor.set(transport.pending_carrier_observer());
        Ok(transport)
    }
}
fn denied(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
impl Pending {
    fn current_receiver(
        &self,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        let caller = self
            .caller
            .as_ref()
            .ok_or_else(|| denied(Reason::StalePlan))?;
        self.originals
            .current_receiver(&caller.witness)
            .ok_or_else(|| denied(Reason::StalePlan))
    }
}
impl UpstreamOpener for Pending {
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.originals.protocols()?;
        self.anchor.get().cloned()
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        self.current_receiver()?;
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
            || !self
                .anchor
                .get()
                .is_some_and(|anchor| original.matches_observer(anchor))
        {
            return Err(denied(Reason::StalePlan));
        }
        self.provider
            .as_ref()
            .ok_or_else(|| denied(Reason::StalePlan))?
            .validate_pending_protocol_original(binding, &original)?;
        self.originals
            .protocols()
            .ok_or_else(|| denied(Reason::StalePlan))?
            .retain(original)
    }

    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
        {
            return Err(denied(Reason::StalePlan));
        }
        self.current_receiver()?;
        let mut caller = self
            .caller
            .as_ref()
            .filter(|caller| &caller.binding == binding)
            .ok_or_else(|| denied(Reason::StalePlan))?
            .clone();
        caller.witness = self.current_receiver()?;
        caller.check(identity, call)
    }
    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        (self
            .anchor
            .get()
            .is_some_and(|anchor| binding == anchor.binding()))
        .then(|| self.originals.capacity())
        .flatten()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
        {
            return Err(denied(Reason::StalePlan));
        }
        if let Some(provider) = &self.provider {
            provider.validate_pending_carrier(binding, &adoption)?;
            self.originals.bind_cleanup_provider(provider, binding)?;
        }
        // The actual caller must accept before I/O. Holding a record alone
        // does not cause cleanup when its connect future is dropped.
        let established = self
            .caller
            .as_ref()
            .map(|caller| {
                self.current_receiver()
                    .map(|receiver| !receiver.same_registration(&caller.witness))
            })
            .transpose()?
            .unwrap_or(false);
        // A confirmed original driver is already the actual Root-held receiver.
        // It receives new children in its same finite record, independently of
        // the now-returned first logical caller. Setup still uses its waiting
        // caller before any IO; no numeric TaskId creates cached authority.
        if !established {
            if let Some(waiting) = &self.waiting {
                waiting.retain(adoption.clone())?;
            }
        }
        self.originals.retain_pending(adoption)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { Err(denied(Reason::Unsupported)) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { Err(denied(Reason::Unsupported)) })
    }
}

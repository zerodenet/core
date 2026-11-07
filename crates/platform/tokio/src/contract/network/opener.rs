use super::*;
use crate::contract::{DeliveryState, Impact, Reason, Stage};

/// Host-supplied execution of an already selected prefix. Implementations must
/// reject stale generations and preserve every hard dial constraint. No direct fallback.
pub trait UpstreamOpener: Send + Sync {
    fn has_preparation_job_owner(&self) -> bool {
        false
    }
    fn reserve_preparation_job(
        &self,
        _binding: &UpstreamBinding,
        _call: &CarrierContext,
    ) -> Result<crate::controlled_carrier::CarrierPreparationJob, ChannelError> {
        Err(ChannelError::new(
            super::super::Stage::Prepare,
            super::super::Reason::Unsupported,
            super::super::Impact::Component,
        ))
    }
    /// Exact same-Host Process dependency and the aggregate's existing UDP
    /// original. Session admission is not inferred from this metadata request.
    fn acquire_process_endpoint_datagram(
        &self,
        _: &UpstreamBinding,
        _: ProcessEndpointDatagramRequest,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierDatagram, ChannelError>> {
        Box::pin(async {
            let mut error =
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
            error.delivery = DeliveryState::NotSubmitted;
            Err(error)
        })
    }
    /// Existing native acquisition under an actual current Process dependency.
    /// Implementations must retain sealed Business admission and both started
    /// Slots. A dependency ID or derived SocketAddr never authorizes this call.
    fn acquire_process_endpoint_stream(
        &self,
        _: &UpstreamBinding,
        _: ProcessEndpointStreamRequest,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierIo, ChannelError>> {
        Box::pin(async {
            let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Session);
            error.delivery = DeliveryState::NotSubmitted;
            Err(error)
        })
    }

    fn packet_caller(
        &self,
        _: &UpstreamBinding,
        _: &crate::contract::context::RequestIdentity,
        _: &CarrierContext,
    ) -> Result<crate::contract::scope::TaskJoinWitness, ChannelError> {
        let mut failure = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Session);
        failure.delivery = DeliveryState::NotSubmitted;
        Err(failure)
    }
    /// Finite cleanup metadata of this actual selected graph. This neither
    /// raises native capacity nor grants IO, task credit or a close receipt.
    fn pending_carrier_capacity(&self, _: &UpstreamBinding) -> Option<usize> {
        None
    }
    /// Validate cleanup identity only, without retaining it or granting I/O.
    /// An implementation must use its actual current plan and binding.
    fn validate_pending_carrier(
        &self,
        _: &UpstreamBinding,
        _: &crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let mut failure = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        failure.delivery = DeliveryState::NotSubmitted;
        Err(failure)
    }

    /// Resolve a selected inner Packet/Flow destination using the actual host
    /// DNS policy. It never prepares a native socket or grants a carrier.
    fn resolve_packet_target(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: crate::contract::context::RequestIdentity,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        Box::pin(async {
            Err(ChannelError::new(
                Stage::Prepare,
                Reason::Unsupported,
                Impact::Session,
            ))
        })
    }
    /// Resolve a protocol-negotiated Datagram peer under an already acquired
    /// control original. Only an explicitly declared association owner and
    /// its actual host Node resolver may provide this capability.
    fn resolve_related_datagram(
        &self,
        _: &UpstreamBinding,
        _: crate::controlled_carrier::CarrierAdoption,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        Box::pin(async {
            let mut error =
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
            error.delivery = DeliveryState::NotSubmitted;
            Err(error)
        })
    }
    /// Observation of the original Business caller's actual tracked join.
    /// This selects user custody only; it issues no grant or close receipt.
    /// Native acquisition must still validate the same sealed caller ticket.
    fn has_tracked_business_caller(&self) -> bool {
        false
    }
    /// Declaration of an actual waiting cleanup ledger, never I/O authority.
    /// Older library/service callers retain their existing pool ownership and
    /// are not qualified for immediate pre-delivery cancellation by this hint.
    /// A declared observer must still validate every actual retain operation.
    fn has_pending_carrier_observer(&self) -> bool {
        false
    }
    /// The actual waiting owner retains this sealed original before a donor
    /// can acquire I/O or suspend. This transfers cleanup authority only; it
    /// grants no native access and never reports that an original is closed.
    fn retain_pending_carrier(
        &self,
        _: &UpstreamBinding,
        adoption: crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.component = Some(adoption.instance().component);
        error.delivery = DeliveryState::NotSubmitted;
        Err(error)
    }
    fn borrow_datagram(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::DatagramLoan, ChannelError>> {
        Box::pin(async {
            let mut error =
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
            error.delivery = DeliveryState::NotSubmitted;
            Err(error)
        })
    }
    /// Observation only: retain no I/O authority and issue no close receipt.
    fn observe_carrier_origin(&self, _: &crate::controlled_carrier::CarrierOrigin) -> bool {
        false
    }
    /// A persistent host provider selects a fresh actual binding, without
    /// retaining a business opener, registry or historical configuration.
    fn select_background_carrier(
        &self,
        _: Option<UpstreamBinding>,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.delivery = DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }
    /// Selection with the actual waiting caller's weak cleanup authority.
    /// The host installs it before constructing any prefix opener or doing I/O.
    fn select_background_carrier_observed(
        &self,
        _: Option<UpstreamBinding>,
        _: CarrierContext,
        _: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async {
            let mut error =
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
            error.delivery = DeliveryState::NotSubmitted;
            Err(error)
        })
    }
    /// Opt-in logical borrowing from the selected real donor. An unowned
    /// BoxStream is never promoted to a controlled carrier after opening it.
    fn borrow_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::StreamLoan, ChannelError>> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.delivery = DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }
    fn carrier_dns_generation(&self) -> Option<crate::contract::dns::DnsGeneration> {
        None
    }
    /// Host-owned node resolution only. The result retains the original domain
    /// and full binding; it can prepare candidates only against that declaration.
    fn resolve_controlled_carrier(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.delivery = DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }
    /// Synchronous original acquisition; the same persistent owner is installed
    /// before the returned packet view can be polled or moved into a driver.
    fn prepare_controlled_datagram(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::CarrierDatagram, ChannelError> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.component = Some(grant.instance().component);
        error.delivery = DeliveryState::NotSubmitted;
        Err(error)
    }
    /// Selection hint only. It issues no grant; the controlled open must still
    /// validate the original request, owner, caller and selected generation.
    fn supports_controlled_stream(&self) -> bool {
        false
    }

    /// The same controlled dynamic request with the actual original logical
    /// custody returned, for an existing selected prefix. No grants are minted.
    fn open_controlled_stream_loan(
        &self,
        _: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::StreamLoan, ChannelError>> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.component = Some(request.grants.instance().component);
        error.session = request.context.session;
        error.delivery = DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }

    /// Multi-candidate resolution/acquisition is opt-in. Reject before legacy
    /// execution or native I/O; an unsupported opener cannot silently fall back.
    fn open_controlled_stream(
        &self,
        _: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let mut error = ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.component = Some(request.grants.instance().component);
        error.session = request.context.session;
        error.delivery = DeliveryState::NotSubmitted;
        Box::pin(async move {
            Err(error.with_source(std::io::Error::other(
                "controlled multi-candidate stream capability was not granted",
            )))
        })
    }

    /// Acquire controlled I/O under the caller's original reservation, whether
    /// native or borrowed. The default consumes synchronous native preparation;
    /// async DNS and logical loans implement this same acquisition boundary.
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierIo, ChannelError>> {
        let prepared = self.prepare_controlled_stream(binding, destination, context.clone(), grant);
        Box::pin(async move {
            let prepared = prepared?;
            Ok(prepared
                .open_with_deadline(context.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
    /// Controlled capability is opt-in. Reject before any legacy opener or I/O.
    fn prepare_controlled_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::PreparedCarrier, ChannelError> {
        let mut error =
            super::ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component);
        error.component = Some(grant.instance().component);
        error.delivery = DeliveryState::NotSubmitted;
        Err(error.with_source(std::io::Error::other(
            "controlled carrier capability was not granted",
        )))
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>>;
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>>;
}

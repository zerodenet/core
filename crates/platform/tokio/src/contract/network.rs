use std::{mem::ManuallyDrop, sync::Arc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::{NetworkGeneration, RoleInstanceId, SessionId},
    request::Destination,
};
use zero_traits::DialPolicy;

use super::{
    datagram::{BoxDatagramSession, DatagramRequest},
    stream::BoxStream,
    BoxFuture, ChannelError,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpstreamBinding {
    pub prefix: Vec<RoleInstanceId>,
    pub dial: DialPolicy,
    pub network_generation: NetworkGeneration,
}

#[derive(Clone)]
pub struct CarrierContext {
    /// None for an explicitly declared background control carrier.
    pub session: Option<SessionId>,
    /// An additional absolute caller bound. Native operation timers still apply.
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
}

/// A finite physical owner supplied by the actual selected role. Capacity and
/// weak task registration do not by themselves authorize a business session;
/// the host opener must validate its sealed demand for every real candidate.
pub struct ControlledStreamRequest {
    pub destination: Destination,
    pub context: CarrierContext,
    pub grants: crate::controlled_carrier::CarrierGrants,
    pub tasks: super::scope::WeakTaskRegistrar,
    /// Inline retirement budget only; never a replacement handshake deadline.
    pub retirement: std::time::Duration,
}

/// One selected, real Process dependency and the same caller's original native grant.
/// This carries no Process authority, Business admission or new pool capacity.
pub struct ProcessEndpointStreamRequest {
    pub process: zero_core::component::ComponentInstanceId,
    pub process_ordinal: usize,
    pub endpoint: Destination,
    pub context: CarrierContext,
    pub grant: crate::controlled_carrier::CarrierGrant,
    /// Original protocol startup bound, capped by the actual invocation deadline.
    pub startup_deadline: Instant,
}

/// A real background aggregate's own original UDP grant and Process dependency.
/// Only session=None may acquire this packet original; it is not a Business ticket.
pub struct ProcessEndpointDatagramRequest {
    pub process: zero_core::component::ComponentInstanceId,
    pub process_ordinal: usize,
    pub endpoint: Destination,
    pub context: CarrierContext,
    pub grant: crate::controlled_carrier::CarrierGrant,
    /// Protocol-owned local startup bound, never an invented Business lifetime.
    pub startup_deadline: Instant,
}

mod opener;
pub use opener::UpstreamOpener;

#[derive(Clone)]
pub struct UpstreamTransport {
    binding: ManuallyDrop<Arc<UpstreamBinding>>,
    opener: ManuallyDrop<Arc<dyn UpstreamOpener>>,
    #[cfg(feature = "native-packet-origin")]
    packet_metadata: Option<zero_tun::packet_memory::PacketMetadataAttachment>,
}

/// Weak reference to the actual waiting owner's existing cleanup ledger.
/// It cannot authorize native I/O and does not retain a historical caller.
#[derive(Clone)]
pub struct PendingCarrierObserver {
    binding: ManuallyDrop<Arc<UpstreamBinding>>,
    opener: ManuallyDrop<std::sync::Weak<dyn UpstreamOpener>>,
    #[cfg(feature = "native-packet-origin")]
    packet_metadata: Option<zero_tun::packet_memory::PacketMetadataAttachment>,
}
impl PendingCarrierObserver {
    /// Same concrete opener, weakly observed; private to runtime contracts.
    /// This checks full binding and liveness, never creates IO authority.
    pub(in crate::contract) fn upgrade_exact(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<UpstreamOpenerLease, ChannelError> {
        if self.binding.as_ref() != binding {
            return Err(ChannelError::new(
                super::Stage::Prepare,
                super::Reason::StalePlan,
                super::Impact::Component,
            ));
        }
        let opener = self.opener.upgrade().ok_or_else(|| {
            ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Revoked,
                super::Impact::Component,
            )
        })?;
        Ok(UpstreamOpenerLease {
            opener: ManuallyDrop::new(opener),
            #[cfg(feature = "native-packet-origin")]
            packet_metadata: self.packet_metadata.clone(),
        })
    }

    /// Read only the actual waiting opener's independently registered caller.
    /// This neither authorizes I/O nor turns a query/task number into a grant.
    pub fn packet_caller(
        &self,
        identity: &crate::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<crate::contract::scope::TaskJoinWitness, ChannelError> {
        let opener = self.opener.upgrade().ok_or_else(|| {
            let mut error = ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Revoked,
                super::Impact::Component,
            );
            error.delivery = super::DeliveryState::NotSubmitted;
            error
        })?;
        opener.packet_caller(&self.binding, identity, call)
    }
    pub fn packet_driver_custody(
        &self,
        identity: &crate::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<Option<crate::contract::component::PacketDriverCallRetained>, ChannelError> {
        let opener = self.opener.upgrade().ok_or_else(|| {
            ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Revoked,
                super::Impact::Component,
            )
        })?;
        opener.packet_driver_custody(&self.binding, identity, call)
    }
    pub fn capacity(&self) -> Option<usize> {
        self.opener
            .upgrade()?
            .pending_carrier_capacity(&self.binding)
    }

    pub fn protocol_original_observer(&self) -> Option<Self> {
        self.opener.upgrade()?.protocol_original_observer()
    }
    /// Canonical actual receiver identity, not equal-valued caller facts.
    pub fn same_observer(&self, other: &Self) -> bool {
        self.binding == other.binding && std::sync::Weak::ptr_eq(&self.opener, &other.opener)
    }
    pub fn retain_protocol_original(
        &self,
        original: crate::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        if !original.matches_observer(self) {
            return Err(ChannelError::new(
                super::Stage::Prepare,
                super::Reason::StalePlan,
                super::Impact::Component,
            ));
        }
        let opener = self.opener.upgrade().ok_or_else(|| {
            ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Revoked,
                super::Impact::Component,
            )
        })?;
        opener.retain_protocol_original(&self.binding, original)
    }
    pub fn retain(
        &self,
        adoption: crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let opener = self.opener.upgrade().ok_or_else(|| {
            let mut error = ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Revoked,
                super::Impact::Component,
            );
            error.delivery = super::DeliveryState::NotSubmitted;
            error
        })?;
        opener.retain_pending_carrier(&self.binding, adoption)
    }
}

impl UpstreamTransport {
    pub fn has_preparation_job_owner(&self) -> bool {
        self.opener.has_preparation_job_owner()
    }
    pub fn reserve_preparation_job(
        &self,
        call: &CarrierContext,
    ) -> Result<crate::controlled_carrier::CarrierPreparationJob, ChannelError> {
        self.opener.reserve_preparation_job(&self.binding, call)
    }
    pub fn acquire_process_endpoint_datagram(
        &self,
        request: ProcessEndpointDatagramRequest,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierDatagram, ChannelError>> {
        self.opener
            .acquire_process_endpoint_datagram(&self.binding, request)
    }
    pub fn acquire_process_endpoint_stream(
        &self,
        request: ProcessEndpointStreamRequest,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierIo, ChannelError>> {
        self.opener
            .acquire_process_endpoint_stream(&self.binding, request)
    }

    pub fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.opener.protocol_original_observer()
    }
    pub(crate) fn original_binding(&self) -> Arc<UpstreamBinding> {
        Arc::clone(&self.binding)
    }
    pub fn pending_carrier_capacity(&self) -> Option<usize> {
        self.opener.pending_carrier_capacity(&self.binding)
    }
    /// Read the original provider's finite declaration for the selected path.
    /// This metadata projection neither selects I/O nor installs a claim.
    pub fn pending_carrier_capacity_for(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.opener.pending_carrier_capacity(binding)
    }
    /// Validate an original against the provider's actual current graph.
    /// The caller must still retain it in its own finite cleanup ledger.
    pub fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: &crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.opener.validate_pending_carrier(binding, adoption)
    }

    pub fn borrow_service_datagram_candidate(
        &self,
        target: crate::controlled_carrier::loan::ServiceDatagramTarget,
    ) -> BoxFuture<
        '_,
        Result<crate::controlled_carrier::loan::ServiceDatagramTargetLoan, ChannelError>,
    > {
        self.opener
            .borrow_service_datagram_candidate(&self.binding, target)
    }
    pub fn resolve_service_datagram_candidates(
        &self,
        issuer: crate::controlled_carrier::loan::ServiceDatagramTargetIssuer,
    ) -> BoxFuture<
        '_,
        Result<crate::controlled_carrier::loan::BorrowedDatagramCandidates, ChannelError>,
    > {
        self.opener
            .resolve_service_datagram_candidates(&self.binding, issuer)
    }
    pub fn resolve_packet_target(
        &self,
        destination: Destination,
        identity: crate::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        self.opener
            .resolve_packet_target(&self.binding, destination, identity, call)
    }

    pub fn resolve_related_datagram(
        &self,
        parent: crate::controlled_carrier::CarrierAdoption,
        peer: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        self.opener
            .resolve_related_datagram(&self.binding, parent, peer, context)
    }
    /// Original caller observation from this exact host demand/binding. The
    /// actual opener must check admission, full identity and live generation;
    /// merely matching a public session number grants no caller provenance.
    pub fn packet_caller(
        &self,
        identity: &crate::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<crate::contract::scope::TaskJoinWitness, ChannelError> {
        self.opener.packet_caller(&self.binding, identity, call)
    }
    pub fn packet_driver_custody(
        &self,
        identity: &crate::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<Option<crate::contract::component::PacketDriverCallRetained>, ChannelError> {
        self.opener
            .packet_driver_custody(&self.binding, identity, call)
    }
    pub fn has_tracked_business_caller(&self) -> bool {
        self.opener.has_tracked_business_caller()
    }
    pub fn observed_pending_carrier(&self) -> Option<PendingCarrierObserver> {
        self.opener
            .has_pending_carrier_observer()
            .then(|| self.pending_carrier_observer())
    }
    pub fn has_pending_carrier_observer(&self) -> bool {
        self.opener.has_pending_carrier_observer()
    }
    pub fn pending_carrier_observer(&self) -> PendingCarrierObserver {
        PendingCarrierObserver {
            binding: self.binding.clone(),
            opener: ManuallyDrop::new(Arc::downgrade(&self.opener)),
            #[cfg(feature = "native-packet-origin")]
            packet_metadata: self.packet_metadata.clone(),
        }
    }
    pub fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        context: CarrierContext,
        waiting: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<Self, ChannelError>> {
        self.opener
            .select_background_carrier_observed(binding, context, waiting)
    }
    pub fn retain_pending_carrier(
        &self,
        adoption: crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.opener.retain_pending_carrier(&self.binding, adoption)
    }
    pub fn borrow_datagram(
        &self,
        endpoint: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::DatagramLoan, ChannelError>> {
        self.opener
            .borrow_datagram(&self.binding, endpoint, context)
    }
    pub fn observe_carrier_origin(
        &self,
        origin: &crate::controlled_carrier::CarrierOrigin,
    ) -> bool {
        self.opener.observe_carrier_origin(origin)
    }
    pub fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<Self, ChannelError>> {
        self.opener.select_background_carrier(binding, context)
    }
    pub fn borrow_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::StreamLoan, ChannelError>> {
        self.opener
            .borrow_stream(&self.binding, destination, context)
    }
    pub fn carrier_dns_generation(&self) -> Option<super::dns::DnsGeneration> {
        self.opener.carrier_dns_generation()
    }
    pub fn resolve_controlled_carrier(
        &self,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        self.opener
            .resolve_controlled_carrier(&self.binding, destination, context)
    }
    pub fn prepare_controlled_datagram(
        &self,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::CarrierDatagram, ChannelError> {
        self.opener
            .prepare_controlled_datagram(&self.binding, destination, context, grant)
    }
    pub fn new(binding: UpstreamBinding, opener: Arc<dyn UpstreamOpener>) -> Self {
        Self {
            binding: ManuallyDrop::new(Arc::new(binding)),
            opener: ManuallyDrop::new(opener),
            #[cfg(feature = "native-packet-origin")]
            packet_metadata: None,
        }
    }
    pub fn supports_controlled_stream(&self) -> bool {
        self.opener.supports_controlled_stream()
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
    pub fn prepare_controlled_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::PreparedCarrier, ChannelError> {
        self.opener
            .prepare_controlled_stream(&self.binding, destination, context, grant)
    }
    pub fn acquire_controlled_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierIo, ChannelError>> {
        self.opener
            .acquire_controlled_stream(&self.binding, destination, context, grant)
    }

    pub fn open_controlled_stream_loan(
        &self,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::StreamLoan, ChannelError>> {
        self.opener
            .open_controlled_stream_loan(&self.binding, request)
    }

    pub fn open_controlled_stream(
        &self,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.opener.open_controlled_stream(&self.binding, request)
    }
    pub fn open_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.opener.open_stream(&self.binding, destination, context)
    }
    pub fn open_datagram(
        &self,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        self.opener.open_datagram(&self.binding, request, context)
    }
}

mod event_limits;
pub use event_limits::EventNetworkLimits;

mod packet_metadata;
pub(in crate::contract) use packet_metadata::UpstreamOpenerLease;

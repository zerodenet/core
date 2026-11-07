mod business;
mod compatibility;
mod datagram;
mod loan;
mod packet_loan;
mod preparation;
mod selection;
use super::health::FirstHopHealth;
use crate::component::instance::invalid;
use crate::protocol_registry::UpstreamConnectServices;
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    context::{with_deadline, OpenContext},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::{BoxStream, ConnectRequest, DynStreamOutbound},
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(crate) struct PrefixOpener {
    pub(super) role: Arc<dyn DynStreamOutbound>,
    // Set only by the actual packet-driver path. Retain its original facts Arc
    // through shorter-prefix borrowing; this is not a witness or IO grant.
    pub(super) service_identity: Option<zero_platform_tokio::contract::context::RequestIdentity>,
    pub(super) business: Option<crate::component::business_carrier::BusinessCarrierRequest>,
    pub(super) plan: Option<std::sync::Weak<crate::component::catalog::ComponentPlan>>,
    pub(super) datagram_slot: Option<Arc<crate::component::instance::ComponentSlot>>,
    pub(super) context: OpenContext,
    pub(super) expected_binding: UpstreamBinding,
    pub(super) expected_endpoint: Destination,
    pub(super) request_destination: Destination,
    pub(super) network: UpstreamConnectServices,
    pub(super) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(super) health: Option<FirstHopHealth>,
}
impl PrefixOpener {
    pub(crate) fn for_service(
        role: Arc<dyn DynStreamOutbound>,
        context: OpenContext,
        expected_binding: UpstreamBinding,
        expected_endpoint: Destination,
        request_destination: Destination,
        network: UpstreamConnectServices,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        Self {
            role,
            service_identity: None,
            business: None,
            plan: None,
            context,
            expected_binding,
            expected_endpoint,
            request_destination,
            network,
            observer,
            health: None,
            // Added by the parallel raw Datagram provider. DNS TCP services
            // receive no unrelated Datagram grant.
            datagram_slot: None,
        }
    }
    fn interrupted(&self, carrier: &CarrierContext, reason: Reason) -> ChannelError {
        let mut error = ChannelError::new(Stage::Dial, reason, Impact::Session);
        error.component = self
            .expected_binding
            .prefix
            .last()
            .map(|role| role.component);
        error.session = carrier.session;
        // The prefix may already have sent a proxy request while opening this
        // carrier. Never claim a retryable empty root after cancellation.
        error.delivery = DeliveryState::Unknown;
        error
    }
}
impl UpstreamOpener for PrefixOpener {
    fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::network::UpstreamTransport, ChannelError>,
    > {
        self.reselect(binding, call, None)
    }
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: zero_platform_tokio::contract::network::PendingCarrierObserver,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::network::UpstreamTransport, ChannelError>,
    > {
        self.reselect(binding, call, Some(waiting))
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        self.acquire_business(binding.clone(), endpoint, call, grant)
    }
    fn has_tracked_business_caller(&self) -> bool {
        self.business
            .as_ref()
            .is_some_and(|request| request.has_tracked_caller())
            || self.context.upstream.has_tracked_business_caller()
    }
    fn has_pending_carrier_observer(&self) -> bool {
        self.context.upstream.observed_pending_carrier().is_some()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != &self.expected_binding
            || binding.network_generation.0 != self.network.network_generation()
        {
            return None;
        }
        self.context
            .upstream
            .pending_carrier_capacity()
            .or_else(|| {
                self.plan
                    .as_ref()?
                    .upgrade()?
                    .pending_carrier_capacity(&binding.prefix)
                    .ok()
            })
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != &self.expected_binding
            || binding.network_generation.0 != self.network.network_generation()
        {
            return Err(invalid(
                Reason::StalePlan,
                "pending validation prefix changed",
            ));
        }
        let plan = self
            .plan
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| invalid(Reason::Revoked, "pending validation plan retired"))?;
        if binding
            .prefix
            .iter()
            .any(|role| plan.validate_carrier_adoption(*role, adoption).is_ok())
        {
            Ok(())
        } else {
            Err(invalid(
                Reason::StalePlan,
                "pending original is outside actual prefix graph",
            ))
        }
    }
    fn packet_driver_custody(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<
        Option<zero_platform_tokio::contract::component::PacketDriverCallRetained>,
        ChannelError,
    > {
        self.packet_caller(binding, identity, call)?;
        if self.business.is_some() {
            Ok(None)
        } else {
            self.context.upstream.packet_driver_custody(identity, call)
        }
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if binding != &self.expected_binding
            || binding.network_generation.0 != self.network.network_generation()
        {
            return Err(invalid(Reason::StalePlan, "Packet caller prefix changed"));
        }
        if let Some(business) = &self.business {
            return business.packet_caller(identity, call);
        }
        self.context.upstream.packet_caller(identity, call)
    }
    fn resolve_service_datagram_candidates(
        &self,
        binding: &UpstreamBinding,
        issuer: zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetIssuer,
    ) -> BoxFuture<
        '_,
        Result<
            zero_platform_tokio::controlled_carrier::loan::BorrowedDatagramCandidates,
            ChannelError,
        >,
    > {
        if binding != &self.expected_binding
            || binding.network_generation.0 != self.network.network_generation()
            || issuer.binding() != binding
        {
            return Box::pin(async {
                Err(crate::component::instance::invalid(
                    Reason::StalePlan,
                    "actual Service candidate binding changed",
                ))
            });
        }
        self.context
            .upstream
            .resolve_service_datagram_candidates(issuer)
    }
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        if binding != &self.expected_binding
            || binding.network_generation.0 != self.network.network_generation()
        {
            return Box::pin(async {
                Err(invalid(Reason::StalePlan, "packet target prefix changed"))
            });
        }
        self.context
            .upstream
            .resolve_packet_target(destination, identity, call)
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != &self.expected_binding
            || binding.network_generation.0 != self.network.network_generation()
        {
            return Err(invalid(
                Reason::StalePlan,
                "pending original prefix changed",
            ));
        }
        self.context.upstream.retain_pending_carrier(adoption)
    }
    fn borrow_service_datagram_candidate(
        &self,
        binding: &UpstreamBinding,
        target: zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTarget,
    ) -> BoxFuture<
        '_,
        Result<
            zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetLoan,
            ChannelError,
        >,
    > {
        self.borrow_selected_target(binding.clone(), target)
    }
    fn borrow_datagram(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError>,
    > {
        self.borrow_selected_datagram(binding.clone(), endpoint, call)
    }
    fn observe_carrier_origin(
        &self,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        self.context.upstream.observe_carrier_origin(origin)
    }

    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        Some(self.network.dns_generation())
    }
    fn borrow_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        carrier: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError>,
    > {
        self.borrow_selected(binding.clone(), destination, carrier)
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.open_compatibility_stream(binding, destination, carrier)
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        self.open_compatibility_datagram(binding, request, carrier)
    }
}

#[cfg(test)]
#[path = "../../tests/relay_prefix.rs"]
mod tests;

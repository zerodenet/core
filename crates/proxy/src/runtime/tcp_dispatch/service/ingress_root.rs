//! Active ingress's selected original physical root. It cannot execute a prefix or
//! substitute a different destination, caller identity, or network generation.
use crate::protocol_registry::UpstreamConnectServices;
use std::sync::{Arc, Weak};
#[path = "ingress_root/controlled.rs"]
mod controlled;
#[cfg(test)]
#[path = "tests/legacy_root.rs"]
mod legacy_stream;
use zero_core::request::Destination;
#[cfg(test)]
use zero_platform_tokio::contract::dns::DnsQueryRole;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::BoxStream,
    BoxFuture, ChannelError, Reason,
};

#[cfg(test)]
use std::{net::SocketAddr, sync::Mutex};
#[cfg(test)]
pub(super) struct Evidence {
    pub(super) local: SocketAddr,
    pub(super) peer: SocketAddr,
}
pub(super) struct RootOpener {
    pub(super) services: UpstreamConnectServices,
    pub(super) binding: UpstreamBinding,
    pub(super) endpoint: Destination,
    #[cfg(test)]
    pub(super) role: DnsQueryRole,
    #[cfg(test)]
    pub(super) evidence: Arc<Mutex<Option<Evidence>>>,
    pub(super) source: Weak<crate::runtime::current_services::CurrentServiceSource>,
    pub(super) selected: zero_core::component::RoleInstanceId,
    pub(super) call: CarrierContext,
    pub(super) candidates: tokio::sync::Mutex<
        Option<(
            zero_platform_tokio::controlled_carrier::CarrierCandidates,
            usize,
        )>,
    >,
}
impl RootOpener {
    fn direct_transport(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<zero_platform_tokio::contract::network::UpstreamTransport, ChannelError> {
        if binding != &self.binding
            || self.call.session.is_some()
            || self.call.cancel.is_cancelled()
        {
            return Err(super::failure(Reason::StalePlan));
        }
        let source = self
            .source
            .upgrade()
            .ok_or_else(|| super::failure(Reason::Revoked))?;
        let current = source
            .runtime_services()
            .map_err(|source| super::failure(Reason::Revoked).with_source(source))?
            .execution();
        let plan = current.components.as_ref().map_err(|source| {
            super::failure(Reason::Revoked)
                .with_source(crate::component::instance::retained(source.clone()))
        })?;
        let slot = plan
            .role_slot(self.selected)
            .ok_or_else(|| super::failure(Reason::StalePlan))?;
        crate::component::direct_bridge::service_transport(
            &slot,
            Arc::downgrade(plan),
            self.services.clone(),
            self.endpoint.clone(),
            binding.clone(),
            self.call.clone(),
            None,
        )
    }
    #[cfg(test)]
    pub(super) fn take_evidence(&self) -> Option<Evidence> {
        self.evidence
            .lock()
            .expect("DNS service root evidence poisoned")
            .take()
    }
}
impl UpstreamOpener for RootOpener {
    fn supports_controlled_stream(&self) -> bool {
        self.direct_transport(&self.binding).is_ok()
    }
    fn open_controlled_stream_loan(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ControlledStreamRequest,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError>,
    > {
        let binding = binding.clone();
        Box::pin(async move {
            let upstream = self.direct_transport(&binding)?;
            let loan = upstream.open_controlled_stream_loan(request).await?;
            if !self.observe_carrier_origin(loan.owner.original()) {
                return Err(super::failure(Reason::StalePlan));
            }
            Ok(loan)
        })
    }
    fn open_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let loan = self.open_controlled_stream_loan(&binding, request).await?;
            Ok(crate::component::background::stateless::service_stream(
                loan,
            ))
        })
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        matches!(self.endpoint.address, zero_core::Address::Domain(_))
            .then(|| self.services.dns_generation())
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        self.acquire_original(binding.clone(), destination, context, grant)
    }
    fn observe_carrier_origin(
        &self,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        let root = origin.native_origin();
        let Some((endpoint, binding)) = root.selected_binding() else {
            return false;
        };
        if endpoint != self.endpoint
            || binding != self.binding
            || self.services.network_generation() != binding.network_generation.0
            || root.is_retired()
        {
            return false;
        }
        let Some(endpoints) = root.endpoints() else {
            return false;
        };
        let Some(_local) = endpoints.local() else {
            return false;
        };
        #[cfg(test)]
        {
            *self
                .evidence
                .lock()
                .expect("service root evidence poisoned") = Some(Evidence {
                local: _local,
                peer: endpoints.peer(),
            });
        }
        true
    }

    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        #[cfg(test)]
        {
            legacy_stream::open(self, binding, destination, context)
        }
        #[cfg(not(test))]
        {
            let _ = (binding, destination, context);
            Box::pin(async { Err(super::failure(Reason::Unsupported)) })
        }
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { Err(super::failure(Reason::Unsupported)) })
    }
}

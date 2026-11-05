//! A DNS service's selected physical root. It cannot execute a prefix or
//! substitute a different destination, caller identity, or network generation.
use crate::protocol_registry::UpstreamConnectServices;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, Weak},
};
mod controlled;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    dns::DnsQueryRole,
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::BoxStream,
    BoxFuture, ChannelError, Reason,
};

pub(super) struct Evidence {
    pub(super) local: SocketAddr,
    pub(super) peer: SocketAddr,
}
pub(super) struct RootOpener {
    pub(super) services: UpstreamConnectServices,
    pub(super) binding: UpstreamBinding,
    pub(super) endpoint: Destination,
    pub(super) role: DnsQueryRole,
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
    pub(super) fn take_evidence(&self) -> Option<Evidence> {
        self.evidence
            .lock()
            .expect("DNS service root evidence poisoned")
            .take()
    }
}
impl UpstreamOpener for RootOpener {
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
        let Some(local) = endpoints.local() else {
            return false;
        };
        *self
            .evidence
            .lock()
            .expect("service root evidence poisoned") = Some(Evidence {
            local,
            peer: endpoints.peer(),
        });
        true
    }

    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != self.binding
                || !binding.prefix.is_empty()
                || destination != self.endpoint
                || context.session.is_some()
            {
                return Err(super::failure(Reason::StalePlan));
            }
            let socket = self
                .services
                .open_root_stream(&binding, destination, self.role, context)
                .await?;
            let local = socket
                .local_addr()
                .map_err(|source| super::failure(Reason::Io).with_source(source))?;
            let peer = socket
                .peer_addr()
                .map_err(|source| super::failure(Reason::Io).with_source(source))?;
            *self
                .evidence
                .lock()
                .expect("DNS service root evidence poisoned") = Some(Evidence { local, peer });
            Ok(Box::pin(socket) as BoxStream)
        })
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

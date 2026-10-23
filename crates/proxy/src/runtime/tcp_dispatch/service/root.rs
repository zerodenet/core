//! A DNS service's selected physical root. It cannot execute a prefix or
//! substitute a different destination, caller identity, or network generation.
use crate::protocol_registry::UpstreamConnectServices;
use std::{net::SocketAddr, sync::Mutex};
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
    pub(super) evidence: Mutex<Option<Evidence>>,
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

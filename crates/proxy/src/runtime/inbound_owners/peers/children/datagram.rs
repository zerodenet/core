//! Host activation of a distinct finite outbound grant for one actual peer.
use super::*;
use crate::component::instance::HostOutboundDatagramResources;
use std::{net::SocketAddr, num::NonZeroUsize};
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::{
    contract::{
        component::{ResourceAccess, ResourceDeclaration},
        lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle},
    },
    EgressInterfaceControl,
};

pub(crate) struct PeerDatagramGrant {
    pub(crate) identity: PeerChildIdentity,
    pub(crate) resources: Arc<dyn ResourceAccess>,
    pub(crate) network_generation: NetworkGeneration,
}
struct GrantedPeerResource {
    provider: Arc<HostOutboundDatagramResources>,
    task: TaskId,
    inner: Box<dyn DynResourceLifecycle>,
    pending: CloseReport,
    protocol_closed: bool,
}
impl PeerResourceContext {
    pub(crate) async fn claim_outbound_datagram<T>(
        &self,
        deadline: Instant,
        peer: SocketAddr,
        egress: EgressInterfaceControl,
        build: impl FnOnce(
            PeerDatagramGrant,
        ) -> Result<(Box<dyn DynResourceLifecycle>, T), ChannelError>,
    ) -> Result<T, ChannelError> {
        let network_generation = NetworkGeneration(egress.generation());
        let cancel = self.binding.cancellation();
        self.claim(deadline, |identity| {
            let declaration = ResourceDeclaration::OutboundDatagram {
                peer,
                network_generation,
                max_sockets: NonZeroUsize::new(1).expect("one finite peer socket"),
            };
            let provider = HostOutboundDatagramResources::new(
                identity.instance,
                &declaration,
                egress,
                cancel,
            )?;
            let (inner, value) = build(PeerDatagramGrant {
                identity,
                resources: provider.clone(),
                network_generation,
            })?;
            Ok((
                erase_lifecycle(GrantedPeerResource {
                    provider,
                    task: identity.task,
                    inner,
                    pending: CloseReport::default(),
                    protocol_closed: false,
                }),
                value,
            ))
        })
        .await
    }
}
impl ResourceLifecycle for GrantedPeerResource {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        self.provider.activate(self.task)?;
        if let Err(error) = self.inner.start(deadline).await {
            self.provider.revoke();
            return Err(error);
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.provider.revoke();
        self.inner.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.provider.revoke();
        if !self.protocol_closed {
            let report = self.inner.shutdown(deadline).await?;
            self.protocol_closed = report.is_complete();
            self.pending.unfinished.clear();
            self.pending.append(report);
        }
        // Keep the original protocol evidence in this persistent wrapper if
        // the provider still reports an Unknown native registration failure.
        if self.protocol_closed {
            self.pending.append(self.provider.shutdown_io()?);
        }
        Ok(std::mem::take(&mut self.pending))
    }
}

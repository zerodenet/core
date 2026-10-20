//! Explicit finite epoch grants; there is deliberately no default activation.
mod close;
mod completion;
pub(crate) use completion::PeerBoundDatagramRegistration;
#[cfg(test)]
mod tests;
use super::*;
use crate::{
    component::bound_datagram::HostBoundDatagramResources,
    runtime::udp_dispatch::operation::DirectSocketSource,
};
use std::num::NonZeroUsize;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{
        DatagramBindingAuthorization, DatagramBindingId, ResourceAccess, ResourceDeclaration,
    },
    lifecycle::{erase_lifecycle, ResourceLifecycle},
};

pub(crate) struct PeerBoundDatagramGrant {
    pub(crate) identity: PeerChildIdentity,
    pub(crate) resources: Arc<dyn ResourceAccess>,
}
#[derive(Clone)]
pub(crate) struct AuthorizedPeerDatagram {
    pub(crate) id: DatagramBindingId,
    pub(crate) source: Arc<dyn DatagramBindingAuthorization>,
    pub(crate) generation: NetworkGeneration,
    pub(crate) policy: crate::runtime::udp_socket::DirectUdpPolicy,
}
pub(crate) struct PeerBoundDatagramAuthority {
    provider: Arc<HostBoundDatagramResources>,
    binding: Arc<HostPeerBinding>,
    max_bindings: NonZeroUsize,
    epochs: Mutex<Vec<AuthorizedPeerDatagram>>,
    completion: PeerBoundDatagramRegistration,
}
impl PeerBoundDatagramAuthority {
    /// Observation only: this neither authorizes another open nor closes a caller.
    pub(crate) fn registration(&self) -> PeerBoundDatagramRegistration {
        self.completion.clone()
    }
    /// One actual binding per healthy policy/network epoch. Multiple logical
    /// sessions/targets do not silently spend another provider binding slot.
    pub(crate) fn authorize(
        &self,
        source: DirectSocketSource,
        generation: NetworkGeneration,
    ) -> Result<AuthorizedPeerDatagram, ChannelError> {
        if !source.belongs_to_peer(&self.binding) {
            return Err(error(
                Reason::InvalidConfig,
                "Direct source belongs to another peer",
            ));
        }
        source.check_current(generation)?;
        let policy = source.policy_identity();
        let mut epochs = self.epochs.lock().expect("Direct epoch authority poisoned");
        epochs.retain(|epoch| epoch.source.check_current(epoch.generation).is_ok());
        if let Some(existing) = epochs.iter().find(|epoch| {
            epoch.policy == policy
                && epoch.generation == generation
                && epoch.source.preferred_port() == source.preferred_port()
        }) {
            return Ok(existing.clone());
        }
        if epochs.len() >= self.max_bindings.get() {
            return Err(error(
                Reason::Capacity,
                "actual Direct epoch authority is full",
            ));
        }
        let source: Arc<dyn DatagramBindingAuthorization> = Arc::new(source);
        let id = self.provider.authorize(source.clone(), generation)?;
        let authorized = AuthorizedPeerDatagram {
            id,
            source,
            generation,
            policy,
        };
        epochs.push(authorized.clone());
        Ok(authorized)
    }
}
struct GrantedBoundPeerResource {
    provider: Arc<HostBoundDatagramResources>,
    completion: PeerBoundDatagramRegistration,
    group: std::sync::Weak<InboundPeerTasks>,
    task: TaskId,
    inner: Box<dyn DynResourceLifecycle>,
    protocol_report: CloseReport,
    native_report: CloseReport,
    protocol_closed: bool,
    failure: Option<SharedError>,
}
impl PeerResourceContext {
    pub(crate) async fn claim_bound_outbound_datagram<T>(
        &self,
        deadline: Instant,
        max_sockets: NonZeroUsize,
        max_bindings: NonZeroUsize,
        build: impl FnOnce(
            PeerBoundDatagramGrant,
        ) -> Result<(Box<dyn DynResourceLifecycle>, T), ChannelError>,
    ) -> Result<(T, Arc<PeerBoundDatagramAuthority>), ChannelError> {
        let cancel = self.binding.cancellation();
        self.claim(deadline, |identity| {
            let declaration = ResourceDeclaration::BoundOutboundDatagram {
                max_sockets,
                max_bindings,
            };
            let provider = HostBoundDatagramResources::new(
                identity.instance,
                identity.task,
                &declaration,
                cancel,
            )?;
            let completion = PeerBoundDatagramRegistration::new(identity, self.binding.clone());
            let authority = Arc::new(PeerBoundDatagramAuthority {
                provider: provider.clone(),
                binding: self.binding.clone(),
                max_bindings,
                epochs: Mutex::new(Vec::new()),
                completion: completion.clone(),
            });
            let (inner, value) = build(PeerBoundDatagramGrant {
                identity,
                resources: provider.clone(),
            })?;
            Ok((
                erase_lifecycle(GrantedBoundPeerResource {
                    provider,
                    completion,
                    group: Arc::downgrade(&self.group),
                    task: identity.task,
                    inner,
                    protocol_report: CloseReport::default(),
                    native_report: CloseReport::default(),
                    protocol_closed: false,
                    failure: None,
                }),
                (value, authority),
            ))
        })
        .await
    }
}
impl GrantedBoundPeerResource {
    fn remember(&mut self, source: SharedError) {
        self.failure.get_or_insert(source.clone());
        if let Some(group) = self.group.upgrade() {
            group.fail(source);
        }
    }
}
impl ResourceLifecycle for GrantedBoundPeerResource {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        self.provider.activate(self.task)?;
        if let Err(error) = self.inner.start(deadline).await {
            let source = Arc::new(error);
            self.remember(source.clone());
            self.provider.revoke();
            return Err(retained(source));
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.provider.revoke();
        self.inner.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.close_joined(deadline).await
    }
}

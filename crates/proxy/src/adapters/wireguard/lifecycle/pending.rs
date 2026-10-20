//! Passive identity prepared before any replacement stack or listener starts.
mod close;
use super::super::inbound::{LinkedEndpoint, LiveInboundDevice};
use std::sync::{Arc, OnceLock};
use zero_config::ListenConfig;
use zero_engine::EngineError;

pub(in crate::adapters::wireguard) struct PendingLinkedEndpoint {
    listen: ListenConfig,
    live: Arc<LiveInboundDevice>,
    ready: OnceLock<Arc<LinkedEndpoint>>,
    resources: Arc<PeerResources>,
}
pub(in crate::adapters::wireguard) struct PeerResources {
    owned: bool,
    peer_stacks: Vec<Arc<crate::runtime::raw_ip::EndpointStackOwner>>,
    peer_carriers: Vec<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>>,
}
impl PendingLinkedEndpoint {
    pub(in crate::adapters::wireguard) fn new(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
    ) -> Self {
        Self::build(listen, live, 0, false).expect("empty passive endpoint")
    }
    /// Pure declaration of exactly the configured peers. The pending map must
    /// retain this object before any peer's owned build can be polled.
    pub(in crate::adapters::wireguard) fn with_peer_slots(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
        peers: usize,
    ) -> Result<Self, EngineError> {
        Self::build(listen, live, peers, true)
    }
    fn build(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
        peers: usize,
        owned: bool,
    ) -> Result<Self, EngineError> {
        if peers > crate::runtime::raw_ip::MAX_RAW_IP_DEVICES {
            return Err(super::invalid("linked endpoint peer owner limit exceeded"));
        }
        Ok(Self {
            listen,
            live,
            ready: OnceLock::new(),
            resources: Arc::new(PeerResources {
                owned,
                peer_stacks: (0..peers)
                    .map(|_| crate::runtime::raw_ip::EndpointStackOwner::suspended())
                    .collect(),
                peer_carriers: (0..peers)
                    .map(|_| {
                        crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot::new()
                    })
                    .collect(),
            }),
        })
    }
    pub(in crate::adapters::wireguard) fn peer_carrier(
        &self,
        peer: usize,
    ) -> Option<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>> {
        self.resources.peer_carriers.get(peer).cloned()
    }
    pub(in crate::adapters::wireguard) fn peer_stack(
        &self,
        peer: usize,
    ) -> Option<Arc<crate::runtime::raw_ip::EndpointStackOwner>> {
        self.resources.peer_stacks.get(peer).cloned()
    }
    pub(super) fn from_ready(link: Arc<LinkedEndpoint>) -> Result<Self, EngineError> {
        let pending = match link.owned_resources() {
            Some(resources) => Self {
                listen: link.listen.clone(),
                live: link.live.clone(),
                ready: OnceLock::new(),
                resources,
            },
            None => Self::new(link.listen.clone(), link.live.clone()),
        };
        pending.install(link)?;
        Ok(pending)
    }
    pub(super) fn is_usable(&self) -> bool {
        self.resources.is_usable()
    }
    pub(super) fn revoke(&self) {
        if self.resources.owned {
            for owner in &self.resources.peer_stacks {
                owner.revoke();
            }
            for owner in &self.resources.peer_carriers {
                owner.revoke();
            }
        }
    }
    pub(super) fn needs_cleanup(&self) -> bool {
        self.resources.needs_cleanup()
            || (self.resources.owned
                && self
                    .ready()
                    .is_some_and(|link| !link.carrier_owners_match(&self.resources.peer_carriers)))
    }
    pub(in crate::adapters::wireguard) async fn stop_peer_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> close::PendingPeerCloseReport {
        let link = self.ready();
        self.resources
            .stop_peer_resources(link.as_deref(), deadline)
            .await
    }
    pub(in crate::adapters::wireguard) fn listen(&self) -> &ListenConfig {
        &self.listen
    }
    pub(in crate::adapters::wireguard) fn live(&self) -> Arc<LiveInboundDevice> {
        self.live.clone()
    }
    pub(in crate::adapters::wireguard) fn ready(&self) -> Option<Arc<LinkedEndpoint>> {
        self.ready.get().cloned()
    }
    pub(super) fn install(&self, link: Arc<LinkedEndpoint>) -> Result<(), EngineError> {
        if self.ready.get().is_some() {
            return Err(super::invalid("linked endpoint already started"));
        }
        if self.listen != link.listen || !Arc::ptr_eq(&self.live(), &link.live) {
            return Err(super::invalid(
                "linked endpoint differs from its passive identity",
            ));
        }
        if self.resources.owned {
            let state = link.state.lock().unwrap_or_else(|error| error.into_inner());
            if !self.resources.is_usable()
                || state.devices.len() > self.resources.peer_stacks.len()
                || !state
                    .devices
                    .iter()
                    .zip(&self.resources.peer_stacks)
                    .all(|(device, owner)| device.is_usable() && owner.owns(device))
                || !link.carrier_owners_match(&self.resources.peer_carriers)
            {
                return Err(super::invalid(
                    "linked resources do not belong to the declared candidate owners",
                ));
            }
        }
        if self.resources.owned {
            link.install_owned_resources(self.resources.clone())?;
        }
        self.ready
            .set(link)
            .map_err(|_| super::invalid("linked endpoint already started"))
    }
}

#[cfg(test)]
mod tests;

impl PeerResources {
    fn is_usable(&self) -> bool {
        self.owned
            && self.peer_stacks.iter().all(|owner| owner.is_open())
            && self.peer_carriers.iter().all(|owner| owner.is_open())
    }
    fn needs_cleanup(&self) -> bool {
        self.owned
            && (self.peer_stacks.iter().any(|owner| owner.needs_cleanup())
                || self.peer_carriers.iter().any(|owner| owner.needs_cleanup()))
    }
}

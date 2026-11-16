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
    // A new transaction may only borrow the current actual graph. Discarding
    // that wrapper has no authority to revoke the original slot's resources.
    cleanup_owned: bool,
}
pub(in crate::adapters::wireguard) struct PeerResources {
    owned: bool,
    graph: std::sync::Mutex<std::sync::Weak<super::recipe::component::LinkedStackOptions>>,
    peer_stacks: Vec<Arc<crate::runtime::raw_ip::EndpointStackOwner>>,
    peers: usize,
    // A candidate admission fence, not a native owner or close receipt.
    cancelled: zero_platform_tokio::contract::CancellationToken,
}
impl PendingLinkedEndpoint {
    pub(super) fn client_peer_count(&self) -> usize {
        self.resources.peer_stacks.len()
    }
    pub(super) fn owns_candidate_originals(&self) -> bool {
        self.cleanup_owned
    }
    #[cfg(test)]
    pub(in crate::adapters::wireguard) fn new(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
    ) -> Self {
        Self::build(listen, live, 0, 0, false).expect("empty passive endpoint")
    }
    /// Pure declaration of exactly the configured peers. The pending map must
    /// retain this object before any peer's owned build can be polled.
    #[cfg(test)]
    pub(in crate::adapters::wireguard) fn with_peer_stacks(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
        peers: usize,
    ) -> Result<Self, EngineError> {
        Self::with_peer_layout(listen, live, peers, peers)
    }
    pub(in crate::adapters::wireguard) fn with_peer_layout(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
        stacks: usize,
        peers: usize,
    ) -> Result<Self, EngineError> {
        Self::build(listen, live, stacks, peers, true)
    }
    fn build(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
        stacks: usize,
        peers: usize,
        owned: bool,
    ) -> Result<Self, EngineError> {
        if stacks > crate::runtime::raw_ip::MAX_RAW_IP_DEVICES
            || peers > crate::runtime::raw_ip::MAX_RAW_IP_DEVICES
        {
            return Err(super::invalid("linked endpoint peer owner limit exceeded"));
        }
        Ok(Self {
            listen,
            live,
            ready: OnceLock::new(),
            resources: Arc::new(PeerResources {
                owned,
                graph: std::sync::Mutex::new(std::sync::Weak::new()),
                peer_stacks: (0..stacks)
                    .map(|_| crate::runtime::raw_ip::EndpointStackOwner::suspended())
                    .collect(),
                peers,
                cancelled: zero_platform_tokio::contract::CancellationToken::new(),
            }),
            cleanup_owned: owned,
        })
    }
    pub(super) fn cancellation(&self) -> zero_platform_tokio::contract::CancellationToken {
        self.resources.cancelled.clone()
    }
    pub(in crate::adapters::wireguard) fn peer_stack(
        &self,
        peer: usize,
    ) -> Option<Arc<crate::runtime::raw_ip::EndpointStackOwner>> {
        self.resources.peer_stacks.get(peer).cloned()
    }
    pub(super) fn same_resources(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.resources, &other.resources)
    }
    pub(super) fn bind_graph(
        &self,
        graph: &Arc<super::recipe::component::LinkedStackOptions>,
    ) -> Result<(), EngineError> {
        let mut slot = self
            .resources
            .graph
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.upgrade().is_some_and(|old| !Arc::ptr_eq(&old, graph)) {
            return Err(super::invalid(
                "peer originals already belong to a different actual graph",
            ));
        }
        *slot = Arc::downgrade(graph);
        Ok(())
    }
    pub(super) fn actual_graph(&self) -> Option<Arc<super::recipe::component::LinkedStackOptions>> {
        self.resources
            .graph
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
    }
    pub(super) fn graph_for(
        &self,
        identity: &[u8],
    ) -> Option<Arc<super::recipe::component::LinkedStackOptions>> {
        self.resources
            .graph
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
            .filter(|graph| graph.matches_identity(identity) && self.is_usable())
    }
    pub(super) fn from_ready(link: Arc<LinkedEndpoint>) -> Result<Self, EngineError> {
        let resources = link.owned_resources().ok_or_else(|| {
            super::invalid("retained linked endpoint has no actual host resource owner")
        })?;
        let pending = Self {
            listen: link.listen.clone(),
            live: link.live.clone(),
            ready: OnceLock::new(),
            resources,
            cleanup_owned: false,
        };
        pending.install(link)?;
        Ok(pending)
    }
    pub(super) fn is_usable(&self) -> bool {
        self.resources.is_usable()
    }
    pub(super) fn revoke(&self) {
        if self.cleanup_owned {
            for owner in &self.resources.peer_stacks {
                owner.revoke();
            }
            self.resources.cancelled.cancel();
        }
    }
    pub(super) fn needs_cleanup(&self) -> bool {
        self.cleanup_owned
            && (self.resources.needs_cleanup()
                || (self.resources.owned
                    && self
                        .ready()
                        .is_some_and(|link| !link.matches_declared_peers(self.resources.peers))))
    }
    pub(in crate::adapters::wireguard) async fn stop_peer_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> close::PendingPeerCloseReport {
        self.resources.stop_peer_resources(deadline).await
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
                || !link.matches_declared_peers(self.resources.peers)
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
            && !self.cancelled.is_cancelled()
            && self.peer_stacks.iter().all(|owner| owner.is_open())
    }
    fn needs_cleanup(&self) -> bool {
        self.owned && self.peer_stacks.iter().any(|owner| owner.needs_cleanup())
    }
}

#[cfg(test)]
impl PeerResources {
    pub(in crate::adapters::wireguard) fn test_actual_graph(
        &self,
    ) -> Option<Arc<super::recipe::component::LinkedStackOptions>> {
        self.graph.lock().unwrap().upgrade()
    }
}

impl PeerResources {
    pub(super) fn actual_flow_sources(
        &self,
    ) -> Result<
        Vec<(
            Arc<crate::runtime::raw_ip::SharedRawIpDevice>,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        )>,
        EngineError,
    > {
        let graph = self
            .graph
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
            .ok_or_else(|| super::invalid("current linked original graph is unavailable"))?;
        let origin = graph
            .actual_source_origin()
            .map_err(std::io::Error::other)?;
        self.peer_stacks
            .iter()
            .map(|owner| {
                // Retirement observes this same retained original even after
                // its IO admission has closed. A closed device still owns its
                // users/queues/Scope debt; it is not an unavailable owner and
                // cannot be replaced by a new or guessed healthy device.
                let device = owner.device().ok_or_else(|| {
                    super::invalid("current linked original stack is unavailable")
                })?;
                Ok((device, origin.clone()))
            })
            .collect()
    }
}

mod memory;

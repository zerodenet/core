//! Passive identity prepared before any replacement stack or listener starts.
use super::super::inbound::{LinkedEndpoint, LiveInboundDevice};
use std::sync::{Arc, OnceLock};
use zero_config::ListenConfig;
use zero_engine::EngineError;

pub(in crate::adapters::wireguard) struct PendingLinkedEndpoint {
    listen: ListenConfig,
    live: Arc<LiveInboundDevice>,
    ready: OnceLock<Arc<LinkedEndpoint>>,
}
impl PendingLinkedEndpoint {
    pub(in crate::adapters::wireguard) fn new(
        listen: ListenConfig,
        live: Arc<LiveInboundDevice>,
    ) -> Self {
        Self {
            listen,
            live,
            ready: OnceLock::new(),
        }
    }
    pub(super) fn from_ready(link: Arc<LinkedEndpoint>) -> Self {
        let pending = Self::new(link.listen.clone(), link.live.clone());
        pending.install(link).expect("fresh ready linked endpoint");
        pending
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
        if self.listen != link.listen || !Arc::ptr_eq(&self.live(), &link.live) {
            return Err(super::invalid(
                "linked endpoint differs from its passive identity",
            ));
        }
        self.ready
            .set(link)
            .map_err(|_| super::invalid("linked endpoint already started"))
    }
}

#[cfg(test)]
mod tests;

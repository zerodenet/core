//! Passive same-crypto behavior; the host checks the exact old terminal first.
use super::super::PendingEndpointMap;
use super::{
    cold::{Binding, ColdListener},
    *,
};
use crate::protocol_registry::{InboundRecovery, PreparedInboundBind};

pub(super) struct Recovery {
    tag: String,
    listen: zero_config::ListenConfig,
    live: Arc<LiveInboundDevice>,
    original: Binding,
    pending: PendingEndpointMap,
}
impl Recovery {
    pub(super) fn new(
        inbound: &InboundConfig,
        live: Arc<LiveInboundDevice>,
        original: Binding,
        pending: PendingEndpointMap,
    ) -> Result<Self, EngineError> {
        let state = Self {
            tag: inbound.tag.clone(),
            listen: inbound.listen.clone(),
            live,
            original,
            pending,
        };
        if !state.matches_private_identity(inbound)? {
            return Err(invalid(
                "WireGuard capsule differs from the actual current private identity",
            ));
        }
        Ok(state)
    }
    fn matches_private_identity(&self, inbound: &InboundConfig) -> Result<bool, EngineError> {
        let InboundProtocolConfig::Wireguard { private_key, .. } = &inbound.protocol else {
            return Ok(false);
        };
        let key = wireguard::validation::parse_key(private_key.as_str())
            .map_err(|_| invalid("invalid WireGuard recovery private identity"))?;
        Ok(self.live.matches_private_key(&key))
    }
    fn select(&self, inbound: &InboundConfig, restore: bool) -> Result<Binding, EngineError> {
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let binding = if let Some(pending) = pending.as_ref() {
            match pending.get(&self.tag) {
                Some(candidate) if candidate.listen() == &inbound.listen => {
                    Binding::Pending(candidate.clone())
                }
                Some(_) => {
                    return Err(invalid(
                        "WireGuard candidate dependency has different listen facts",
                    ))
                }
                None if restore && !matches!(self.original, Binding::Standalone) => {
                    return Err(invalid(
                        "WireGuard restoration requires its actual linked dependency",
                    ))
                }
                None => Binding::Standalone,
            }
        } else if restore {
            self.original.clone()
        } else if matches!(self.original, Binding::Standalone) {
            Binding::Standalone
        } else {
            return Err(invalid(
                "WireGuard replacement requires a cold dependency transaction",
            ));
        };
        drop(pending);
        if let Some(linked) = binding.ready()? {
            if !Arc::ptr_eq(&linked.live, &self.live) {
                return Err(invalid(
                    "WireGuard recovery dependency changed actual crypto owner",
                ));
            }
            let state = linked
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.devices.iter().any(|device| !device.is_usable()) {
                return Err(invalid(
                    "WireGuard recovery cannot revive a retired linked dependency",
                ));
            }
        }
        Ok(binding)
    }
    fn prepare(
        &self,
        inbound: &InboundConfig,
        restore: bool,
    ) -> Result<PreparedInboundBind, EngineError> {
        let binding = self.select(inbound, restore)?;
        let operation =
            ColdListener::new(inbound, self.live.clone(), binding, self.pending.clone())?;
        super::super::controlled_bind::cold_bind(inbound, Box::new(operation))
    }
}
impl InboundRecovery for Recovery {
    fn prepare_restore(
        &self,
        inbound: InboundConfig,
        _: Option<&std::path::Path>,
    ) -> Result<PreparedInboundBind, EngineError> {
        if inbound.tag != self.tag
            || inbound.listen != self.listen
            || !self.matches_private_identity(&inbound)?
        {
            return Err(invalid(
                "WireGuard recovery differs from the original private identity or listener",
            ));
        }
        self.prepare(&inbound, true)
    }
    fn prepare_replacement(
        &self,
        inbound: InboundConfig,
        _: Option<&std::path::Path>,
    ) -> Result<Option<PreparedInboundBind>, EngineError> {
        if inbound.tag != self.tag || !self.matches_private_identity(&inbound)? {
            return Ok(None);
        }
        self.prepare(&inbound, false).map(Some)
    }
}

#[cfg(test)]
mod tests;

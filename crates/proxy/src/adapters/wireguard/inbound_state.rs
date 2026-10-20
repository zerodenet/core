//! Weak passive crypto seeds; actual listener metadata selects live observations.
use super::{inbound, inbound_protocol_identity, invalid, InboundDeviceKey, WireguardAdapter};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Weak};
use zero_config::{InboundConfig, InboundProtocolConfig};
use zero_engine::EngineError;

fn key(inbound: &InboundConfig) -> Result<InboundDeviceKey, EngineError> {
    let InboundProtocolConfig::Wireguard { private_key, .. } = &inbound.protocol else {
        return Err(invalid("expected WireGuard inbound"));
    };
    // Protocol-private identity only. Neither cache keys nor secrets are logged.
    Ok((
        inbound.tag.clone(),
        Sha256::digest(private_key.as_str().as_bytes()).into(),
    ))
}

impl WireguardAdapter {
    pub(super) fn passive_inbound_live(
        &self,
        inbound: &InboundConfig,
    ) -> Result<Arc<inbound::LiveInboundDevice>, EngineError> {
        let key = key(inbound)?;
        let mut devices = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        devices.retain(|_, weak| weak.strong_count() > 0);
        if let Some(live) = devices.get(&key).and_then(Weak::upgrade) {
            // A candidate cannot mutate a running device or replace its history.
            return Ok(live);
        }
        let identity = inbound_protocol_identity(&inbound.protocol)
            .ok_or_else(|| invalid("cannot identify WireGuard inbound"))?;
        let live = Arc::new(inbound::LiveInboundDevice::new(
            inbound::prepare_device(inbound)?,
            identity,
        ));
        devices.insert(key, Arc::downgrade(&live));
        Ok(live)
    }

    pub(super) fn cache_inbound_live(
        &self,
        inbound: &InboundConfig,
        live: &Arc<inbound::LiveInboundDevice>,
    ) -> Result<(), EngineError> {
        let key = key(inbound)?;
        let mut devices = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        devices.retain(|old_key, weak| {
            weak.upgrade()
                .is_some_and(|current| old_key == &key || !Arc::ptr_eq(&current, live))
        });
        devices.insert(key, Arc::downgrade(live));
        Ok(())
    }

    pub(super) fn prune_inbound_devices(&self) {
        self.inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, weak| weak.strong_count() > 0);
    }

    pub(super) fn active_inbound_live(&self, tag: &str) -> Option<Arc<inbound::LiveInboundDevice>> {
        let mut devices = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        devices.retain(|_, weak| weak.strong_count() > 0);
        let mut found = None;
        for ((candidate, _), weak) in devices.iter() {
            if candidate != tag {
                continue;
            }
            let Some(live) = weak.upgrade().filter(|live| live.listener_active()) else {
                continue;
            };
            if found
                .as_ref()
                .is_some_and(|previous| !Arc::ptr_eq(previous, &live))
            {
                return None; // Ambiguous metadata grants neither update nor observation.
            }
            found = Some(live);
        }
        found
    }
}

#[cfg(test)]
#[path = "tests/inbound_state.rs"]
mod tests;

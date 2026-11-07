//! Query the same original maps without allocating temporary tuple keys/labels.
use super::*;
impl RawIpDevicePool {
    pub(crate) fn health_snapshot_unlabeled(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
    ) -> Option<OutboundDeviceHealthSnapshot> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let unresolved = state
            .unresolved_endpoints
            .iter()
            .any(|((name, index), failed)| {
                name == tag && *index == peer_index && *failed == identity
            });
        let original = state
            .entries
            .iter()
            .find(|((name, index), entry)| {
                name == tag && *index == peer_index && entry.identity == identity
            })
            .and_then(|(_, entry)| entry.cell.get());
        if let Some(original) = original {
            let mut facts = original.health_snapshot(String::new(), peer_index);
            facts.endpoint_resolution_failed = unresolved;
            return Some(facts);
        }
        let health = if unresolved {
            zero_api::OutboundDeviceHealthState::EndpointUnresolved
        } else if state.shutdown {
            zero_api::OutboundDeviceHealthState::Stopped
        } else {
            return None;
        };
        Some(OutboundDeviceHealthSnapshot {
            peer_index,
            state: health,
            endpoint_resolution_failed: unresolved,
            ..Default::default()
        })
    }
}

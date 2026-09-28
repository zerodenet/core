use std::time::{Duration, Instant};
use zero_api::{OutboundDeviceHealthSnapshot, OutboundDeviceHealthState};

#[derive(Default)]
pub(super) struct DeviceHealth {
    pub(super) last_handshake: Option<Instant>,
    pub(super) last_authenticated_packet: Option<Instant>,
}

impl DeviceHealth {
    pub(super) fn snapshot(
        &self,
        tag: String,
        peer_index: usize,
        closed: bool,
    ) -> OutboundDeviceHealthSnapshot {
        const FRESH: Duration = Duration::from_secs(180);
        let handshake_age = self.last_handshake.map(|time| time.elapsed());
        let packet_age = self.last_authenticated_packet.map(|time| time.elapsed());
        let state = if closed {
            OutboundDeviceHealthState::Stopped
        } else if handshake_age.is_none() {
            OutboundDeviceHealthState::AwaitingHandshake
        } else if self.last_authenticated_packet.is_some_and(|packet| {
            self.last_handshake
                .is_some_and(|handshake| packet >= handshake)
                && packet_age.is_some_and(|age| age <= FRESH)
        }) {
            OutboundDeviceHealthState::Reachable
        } else if handshake_age.is_some_and(|age| age > FRESH) {
            OutboundDeviceHealthState::Degraded
        } else {
            OutboundDeviceHealthState::RecentlyHandshaken
        };
        OutboundDeviceHealthSnapshot {
            tag,
            peer_index,
            state,
            last_handshake_age_ms: handshake_age.map(|age| age.as_millis() as u64),
            last_authenticated_packet_age_ms: packet_age.map(|age| age.as_millis() as u64),
            endpoint_resolution_failed: false,
        }
    }
}

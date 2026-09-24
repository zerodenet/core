use std::time::{Duration, Instant};

use zero_api::OutboundDeviceHealthState;

use super::DeviceHealth;

#[test]
fn authenticated_data_and_stale_handshake_have_distinct_states() {
    let now = Instant::now();
    let mut health = DeviceHealth {
        last_handshake: now.checked_sub(Duration::from_secs(181)),
        last_authenticated_packet: None,
    };
    assert_eq!(
        health.snapshot("wg".to_owned(), 0, false).state,
        OutboundDeviceHealthState::Degraded
    );

    health.last_authenticated_packet = Some(now);
    assert_eq!(
        health.snapshot("wg".to_owned(), 0, false).state,
        OutboundDeviceHealthState::Reachable
    );

    health.last_handshake = Some(Instant::now());
    assert_eq!(
        health.snapshot("wg".to_owned(), 0, false).state,
        OutboundDeviceHealthState::RecentlyHandshaken
    );
}

//! Map protocol profile names to shared congestion parameters.
use crate::settings::BbrProfile;
use zero_transport::quic::bbr::BbrParameters;
pub(super) fn parameters(profile: BbrProfile) -> BbrParameters {
    match profile {
        BbrProfile::Standard => BbrParameters::default(),
        BbrProfile::Conservative => BbrParameters {
            startup_pacing_gain: 2.25,
            startup_window_gain: 1.75,
            window_gain: 1.75,
            startup_rounds: 2,
            drain_to_target: true,
            detect_overshooting: true,
            loss_multiplier: 1,
            avoid_overestimate: true,
            reduce_ack_height_on_bandwidth_growth: true,
            ..Default::default()
        },
        BbrProfile::Aggressive => BbrParameters {
            startup_pacing_gain: 3.0,
            startup_window_gain: 2.25,
            window_gain: 2.5,
            startup_rounds: 4,
            startup_ack_aggregation: true,
            expire_startup_ack_aggregation: true,
            ..Default::default()
        },
    }
}

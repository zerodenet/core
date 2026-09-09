use super::congestion::Factory;
use crate::settings::{BbrProfile, Settings};
use quinn::congestion::ControllerFactory;
use quinn_proto::congestion::{PacketEvent, PacketKey};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[test]
fn protocol_profiles_select_shared_bbr_and_forward_packet_feedback() {
    for (profile, initial_rate) in [
        (BbrProfile::Standard, 1_384_800),
        (BbrProfile::Conservative, 1_080_000),
        (BbrProfile::Aggressive, 1_440_000),
    ] {
        let now = Instant::now();
        let mut controller = Arc::new(Factory(Settings {
            bbr_profile: profile,
            bbr_initial_window: 48_000,
            ..Default::default()
        }))
        .build(now, 1200);
        assert_eq!(controller.initial_window(), 48_000);
        assert_eq!(controller.pacing_rate(), Some(initial_rate));
        controller.on_packet_event(PacketEvent::Sent {
            key: PacketKey(2, 0),
            now,
            bytes: 1200,
            in_flight: 0,
            ack_eliciting: true,
        });
        controller.on_packet_event(PacketEvent::Acked {
            key: PacketKey(2, 0),
        });
        controller.on_packet_event(PacketEvent::FeedbackEnd {
            now: now + Duration::from_millis(100),
            in_flight: 0,
            min_rtt: Duration::from_millis(100),
        });
        assert_eq!(controller.window(), 49_200);
        assert_eq!(controller.pacing_rate(), Some(480_000));
    }
}

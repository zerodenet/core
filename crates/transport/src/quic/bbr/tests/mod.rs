use super::*;
use quinn::congestion::Controller;
use quinn_proto::congestion::PacketEvent;
mod boundaries;
mod reference;

#[test]
fn packet_feedback_samples_delivery_and_preserves_bbr_initial_pacing() {
    let now = Instant::now();
    let mut bbr = Bbr::new(BbrParameters::default(), 38_400, now, 1200);
    assert_eq!(bbr.window(), 38_400);
    assert_eq!(bbr.pacing_rate(), Some(1_107_840));
    for n in 0..32 {
        bbr.on_packet_event(PacketEvent::Sent {
            key: PacketKey(2, n),
            now,
            bytes: 1200,
            in_flight: n * 1200,
            ack_eliciting: true,
        });
    }
    for n in 0..32 {
        bbr.on_packet_event(PacketEvent::Acked {
            key: PacketKey(2, n),
        });
    }
    bbr.on_packet_event(PacketEvent::FeedbackEnd {
        now: now + Duration::from_millis(100),
        in_flight: 0,
        min_rtt: Duration::from_millis(100),
    });
    assert_eq!(bbr.bandwidth.best(), 3_072_000);
    assert_eq!(bbr.window(), 76_800);
    assert_eq!(bbr.pacing_rate(), Some(384_000));
    assert_eq!(bbr.round, 1);
}

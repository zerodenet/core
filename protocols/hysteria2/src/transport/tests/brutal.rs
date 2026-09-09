use super::*;

#[test]
fn brutal_packet_feedback_matches_pinned_official_reference_vectors() {
    // app/v2.12.2 (619a6f856b69fb7ee6a7a379e810e68b84004605):
    // core/internal/congestion/brutal/brutal.go, OnCongestionEventEx.
    for disabled in [false, true] {
        for (acked, lost, expected) in [
            (49, 0, 1_000_000),
            (40, 9, 1_000_000),
            (45, 5, 1_111_111),
            (40, 10, 1_250_000),
            (25, 25, 1_250_000),
            (50, 0, 1_000_000),
        ] {
            let now = Instant::now();
            let mut brutal = Brutal::new(now, 1200, disabled, Arc::new(AtomicU64::new(1_000_000)));
            brutal.sample(now, acked, lost);
            assert_eq!(
                brutal.pacing_rate(),
                Some(if disabled { 1_000_000 } else { expected })
            );
        }
    }
}

#[test]
fn brutal_compensates_packet_loss_and_expires_samples() {
    let now = Instant::now();
    let rate = Arc::new(AtomicU64::new(1_000_000));
    let mut brutal = Brutal::new(now, 1200, false, rate.clone());
    brutal.rtt = Duration::from_millis(100);
    assert_eq!(brutal.window(), 200_000);
    brutal.sample(now, 45, 5);
    assert_eq!(brutal.pacing_rate(), Some(1_111_111));
    assert_eq!(brutal.window(), 222_222);
    brutal.sample(now, 0, 50);
    assert_eq!(brutal.pacing_rate(), Some(1_250_000));
    brutal.sample(now + Duration::from_secs(6), 1, 0);
    assert_eq!(brutal.pacing_rate(), Some(1_000_000));
    rate.store(2_000_000, Ordering::Relaxed);
    assert_eq!(brutal.pacing_rate(), Some(2_000_000));
}
#[test]
fn brutal_can_disable_loss_compensation_without_reducing_window_on_loss() {
    let now = Instant::now();
    let mut brutal = Brutal::new(now, 1200, true, Arc::new(AtomicU64::new(1_000_000)));
    brutal.rtt = Duration::from_millis(100);
    brutal.sample(now, 50, 50);
    brutal.on_congestion_event(now, now, true, 60_000);
    assert_eq!(brutal.pacing_rate(), Some(1_000_000));
    assert_eq!(brutal.window(), 200_000);
}

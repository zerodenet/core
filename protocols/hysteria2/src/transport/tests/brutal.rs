use super::*;
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

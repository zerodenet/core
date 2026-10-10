use super::ReturnMaintenance;
use std::time::{Duration, Instant};

#[test]
fn return_cleanup_scans_twenty_times_less_often_than_protocol_ticks() {
    let now = Instant::now();
    let mut maintenance = ReturnMaintenance::new(now);
    let scans = (1..=240)
        .filter(|tick| maintenance.due(now + Duration::from_millis(tick * 250)))
        .count();
    assert_eq!(scans, 12);
    assert!(maintenance.due(now + Duration::from_secs(600)));
    assert!(!maintenance.due(now + Duration::from_secs(600)));
}

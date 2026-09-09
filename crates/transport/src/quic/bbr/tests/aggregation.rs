use super::*;

#[test]
fn higher_bandwidth_removes_previously_overestimated_ack_height() {
    let now = Instant::now();
    for reduce in [false, true] {
        let mut aggregation = Aggregation::new(BbrParameters {
            reduce_ack_height_on_bandwidth_growth: reduce,
            ..Default::default()
        });
        assert_eq!(aggregation.update(now, 1200, 80_000, false, 1), 0);
        assert_eq!(
            aggregation.update(now + Duration::from_millis(1), 2400, 80_000, false, 1),
            3590
        );
        aggregation.update(now + Duration::from_millis(100), 1200, 80_000_000, true, 2);
        assert_eq!(aggregation.best(), if reduce { 0 } else { 3590 });
    }
}

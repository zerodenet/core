use super::*;
#[test]
fn explicit_rate_is_independent_of_cwnd_and_rtt() {
    let now = Instant::now();
    for window in [20_000, 200_000, 2_000_000] {
        let mut pacer = Pacer::new(Duration::from_millis(100), window, 1200, now);
        pacer.tokens = 0;
        let wake = pacer
            .delay_with_rate(
                Duration::from_millis(100),
                1200,
                1200,
                window,
                Some(1_000_000),
                now,
            )
            .unwrap();
        assert_eq!(wake - now, Duration::from_micros(1200));
        assert!(pacer
            .delay_with_rate(
                Duration::from_millis(100),
                1200,
                1200,
                window,
                Some(1_000_000),
                wake
            )
            .is_none());
    }
}
#[test]
fn sub_byte_credit_survives_frequent_polls() {
    let now = Instant::now();
    let mut pacer = Pacer::new(Duration::from_millis(100), 20_000, 1200, now);
    pacer.tokens = 0;
    for step in 1..=20 {
        pacer.delay_with_rate(
            Duration::from_millis(100),
            1200,
            1200,
            20_000,
            Some(100_000),
            now + Duration::from_micros(step),
        );
    }
    assert!(pacer.tokens >= 1);
}

use super::*;

#[test]
fn repeated_family_failure_is_suppressed_with_a_summary_count() {
    let now = Instant::now();
    let mut limiter = FailureLogLimiter::default();
    assert_eq!(
        limiter.observe("ipv6", now),
        FailureLogDecision::Warn { suppressed: 0 }
    );
    assert_eq!(
        limiter.observe("ipv6", now + Duration::from_secs(1)),
        FailureLogDecision::Suppress
    );
    assert_eq!(
        limiter.observe("ipv6", now + WARNING_INTERVAL),
        FailureLogDecision::Warn { suppressed: 1 }
    );
}

#[test]
fn address_families_have_independent_warning_windows() {
    let now = Instant::now();
    let mut limiter = FailureLogLimiter::default();
    assert!(matches!(
        limiter.observe("ipv4", now),
        FailureLogDecision::Warn { .. }
    ));
    assert!(matches!(
        limiter.observe("ipv6", now),
        FailureLogDecision::Warn { .. }
    ));
}

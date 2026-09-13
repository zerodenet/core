use super::{expires_soon, EXPIRY_MARGIN_SECS};

#[test]
fn cached_certificate_expires_at_the_two_minute_safety_margin() {
    let now = 1_000_000;
    assert!(!expires_soon(now + EXPIRY_MARGIN_SECS + 1, now));
    assert!(expires_soon(now + EXPIRY_MARGIN_SECS, now));
    assert!(expires_soon(now - 1, now));
}

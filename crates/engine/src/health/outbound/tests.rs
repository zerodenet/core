use std::sync::{Arc, Barrier};

use super::*;

fn quarantined() -> Arc<OutboundHealth> {
    let health = Arc::new(OutboundHealth::new());
    for _ in 0..FAILURE_THRESHOLD {
        health.record_failure("node-a");
    }
    assert!(health.check("node-a").is_err());
    health
}

fn expire_cooldown(health: &OutboundHealth) {
    let mut state = health.inner.lock().unwrap();
    state.unhealthy.get_mut("node-a").unwrap().since = Instant::now() - QUARANTINE_DURATION;
}

#[test]
fn one_concurrent_half_open_attempt_recovers_on_success() {
    let health = quarantined();
    expire_cooldown(&health);
    let barrier = Arc::new(Barrier::new(16));
    let threads = (0..16)
        .map(|_| {
            let health = Arc::clone(&health);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                health.begin("node-a").ok()
            })
        })
        .collect::<Vec<_>>();
    let mut attempts = threads
        .into_iter()
        .filter_map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(attempts.len(), 1);
    assert!(health.check("node-a").is_err());
    attempts.pop().unwrap().succeeded();
    assert!(health.check("node-a").is_ok());
}

#[test]
fn failed_half_open_attempt_restarts_quarantine_immediately() {
    let health = quarantined();
    expire_cooldown(&health);
    health.begin("node-a").unwrap().failed();
    assert!(health.check("node-a").is_err());
}

#[test]
fn abandoned_half_open_attempt_releases_slot_and_restarts_cooldown() {
    let health = quarantined();
    expire_cooldown(&health);
    drop(health.begin("node-a").unwrap());
    assert!(health.check("node-a").is_err());
    expire_cooldown(&health);
    health.begin("node-a").unwrap().neutral();
    assert!(health.check("node-a").is_err());
}

#[test]
fn stale_half_open_failure_does_not_undo_another_success() {
    let health = quarantined();
    expire_cooldown(&health);
    let attempt = health.begin("node-a").unwrap();
    health.record_success("node-a");
    attempt.failed();
    assert!(health.check("node-a").is_ok());
}

#[test]
fn stale_half_open_result_does_not_finish_a_new_attempt() {
    let health = quarantined();
    expire_cooldown(&health);
    let stale = health.begin("node-a").unwrap();
    health.record_success("node-a");
    for _ in 0..FAILURE_THRESHOLD {
        health.record_failure("node-a");
    }
    expire_cooldown(&health);
    let current = health.begin("node-a").unwrap();
    stale.failed();
    assert!(health.check("node-a").is_err());
    current.succeeded();
    assert!(health.check("node-a").is_ok());
}

#[test]
fn stale_half_open_success_does_not_clear_a_new_quarantine() {
    let health = quarantined();
    expire_cooldown(&health);
    let stale = health.begin("node-a").unwrap();
    health.record_success("node-a");
    for _ in 0..FAILURE_THRESHOLD {
        health.record_failure("node-a");
    }
    expire_cooldown(&health);
    let current = health.begin("node-a").unwrap();
    stale.succeeded();
    assert!(health.check("node-a").is_err());
    current.failed();
    assert!(health.check("node-a").is_err());
}

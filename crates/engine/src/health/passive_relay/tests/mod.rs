use super::*;

fn key(port: u16) -> PassiveRelayHealthKey {
    PassiveRelayHealthKey {
        policy_tag: "hk".to_owned(),
        member_tag: "hk-ss-1".to_owned(),
        target: Address::Domain("landing.example".to_owned()),
        port,
    }
}

#[test]
fn quarantines_only_after_failure_threshold_and_ratio() {
    let health = PassiveRelayHealth::default();
    let key = key(443);
    let now = Instant::now();

    health.record_at(key.clone(), PassiveRelayOutcome::Success, false, now);
    health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    assert_eq!(health.allow_flow_at(&key, now), Some(false));

    assert!(matches!(
        health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now),
        Some(PassiveRelayHealthTransition::Quarantined(_))
    ));
    assert_eq!(health.allow_flow_at(&key, now), None);
}

#[test]
fn scopes_quarantine_to_target_port() {
    let health = PassiveRelayHealth::default();
    let blocked = key(14788);
    let unaffected = key(14688);
    let now = Instant::now();

    for _ in 0..MIN_FAILURES {
        health.record_at(blocked.clone(), PassiveRelayOutcome::Failure, false, now);
    }

    assert_eq!(health.allow_flow_at(&blocked, now), None);
    assert_eq!(health.allow_flow_at(&unaffected, now), Some(false));
}

#[test]
fn permits_one_half_open_flow_and_recovers_on_success() {
    let health = PassiveRelayHealth::default();
    let key = key(443);
    let now = Instant::now();
    for _ in 0..MIN_FAILURES {
        health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    }

    let after_quarantine = now + INITIAL_QUARANTINE;
    assert_eq!(health.allow_flow_at(&key, after_quarantine), Some(true));
    assert_eq!(health.allow_flow_at(&key, after_quarantine), None);

    let transition = health.record_at(
        key.clone(),
        PassiveRelayOutcome::Success,
        true,
        after_quarantine,
    );
    assert_eq!(transition, Some(PassiveRelayHealthTransition::Healthy));
    assert_eq!(health.allow_flow_at(&key, after_quarantine), Some(false));
}

#[test]
fn half_open_failure_doubles_quarantine() {
    let health = PassiveRelayHealth::default();
    let key = key(443);
    let now = Instant::now();
    for _ in 0..MIN_FAILURES {
        health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    }

    let half_open_at = now + INITIAL_QUARANTINE;
    assert_eq!(health.allow_flow_at(&key, half_open_at), Some(true));
    let transition = health.record_at(
        key.clone(),
        PassiveRelayOutcome::Failure,
        true,
        half_open_at,
    );
    assert_eq!(
        transition,
        Some(PassiveRelayHealthTransition::Quarantined(
            INITIAL_QUARANTINE * 2
        ))
    );

    assert_eq!(
        health.allow_flow_at(&key, half_open_at + INITIAL_QUARANTINE),
        None
    );
    assert_eq!(
        health.allow_flow_at(&key, half_open_at + INITIAL_QUARANTINE * 2),
        Some(true)
    );
}

#[test]
fn in_flight_failures_do_not_extend_an_active_quarantine() {
    let health = PassiveRelayHealth::default();
    let key = key(443);
    let now = Instant::now();
    for _ in 0..MIN_FAILURES {
        health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    }

    assert_eq!(
        health.record_at(
            key.clone(),
            PassiveRelayOutcome::Failure,
            false,
            now + Duration::from_secs(10),
        ),
        None
    );
    assert_eq!(
        health.allow_flow_at(&key, now + INITIAL_QUARANTINE),
        Some(true)
    );
}

#[test]
fn neutral_half_open_outcome_releases_probe_and_keeps_quarantine() {
    let health = PassiveRelayHealth::default();
    let key = key(443);
    let now = Instant::now();
    for _ in 0..MIN_FAILURES {
        health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    }

    let half_open_at = now + INITIAL_QUARANTINE;
    assert_eq!(health.allow_flow_at(&key, half_open_at), Some(true));
    assert_eq!(
        health.record_at(
            key.clone(),
            PassiveRelayOutcome::Neutral,
            true,
            half_open_at,
        ),
        None
    );
    assert_eq!(health.allow_flow_at(&key, half_open_at), None);
    assert_eq!(
        health.allow_flow_at(&key, half_open_at + INITIAL_QUARANTINE),
        Some(true)
    );
}

#[test]
fn cleanup_removes_expired_healthy_and_observation_only_entries() {
    let health = PassiveRelayHealth::default();
    let now = Instant::now();
    for port in 10_000..11_000 {
        health.record_at(key(port), PassiveRelayOutcome::Success, false, now);
    }
    assert_eq!(health.entry_count(), 1_000);

    health.cleanup_at(now + OBSERVATION_WINDOW + Duration::from_millis(1));
    assert_eq!(health.entry_count(), 0);
}

#[test]
fn cleanup_keeps_quarantine_and_half_open_state() {
    let health = PassiveRelayHealth::default();
    let key = key(443);
    let now = Instant::now();
    for _ in 0..MIN_FAILURES {
        health.record_at(key.clone(), PassiveRelayOutcome::Failure, false, now);
    }
    let half_open_at = now + INITIAL_QUARANTINE;
    assert_eq!(health.allow_flow_at(&key, half_open_at), Some(true));

    health.cleanup_at(now + OBSERVATION_WINDOW + Duration::from_millis(1));
    assert_eq!(health.entry_count(), 1);
    assert_eq!(
        health.allow_flow_at(&key, now + OBSERVATION_WINDOW + Duration::from_millis(1)),
        None
    );
}

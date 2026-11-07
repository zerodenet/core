use super::super::legacy_replay::LegacyReplay;
use super::*;
use crate::validation::ReplayPolicy;

#[test]
fn actual_original_filters_charge_fixed_server_and_client_bitmap_sizes_once() {
    let server_bytes = bitmap_bytes(true);
    let client_bytes = bitmap_bytes(false);
    assert_eq!(server_bytes, 3_594_400);
    assert_eq!(client_bytes, 89_864);
    let budget = LegacyReplayBudget::new(Some(server_bytes + client_bytes)).unwrap();
    let server = LegacyReplay::new(ReplayPolicy::Reject, true)
        .with_bloom_budget(budget.clone())
        .unwrap();
    let client = LegacyReplay::new(ReplayPolicy::Reject, false)
        .with_bloom_budget(budget.clone())
        .unwrap();
    assert_eq!(budget.retained(), 0, "cold originals allocate no bitmap");
    server.check(b"accepted-server-salt").unwrap();
    assert_eq!(budget.retained(), server_bytes);
    let alias = server.clone().with_policy(ReplayPolicy::Detect);
    alias.check(b"accepted-server-salt").unwrap();
    assert!(server.check(b"accepted-server-salt").is_err());
    assert_eq!(
        budget.retained(),
        server_bytes,
        "same membership/rotation book is not recharged per view"
    );
    assert_eq!(client.generate(16).unwrap().len(), 16);
    assert_eq!(budget.retained(), server_bytes + client_bytes);
    let another = LegacyReplay::new(ReplayPolicy::Reject, false)
        .with_bloom_budget(budget.clone())
        .unwrap();
    assert!(another.generate(16).is_err());
    drop(server);
    drop(alias);
    drop(client);
    assert_eq!(
        budget.retained(),
        server_bytes + client_bytes,
        "dropping consumer views cannot erase valid original Bloom membership"
    );
    assert!(
        another.generate(16).is_err(),
        "pressure cannot discard retired valid history"
    );
    assert!(budget
        .validate(Some(server_bytes + client_bytes - 1))
        .is_err());
}

#[test]
fn default_ignore_detect_and_reject_keep_their_original_legacy_rules() {
    let bytes = bitmap_bytes(true);
    let budget = LegacyReplayBudget::new(Some(bytes)).unwrap();
    let default = LegacyReplay::new(ReplayPolicy::Default, true)
        .with_bloom_budget(budget.clone())
        .unwrap();
    let ignored = default.clone().with_policy(ReplayPolicy::Ignore);
    default.check(b"same").unwrap();
    ignored.check(b"same").unwrap();
    ignored.generate(16).unwrap();
    assert_eq!(
        budget.retained(),
        0,
        "these original paths do not allocate a membership filter"
    );
    let detected = default.clone().with_policy(ReplayPolicy::Detect);
    detected.check(b"same").unwrap();
    detected.check(b"same").unwrap();
    let rejected = default.clone().with_policy(ReplayPolicy::Reject);
    assert!(
        rejected.check(b"same").is_err(),
        "policy views share the same original membership"
    );
    assert_eq!(budget.retained(), bytes);
}

#[test]
fn byte_policy_validation_does_not_install_and_never_forks_a_live_book() {
    let bytes = bitmap_bytes(false);
    let budget = LegacyReplayBudget::new(Some(bytes)).unwrap();
    let original = LegacyReplay::new(ReplayPolicy::Reject, false)
        .with_bloom_budget(budget.clone())
        .unwrap();
    original.generate(16).unwrap();
    assert!(original
        .clone()
        .with_bloom_budget(LegacyReplayBudget::new(None).unwrap())
        .is_err());
    budget.validate(Some(bytes * 2)).unwrap();
    let second = LegacyReplay::new(ReplayPolicy::Reject, false)
        .with_bloom_budget(budget.clone())
        .unwrap();
    assert!(
        second.generate(16).is_err(),
        "cold growth has no installation authority"
    );
    budget.install(bytes * 2).unwrap();
    second.generate(16).unwrap();
    assert_eq!(budget.retained(), bytes * 2);
    assert!(budget.install(bytes).is_err());
    assert_eq!(budget.retained(), bytes * 2);
}

#[test]
fn original_client_rotation_keeps_both_generations_without_changing_its_bitmap_charge() {
    let bytes = bitmap_bytes(false);
    let budget = LegacyReplayBudget::new(Some(bytes)).unwrap();
    let client = LegacyReplay::new(ReplayPolicy::Reject, false)
        .with_bloom_budget(budget.clone())
        .unwrap();
    let first = client.generate(16).unwrap();
    for _ in 1..5_000 {
        client.generate(16).unwrap();
    }
    assert!(client.check(&first).is_err());
    for _ in 0..5_000 {
        client.generate(16).unwrap();
    }
    assert!(
        client.check(&first).is_err(),
        "the reference retains the preceding full generation"
    );
    client.generate(16).unwrap();
    client.check(&first).unwrap();
    assert_eq!(
        budget.retained(),
        bytes,
        "natural reference rotation clears/reuses original bitmap allocation"
    );
}

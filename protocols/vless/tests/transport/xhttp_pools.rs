use super::*;

#[test]
fn prepared_relay_access_does_not_retain_pool_registry() {
    let owner = Arc::new(Pools::default());
    let weak = Arc::downgrade(&owner);
    let access = owner.access();
    let pool = access.pool("final", "relay-a", Default::default());
    drop(owner);
    assert!(weak.upgrade().is_none());
    // A finishing old flow can still obtain a private pool after shutdown.
    let finishing = access.pool("final", "relay-a", Default::default());
    drop((pool, finishing, access));
}

#[test]
fn reload_prevents_old_prepared_flows_from_repopulating_current_pool_registry() {
    let owner = Arc::new(Pools::default());
    let old = owner.access();
    let active = old.pool("final", "relay-a", Default::default());
    assert_eq!(owner.0.lock().unwrap().pools.len(), 1);
    owner.retire();
    let finishing = old.pool("final", "relay-a", Default::default());
    assert!(owner.0.lock().unwrap().pools.is_empty());
    let current = owner.access().pool("final", "relay-a", Default::default());
    assert_eq!(owner.0.lock().unwrap().pools.len(), 1);
    drop((active, finishing, current));
}

#[test]
fn pool_identity_separates_tags_and_relay_paths_without_delimiter_ambiguity() {
    let owner = Arc::new(Pools::default());
    let access = owner.access();
    for (tag, path) in [("a", "bc"), ("ab", "c"), ("a", "bd"), ("a", "bc")] {
        access.pool(tag, path, Default::default());
    }
    assert_eq!(owner.0.lock().unwrap().pools.len(), 3);
}

#[test]
fn retained_admission_observes_actual_owner_retirement_and_drop() {
    let owner = Arc::new(Pools::default());
    let first = owner.access();
    first.check_current().unwrap();
    owner.retire();
    assert!(first.check_current().is_err());
    let second = owner.access();
    second.check_current().unwrap();
    owner.retire();
    let third = owner.access();
    third.check_current().unwrap();
    assert!(first.check_current().is_err());
    assert!(second.check_current().is_err());
    drop(owner);
    assert!(third.check_current().is_err());
}

#[test]
fn retained_access_has_no_default_authority_or_epoch_history() {
    assert!(PoolAccess::default().check_current().is_err());
    let owner = Arc::new(Pools::default());
    let access = owner.access();
    assert_eq!(access.generation.strong_count(), 1);
    owner.retire();
    assert_eq!(access.generation.strong_count(), 0);
    assert!(access.owner.upgrade().is_some());
    assert!(access.check_current().is_err());
}

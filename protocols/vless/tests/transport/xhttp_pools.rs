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

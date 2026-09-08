use super::RouteLease;

#[test]
fn dropping_owner_releases_lease_while_a_duplicate_descriptor_survives() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("routes.json");
    let owner = RouteLease::acquire_at(path.clone(), "first").unwrap();
    let duplicate = owner._lock.try_clone().unwrap();

    assert!(RouteLease::acquire_at(path.clone(), "second").is_err());
    drop(owner);
    let next = RouteLease::acquire_at(path.clone(), "second").unwrap();
    drop(duplicate);
    assert!(RouteLease::acquire_at(path.clone(), "third").is_err());
    drop(next);
    RouteLease::acquire_at(path, "third").unwrap();
}

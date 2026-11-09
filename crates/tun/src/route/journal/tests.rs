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

#[cfg(target_os = "macos")]
#[test]
fn owned_route_journal_keeps_same_source_file_custody_after_tls_returns_until_explicit_close() {
    use super::RouteJournal;
    use crate::{
        macos_privilege::session::MacosHelperOwner, packet_memory::PacketReturnTotals,
        route::RouteInterface,
    };
    let root = std::sync::Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, 65536).unwrap();
    let route_duty = owner.lease_routes().unwrap();
    let context = owner.enter().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.json");
    let lease = RouteLease::acquire_at(path.clone(), "owned-route").unwrap();
    let mut journal = RouteJournal::new(
        lease,
        "utun-fixture",
        false,
        7,
        RouteInterface::new("en0".to_owned(), 9).unwrap(),
        None,
    )
    .unwrap();
    journal.record_route("0.0.0.0/1").unwrap();
    drop(context);
    // This retains the actual Source even when the call's TLS has returned;
    // it must not silently switch to library std::fs material acquisition.
    journal
        .record_exclusion("192.0.2.1".parse().unwrap())
        .unwrap();
    owner.stop();
    assert!(owner.require_closed().is_err());
    assert!(!journal.owned_closed());
    journal.cleanup(|_| Ok(()), |_| Ok(())).unwrap();
    journal.close_owned().unwrap();
    assert!(journal.owned_closed());
    route_duty.finish_cleanup().unwrap();
    owner.require_closed().unwrap();
    assert!(!path.exists());
    // Receipt idempotence does not reenter the now-closed file capability.
    journal.close_owned().unwrap();
}

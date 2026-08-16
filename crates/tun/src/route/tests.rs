use std::net::{IpAddr, Ipv4Addr};

use super::{RouteInterface, RouteJournal, RouteLease};

fn journal(path: std::path::PathBuf) -> RouteJournal {
    RouteJournal {
        tun_name: "test-tun".to_owned(),
        ipv6: false,
        tun_index: 7,
        egress: RouteInterface::new("physical0".to_owned(), 9).unwrap(),
        gateway: Some("192.0.2.1".to_owned()),
        excluded: Vec::new(),
        installed: Vec::new(),
        path,
        _lease: None,
    }
}

#[test]
fn recovery_journal_persists_installed_routes_and_exclusions() {
    let directory = tempfile::tempdir().expect("temporary journal directory");
    let path = directory.path().join("routes.json");
    let mut journal = journal(path.clone());
    let excluded = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10));

    journal.record_exclusion(excluded).unwrap();
    journal.record_route("0.0.0.0/1").unwrap();

    let recovered: RouteJournal = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(recovered.excluded, vec![excluded]);
    assert_eq!(recovered.installed, vec!["0.0.0.0/1"]);
    assert_eq!(recovered.egress.name(), "physical0");
    assert_eq!(recovered.gateway.as_deref(), Some("192.0.2.1"));
}

#[test]
fn journal_updates_reconciled_egress_and_forgets_removed_exclusions() {
    let directory = tempfile::tempdir().expect("temporary journal directory");
    let path = directory.path().join("routes.json");
    let mut journal = journal(path.clone());
    let excluded = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10));
    journal.record_route("0.0.0.0/1").unwrap();
    journal.record_exclusion(excluded).unwrap();
    journal
        .replace_egress(
            RouteInterface::new("physical1".to_owned(), 10).unwrap(),
            Some("192.0.2.2".to_owned()),
        )
        .unwrap();
    journal.forget_exclusion(excluded).unwrap();

    let recovered: RouteJournal = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(recovered.egress.name(), "physical1");
    assert_eq!(recovered.gateway.as_deref(), Some("192.0.2.2"));
    assert!(recovered.excluded.is_empty());
    assert_eq!(recovered.installed, vec!["0.0.0.0/1"]);
}

#[test]
fn recovery_journal_accepts_legacy_entries_without_gateway() {
    let recovered: RouteJournal = serde_json::from_value(serde_json::json!({
        "tun_name": "legacy-tun",
        "ipv6": false,
        "tun_index": 7,
        "egress": {"name": "physical0", "index": 9},
        "excluded": ["192.0.2.10"],
        "installed": ["0.0.0.0/1"]
    }))
    .expect("deserialize legacy route journal");
    assert!(recovered.gateway.is_none());
}

#[test]
fn failed_cleanup_keeps_only_unremoved_items_for_next_start() {
    let directory = tempfile::tempdir().expect("temporary journal directory");
    let path = directory.path().join("routes.json");
    let mut journal = journal(path.clone());
    let excluded = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
    journal.record_exclusion(excluded).unwrap();
    journal.record_route("0.0.0.0/1").unwrap();

    let error = journal
        .cleanup(|_| Err(std::io::Error::other("route busy")), |_| Ok(()))
        .expect_err("failed route removal must be reported");
    assert_eq!(error.to_string(), "route busy");

    let retained: RouteJournal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(retained.installed, vec!["0.0.0.0/1"]);
    assert!(retained.excluded.is_empty());

    journal.cleanup(|_| Ok(()), |_| Ok(())).unwrap();
    assert!(!path.exists());
}

#[test]
fn route_lease_serializes_same_device_transactions() {
    let directory = tempfile::tempdir().expect("temporary lease directory");
    let path = directory.path().join("routes.json");
    let first = RouteLease::acquire_at(path.clone(), "test-tun").expect("first lease");

    let error = RouteLease::acquire_at(path.clone(), "test-tun")
        .expect_err("second route transaction must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);

    drop(first);
    RouteLease::acquire_at(path, "test-tun").expect("lease should release on drop");
}

#[test]
fn recovery_key_can_load_a_journal_from_a_renumbered_dynamic_device() {
    let directory = tempfile::tempdir().expect("temporary journal directory");
    let path = directory.path().join("routes-stable-tag-v4.json");
    let lease = RouteLease::acquire_at(path.clone(), "stable-tag").expect("initial lease");
    let mut journal = RouteJournal::new(
        lease,
        "utun8",
        false,
        8,
        RouteInterface::new("en0".to_owned(), 4).unwrap(),
        Some("192.0.2.1".to_owned()),
    )
    .unwrap();
    journal.record_route("0.0.0.0/1").unwrap();
    drop(journal);

    let lease = RouteLease::acquire_at(path, "stable-tag").expect("recovery lease");
    let recovered = RouteJournal::load(&lease, false)
        .unwrap()
        .expect("recovery journal");
    assert_eq!(recovered.tun_name, "utun8");
    assert_eq!(recovered.installed, vec!["0.0.0.0/1"]);
}

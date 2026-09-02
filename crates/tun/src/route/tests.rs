use std::net::{IpAddr, Ipv4Addr};

use super::{
    capture_route_prefixes, capture_route_prefixes_with_exclusions, family_exclusions_for_egress,
    EgressUnavailableReason, FamilyEgressState, RouteInterface, RouteJournal, RouteLease,
};

#[test]
fn capture_plan_defaults_to_split_routes_and_filters_explicit_families() {
    assert_eq!(
        capture_route_prefixes("10.66.0.1".parse().unwrap(), &[]),
        vec!["0.0.0.0/1".parse().unwrap(), "128.0.0.0/1".parse().unwrap()]
    );
    assert_eq!(
        capture_route_prefixes(
            "fd66::1".parse().unwrap(),
            &[
                "203.0.113.0/24".parse().unwrap(),
                "2001:db8::/32".parse().unwrap(),
            ],
        ),
        vec!["2001:db8::/32".parse().unwrap()]
    );
}

#[test]
fn capture_plan_subtracts_exclusions_for_both_address_families() {
    let ipv4 = capture_route_prefixes_with_exclusions(
        "10.66.0.1".parse().unwrap(),
        &["10.0.0.0/8".parse().unwrap()],
        &["10.64.0.0/10".parse().unwrap()],
    );
    assert_eq!(
        ipv4,
        vec![
            "10.0.0.0/10".parse().unwrap(),
            "10.128.0.0/9".parse().unwrap()
        ]
    );

    let ipv6 = capture_route_prefixes_with_exclusions(
        "fd66::1".parse().unwrap(),
        &["2001:db8::/32".parse().unwrap()],
        &["2001:db8:8000::/33".parse().unwrap()],
    );
    assert_eq!(ipv6, vec!["2001:db8::/33".parse().unwrap()]);
}

#[test]
fn default_capture_excludes_only_the_requested_ipv4_cidr() {
    let routes = capture_route_prefixes_with_exclusions(
        "10.66.0.1".parse().unwrap(),
        &[],
        &["16.0.0.0/8".parse().unwrap()],
    );
    assert_eq!(
        routes,
        vec![
            "0.0.0.0/4".parse().unwrap(),
            "17.0.0.0/8".parse().unwrap(),
            "18.0.0.0/7".parse().unwrap(),
            "20.0.0.0/6".parse().unwrap(),
            "24.0.0.0/5".parse().unwrap(),
            "32.0.0.0/3".parse().unwrap(),
            "64.0.0.0/2".parse().unwrap(),
            "128.0.0.0/1".parse().unwrap(),
        ]
    );
    let excluded: IpAddr = "16.0.0.1".parse().unwrap();
    let stun_server: IpAddr = "20.93.239.169".parse().unwrap();
    assert!(!routes.iter().any(|route| route.contains(&excluded)));
    assert!(routes.iter().any(|route| route.contains(&stun_server)));
}

#[test]
fn broader_and_unrelated_exclusions_are_deterministic() {
    assert!(capture_route_prefixes_with_exclusions(
        "10.66.0.1".parse().unwrap(),
        &["10.0.0.0/8".parse().unwrap()],
        &["0.0.0.0/0".parse().unwrap()],
    )
    .is_empty());
    assert_eq!(
        capture_route_prefixes_with_exclusions(
            "10.66.0.1".parse().unwrap(),
            &["10.0.0.0/8".parse().unwrap()],
            &["192.0.2.0/24".parse().unwrap()],
        ),
        vec!["10.0.0.0/8".parse().unwrap()]
    );
}

fn journal(path: std::path::PathBuf) -> RouteJournal {
    RouteJournal {
        tun_name: "test-tun".to_owned(),
        ipv6: false,
        tun_index: 7,
        egress: RouteInterface::new("physical0".to_owned(), 9).unwrap(),
        gateway: Some("192.0.2.1".to_owned()),
        excluded: Vec::new(),
        installed: Vec::new(),
        scoped_bypass: false,
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
    assert!(!recovered.scoped_bypass);
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
    assert!(!recovered.scoped_bypass);
}

#[test]
fn host_exclusions_require_native_family_egress() {
    let excluded = [
        "2001:db8::53".parse().unwrap(),
        "192.0.2.53".parse().unwrap(),
    ];
    let available =
        FamilyEgressState::Available(RouteInterface::new("physical6".to_owned(), 6).unwrap());
    assert_eq!(
        family_exclusions_for_egress(&excluded, true, &available),
        vec!["2001:db8::53".parse::<IpAddr>().unwrap()]
    );

    let unavailable = FamilyEgressState::Unavailable(EgressUnavailableReason::NoDefaultRoute);
    assert!(family_exclusions_for_egress(&excluded, true, &unavailable).is_empty());
    assert!(family_exclusions_for_egress(&excluded, true, &FamilyEgressState::Unknown).is_empty());
}

#[test]
fn journal_retains_a_recorded_scoped_bypass_until_it_is_forgotten() {
    let directory = tempfile::tempdir().expect("temporary journal directory");
    let path = directory.path().join("routes.json");
    let mut journal = journal(path.clone());

    journal.record_scoped_bypass().unwrap();
    journal.cleanup(|_| Ok(()), |_| Ok(())).unwrap();

    let retained: RouteJournal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(retained.scoped_bypass);

    journal.forget_scoped_bypass().unwrap();
    assert!(!path.exists());
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
fn route_lease_serializes_different_instances_per_address_family() {
    let directory = tempfile::tempdir().expect("temporary lease directory");
    let lock_path = directory.path().join("routes-v4.owner.lock");
    let owner_path = lock_path.with_extension("");
    let first = RouteLease::acquire_paths(
        directory.path().join("routes-first-v4.json"),
        lock_path.clone(),
        "first-instance",
    )
    .expect("first instance lease");

    let error = RouteLease::acquire_paths(
        directory.path().join("routes-second-v4.json"),
        lock_path.clone(),
        "second-instance",
    )
    .expect_err("a different tag must not own the same family concurrently");
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(error.to_string().contains("active owner `first-instance`"));
    assert_eq!(
        std::fs::read_to_string(&owner_path).unwrap(),
        "first-instance"
    );

    drop(first);
    assert!(!owner_path.exists());
    RouteLease::acquire_paths(
        directory.path().join("routes-second-v4.json"),
        lock_path,
        "second-instance",
    )
    .expect("next instance takes ownership after release");
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

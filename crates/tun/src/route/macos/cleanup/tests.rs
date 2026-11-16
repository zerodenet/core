use super::*;
use std::cell::RefCell;

fn polluted(interface: &str, gateway: &str) -> table::RouteEntry {
    table::RouteEntry {
        prefix: "10.19.7.2/32".parse().unwrap(),
        gateway: gateway.into(),
        interface: interface.into(),
        flags: "UGHS".into(),
    }
}
#[test]
fn polluted_peer_exclusion_is_removed_only_with_its_recorded_route_identity() {
    let os = RefCell::new(vec![
        polluted("en5", "192.0.2.1"),
        polluted("utun9", "10.77.0.1"),
    ]);
    // A foreign competing route blocks cleanup before any mutation.
    let result = remove_with(
        "10.19.7.2/32".parse().unwrap(),
        "en5",
        Some("192.0.2.1"),
        false,
        || Ok(os.borrow().clone()),
        || panic!("must preserve the other VPN"),
    );
    assert!(result.is_err());
    os.borrow_mut().pop();
    remove_with(
        "10.19.7.2/32".parse().unwrap(),
        "en5",
        Some("192.0.2.1"),
        false,
        || Ok(os.borrow().clone()),
        || {
            os.borrow_mut().clear();
            Ok(())
        },
    )
    .unwrap();
    assert!(os.borrow().is_empty());
}
#[test]
fn changed_gateway_absent_routes_and_unconfirmed_deletion_do_not_claim_ownership() {
    let prefix = "10.19.7.2/32".parse().unwrap();
    assert!(remove_with(
        prefix,
        "en5",
        Some("192.0.2.1"),
        false,
        || Ok(vec![polluted("en5", "192.0.2.254")]),
        || panic!("foreign gateway")
    )
    .is_err());
    remove_with(
        prefix,
        "en5",
        Some("192.0.2.1"),
        false,
        || Ok(vec![]),
        || panic!("already absent"),
    )
    .unwrap();
    assert!(remove_with(
        prefix,
        "en5",
        Some("192.0.2.1"),
        false,
        || Ok(vec![polluted("en5", "192.0.2.1")]),
        || Ok(())
    )
    .is_err());
}

#[test]
fn original_polluted_journal_survives_failed_cleanup_and_is_cleared_only_after_readback() {
    use crate::route::journal::{RouteJournal, RouteLease};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("routes.json");
    let peer = "10.19.7.2".parse().unwrap();
    let lease = RouteLease::acquire_at(path.clone(), "znet-tun").unwrap();
    let mut journal = RouteJournal::new(
        lease,
        "utun7",
        false,
        0,
        RouteInterface::new("en5".into(), 5).unwrap(),
        Some("192.0.2.1".into()),
    )
    .unwrap();
    journal.record_exclusion(peer).unwrap();
    drop(journal); // Crash persisted this exclusion; no native cleanup happened.
    let lease = RouteLease::acquire_at(path.clone(), "znet-tun").unwrap();
    let mut journal = RouteJournal::load(&lease, false).unwrap().unwrap();
    let os = RefCell::new(vec![polluted("en5", "192.0.2.254")]);
    let cleanup = |journal: &mut RouteJournal, delete: bool| {
        journal.cleanup(
            |_| panic!("only the polluted host exclusion was recorded"),
            |ip| {
                remove_with(
                    IpNet::new(ip, 32).unwrap(),
                    "en5",
                    Some("192.0.2.1"),
                    false,
                    || Ok(os.borrow().clone()),
                    || {
                        if delete {
                            os.borrow_mut().clear();
                        }
                        Ok(())
                    },
                )
            },
        )
    };
    assert!(cleanup(&mut journal, true).is_err());
    assert_eq!(journal.excluded, vec![peer]);
    assert_eq!(os.borrow()[0].gateway, "192.0.2.254");
    os.borrow_mut()[0].gateway = "192.0.2.1".into();
    assert!(cleanup(&mut journal, false).is_err()); // No successful readback.
    assert_eq!(
        RouteJournal::load(&lease, false).unwrap().unwrap().excluded,
        vec![peer]
    );
    cleanup(&mut journal, true).unwrap();
    assert!(!path.exists());
    assert!(os.borrow().is_empty());
}

#[test]
fn reused_interface_name_cannot_retire_the_original_journal() {
    use crate::route::journal::{RouteJournal, RouteLease};
    use std::cell::Cell;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("routes.json");
    let lease = RouteLease::acquire_at(path.clone(), "znet-tun").unwrap();
    let mut journal = RouteJournal::new(
        lease,
        "utun7",
        false,
        7,
        RouteInterface::new("en5".into(), 5).unwrap(),
        Some("192.0.2.1".into()),
    )
    .unwrap();
    let peer = "10.19.7.2".parse().unwrap();
    journal.record_exclusion(peer).unwrap();
    let original = journal.egress.clone();
    let observed_index = Cell::new(6); // Same name/gateway, another interface generation.
    let os = RefCell::new(vec![polluted("en5", "192.0.2.1")]);
    let cleanup = |journal: &mut RouteJournal| {
        journal.cleanup(
            |_| panic!("no captures"),
            |ip| {
                remove_with(
                    IpNet::new(ip, 32).unwrap(),
                    original.name(),
                    Some("192.0.2.1"),
                    false,
                    || Ok(os.borrow().clone()),
                    || {
                        require_index(original.name(), original.index(), observed_index.get())?;
                        os.borrow_mut().clear();
                        Ok(())
                    },
                )
            },
        )
    };
    assert!(cleanup(&mut journal).is_err());
    assert_eq!(journal.excluded, [peer]);
    assert_eq!(os.borrow().len(), 1);
    let retained: RouteJournal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(retained.egress.index(), 5);
    assert_eq!(retained.excluded, [peer]);
    observed_index.set(5);
    cleanup(&mut journal).unwrap();
    assert!(!path.exists());
}

#[test]
fn legacy_unknown_identity_and_changed_capture_gateway_are_never_deleted() {
    let prefix = "10.19.7.2/32".parse().unwrap();
    let os = RefCell::new(vec![table::RouteEntry {
        prefix,
        interface: "utun7".into(),
        gateway: "link#7".into(),
        flags: "UHS".into(),
    }]);
    assert!(remove_with(
        prefix,
        "utun7",
        None,
        false,
        || Ok(os.borrow().clone()),
        || {
            require_index("utun7", 0, 7)?;
            panic!("legacy journal has no original interface index")
        }
    )
    .is_err());
    os.borrow_mut().clear();
    remove_with(
        prefix,
        "utun7",
        None,
        false,
        || Ok(os.borrow().clone()),
        || panic!("already absent, no new owner may be touched"),
    )
    .unwrap();
    assert!(remove_with(
        prefix,
        "utun7",
        None,
        false,
        || Ok(vec![polluted("utun7", "192.0.2.254")]),
        || panic!("changed next hop")
    )
    .is_err());
}

#[test]
fn scoped_cleanup_preserves_native_default_and_other_interfaces() {
    let prefix = "0.0.0.0/0".parse().unwrap();
    let row = |interface: &str, gateway: &str, flags: &str| table::RouteEntry {
        prefix,
        interface: interface.into(),
        gateway: gateway.into(),
        flags: flags.into(),
    };
    let os = RefCell::new(vec![
        row("en5", "192.0.2.1", "UGSI"),
        row("en0", "192.168.0.1", "UGSI"),
        row("en5", "192.0.2.1", "UGS"),
    ]);
    remove_with(
        prefix,
        "en5",
        Some("192.0.2.1"),
        true,
        || Ok(os.borrow().clone()),
        || {
            os.borrow_mut()
                .retain(|entry| !(entry.interface == "en5" && entry.flags.contains('I')));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(os.borrow().len(), 2);
    assert!(os.borrow().iter().any(|row| row.interface == "en0"));
    assert!(os.borrow().iter().any(|row| !row.flags.contains('I')));
}

//! Simulated table observations test policy; they are not native close receipts.
use super::*;
use crate::route::{journal::RouteLease, RouteInterface};
use std::cell::RefCell;
fn old_journal(path: &Path) -> RouteJournal {
    RouteJournal::new(
        RouteLease::acquire_at(path.to_owned(), "fixture").unwrap(),
        "utun7",
        false,
        0,
        RouteInterface::new("en5".into(), 5).unwrap(),
        Some("192.0.2.1".into()),
    )
    .unwrap()
}
fn route(interface: &str) -> table::RouteEntry {
    table::RouteEntry {
        prefix: "0.0.0.0/1".parse().unwrap(),
        interface: interface.into(),
        gateway: "10.19.7.2".into(),
        flags: "UGS".into(),
    }
}
#[test]
fn legacy_identity_is_not_invented_and_absence_allows_retry_after_external_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.json");
    let mut journal = old_journal(&path);
    journal.record_route("0.0.0.0/1").unwrap();
    // Produce the exact old schema, including the old zero interface index.
    let mut old: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    old.as_object_mut().unwrap().remove("capture_gateway");
    old.as_object_mut().unwrap().remove("pending");
    std::fs::write(&path, serde_json::to_vec(&old).unwrap()).unwrap();
    drop(journal);
    let lease = RouteLease::acquire_at(path.clone(), "fixture").unwrap();
    let mut journal = RouteJournal::load(&lease, false).unwrap().unwrap();
    assert!(legacy_capture(&journal));
    let os = RefCell::new(vec![route("utun7")]);
    let mut recover = |journal: &mut RouteJournal| {
        journal.cleanup(
            |prefix| {
                confirm_absent_with(
                    &path,
                    "utun7",
                    false,
                    prefix,
                    "legacy identity unknown",
                    || Ok(os.borrow().clone()),
                )
            },
            |_| panic!("no exclusions"),
        )
    };
    let source = recover(&mut journal).unwrap_err();
    let message = source.to_string();
    assert!(message.contains("TUN_ROUTE_RECOVERY_REQUIRED"));
    assert!(message.contains("legacy.json"));
    assert!(message.contains("0.0.0.0/1"));
    assert!(message.contains("Preserve this journal"));
    assert_eq!(journal.tun_index, 0);
    assert!(journal.capture_gateway.is_none());
    assert!(path.exists());
    assert_eq!(os.borrow().len(), 1);
    os.borrow_mut().clear(); // External cleanup, not an invented kernel removal.
    recover(&mut journal).unwrap();
    assert!(!path.exists());
    assert_eq!(journal.tun_index, 0);
}
#[test]
fn foreign_rows_and_failed_observations_cannot_retire_unknown_intents() {
    let path = Path::new("pending.json");
    for row in [route("utun7"), route("utun99")] {
        assert!(
            confirm_absent_with(path, "utun7", false, "0.0.0.0/1", "pending", || Ok(vec![
                row
            ]))
            .is_err()
        );
    }
    assert!(
        confirm_absent_with(path, "utun7", false, "0.0.0.0/1", "pending", || Err(
            io::Error::other("snapshot failed")
        ))
        .is_err()
    );
    confirm_absent_with(path, "utun7", false, "0.0.0.0/1", "pending", || Ok(vec![])).unwrap();
}
#[test]
fn current_identity_is_not_legacy_but_missing_capture_gateway_is() {
    let dir = tempfile::tempdir().unwrap();
    let mut journal = old_journal(&dir.path().join("routes.json"));
    journal.tun_index = 7;
    assert!(legacy_capture(&journal));
    journal.capture_gateway = Some("10.19.7.2".into());
    assert!(!legacy_capture(&journal));
}

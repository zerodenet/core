//! Durable journal boundaries; no OS route acquisition/closure is fabricated.
use super::*;
use crate::route::{journal::RouteJournal, RouteInterface};
use std::{cell::Cell, io, path::Path};
fn journal(path: &Path) -> RouteJournal {
    RouteJournal::new(
        RouteLease::acquire_at(path.to_owned(), "fixture").unwrap(),
        "utun7",
        false,
        7,
        RouteInterface::new("en5".into(), 5).unwrap(),
        Some("192.0.2.1".into()),
    )
    .unwrap()
}
#[test]
fn intent_is_on_disk_before_mutation_and_confirmation_is_a_separate_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("routes.json");
    let mut journal = journal(&path);
    journal
        .install_route_with("0.0.0.0/1", || {
            let stored: RouteJournal = serde_json::from_slice(&std::fs::read(&path)?).unwrap();
            assert_eq!(stored.pending, ["0.0.0.0/1"]);
            assert!(stored.installed.is_empty());
            Ok(())
        })
        .unwrap();
    let stored: RouteJournal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(stored.pending.is_empty());
    assert_eq!(stored.installed, ["0.0.0.0/1"]);
}
#[test]
fn interrupted_and_failed_mutations_remain_intents_never_confirmed_routes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("routes.json");
    let mut journal = journal(&path);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        journal.install_route_with("10.19.7.2/32", || {
            panic!("interruption after persisted intent")
        })
    }));
    assert!(result.is_err());
    drop(journal);
    let lease = RouteLease::acquire_at(path.clone(), "fixture").unwrap();
    let mut recovered = RouteJournal::load(&lease, false).unwrap().unwrap();
    assert_eq!(recovered.pending, ["10.19.7.2/32"]);
    assert!(recovered.installed.is_empty());
    // Ordinary cleanup cannot reinterpret a pending acquisition as ownership.
    recovered
        .cleanup(
            |_| panic!("pending is not installed"),
            |_| panic!("no exclusions"),
        )
        .unwrap();
    assert!(path.exists());
    assert!(recovered
        .cleanup_pending_with(|_| Err(io::Error::other("native identity remains unknown")))
        .is_err());
    assert_eq!(recovered.pending, ["10.19.7.2/32"]);
    // Only a separate actual absence observation permits metadata retirement.
    recovered.cleanup_pending_with(|_| Ok(())).unwrap();
    assert!(!path.exists());
}
#[test]
fn failed_intent_persistence_never_invokes_mutation_and_command_error_does_not_confirm() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("routes.json");
    let mut journal = journal(&path);
    let invoked = Cell::new(false);
    journal.path = dir.path().join("missing/routes.json");
    assert!(journal
        .install_route_with("0.0.0.0/1", || {
            invoked.set(true);
            Ok(())
        })
        .is_err());
    assert!(!invoked.get());
    journal.path = path.clone();
    assert!(journal
        .install_route_with("128.0.0.0/1", || Err(io::Error::other(
            "command outcome unknown"
        )))
        .is_err());
    let stored: RouteJournal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert!(stored.installed.is_empty());
    assert!(stored.pending.contains(&"128.0.0.0/1".to_owned()));
}

use super::*;
use crate::packet_memory::{PacketMemoryError, PacketMetadataLayout, PacketReturnTotals};
use std::sync::Arc;
fn setup() -> (Arc<PacketReturnTotals>, RouteFileFaults, RouteFileOriginals) {
    let root = Arc::new(PacketReturnTotals::default());
    let faults = RouteFileFaults::prepare(&root.driver_metadata_view()).unwrap();
    let files = RouteFileOriginals::new(faults.clone());
    (root, faults, files)
}
fn acquire(
    files: &mut RouteFileOriginals,
    view: &crate::packet_memory::PacketDriverMetadataView,
    dir: &std::path::Path,
) -> io::Result<()> {
    RouteLease::acquire_retained_paths(
        files,
        dir.join("journal.json"),
        dir.join("route.lock"),
        "actual-source",
        false,
        view,
    )
}
#[test]
fn failed_prepare_keeps_the_actual_lock_until_explicit_close_and_pause_cannot_block_it() {
    let (root, faults, mut files) = setup();
    let directory = tempfile::tempdir().unwrap();
    let view = root.driver_metadata_view();
    let baseline = root.driver_metadata().occupied;
    acquire(&mut files, &view, directory.path()).unwrap();
    assert!(files.has_pending());
    let lock_path = directory.path().join("route.lock");
    let contender = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    assert!(fs2::FileExt::try_lock_exclusive(&contender).is_err());
    // Same real acquired lease survives the later fallible journal/query
    // window; the test deliberately does not publish a route guard.
    let next = PacketMetadataLayout::new(
        root.driver_metadata().limit.allocations(),
        root.driver_metadata().limit.bytes() - 1,
    );
    let paused = root.pause_driver_metadata_for_limit(next).unwrap().unwrap();
    assert!(matches!(
        faults.begin(&view),
        Err(PacketMemoryError::Paused)
    ));
    files.retire_pending().unwrap();
    assert!(!files.has_pending());
    fs2::FileExt::try_lock_exclusive(&contender).unwrap();
    fs2::FileExt::unlock(&contender).unwrap();
    assert_eq!(root.driver_metadata().occupied, baseline);
    assert!(!faults.has_failures());
    drop(paused);
}
#[test]
fn failed_flock_keeps_its_original_without_removing_another_owners_metadata() {
    let (root, faults, mut files) = setup();
    let directory = tempfile::tempdir().unwrap();
    let lock_path = directory.path().join("route.lock");
    let donor = std::fs::File::create(&lock_path).unwrap();
    fs2::FileExt::try_lock_exclusive(&donor).unwrap();
    let owner = lock_path.with_extension("");
    std::fs::write(&owner, b"the-real-other-owner").unwrap();
    assert!(acquire(&mut files, &root.driver_metadata_view(), directory.path()).is_err());
    assert!(files.has_pending() && faults.has_failures());
    files.retire_pending().unwrap();
    assert_eq!(std::fs::read(owner).unwrap(), b"the-real-other-owner");
    assert!(fs2::FileExt::try_lock_exclusive(&donor).is_ok());
}
#[test]
fn paid_fault_history_backpressures_next_acquire_before_native_and_releases_after_delivery() {
    let (root, faults, mut files) = setup();
    let view = root.driver_metadata_view();
    let directory = tempfile::tempdir().unwrap();
    let lock_path = directory.path().join("route.lock");
    let donor = std::fs::File::create(&lock_path).unwrap();
    fs2::FileExt::try_lock_exclusive(&donor).unwrap();
    let before = root.driver_metadata().occupied;
    assert!(acquire(&mut files, &view, directory.path()).is_err());
    files.retire_pending().unwrap();
    let occupied = root.driver_metadata().occupied;
    assert!(occupied.bytes() > before.bytes());
    let diagnostic = faults.diagnostic();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        occupied,
    )
    .unwrap();
    let empty = directory.path().join("new");
    std::fs::create_dir(&empty).unwrap();
    assert!(acquire(&mut files, &view, &empty).is_err());
    assert!(
        !empty.join("route.lock").exists(),
        "reject before actual Native acquisition"
    );
    assert!(!files.has_pending());
    drop(files);
    drop(faults);
    assert_eq!(
        root.driver_metadata().occupied,
        occupied,
        "receiver still owns original history"
    );
    drop(diagnostic);
    assert_eq!(
        root.driver_metadata().occupied,
        zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
    );
}
#[test]
fn prepaid_constructor_returns_original_credit_and_foreign_root_cannot_start_io() {
    let (root, faults, mut files) = setup();
    let small = root
        .driver_metadata_view()
        .reserve(PacketMetadataLayout::new(0, 0))
        .unwrap();
    let (error, small) = RouteFileFaults::from_prepaid(small).err().unwrap();
    assert_eq!(error, PacketMemoryError::Capacity);
    assert_eq!(small.layout(), PacketMetadataLayout::new(0, 0));
    drop(small);
    let foreign = Arc::new(PacketReturnTotals::default());
    let directory = tempfile::tempdir().unwrap();
    assert!(acquire(
        &mut files,
        &foreign.driver_metadata_view(),
        directory.path()
    )
    .is_err());
    assert!(!directory.path().join("route.lock").exists());
    assert!(!files.has_pending() && !faults.has_failures());
}

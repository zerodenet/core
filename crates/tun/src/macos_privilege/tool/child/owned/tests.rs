use super::*;
use crate::packet_memory::{PacketMetadataLayout, PacketReturnTotals};
use std::sync::Arc;
fn backing(memory: &PacketMemoryView) -> [PacketMetadataArc<Storage>; 2] {
    std::array::from_fn(|_| {
        Storage::from_prepaid(
            memory
                .reserve_driver_metadata(Storage::layout().unwrap())
                .unwrap(),
        )
        .unwrap()
    })
}
#[test]
fn actual_local_child_result_holds_same_source_storage_until_consumed_before_next_command() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let storage = backing(&memory);
    let charged = root.driver_metadata().occupied;
    let mut child = ChildOriginal::new();
    // Test-only fixed executable; production callers above retain their exact
    // original three privileged programs and closed metadata-query projection.
    let output = run(
        &mut child,
        "/bin/cat",
        &[],
        Some("actual result"),
        &memory,
        &storage,
        65536,
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(&*output.stdout, b"actual result");
    assert!(child.reusable());
    let first = child.child.as_ref().unwrap().id();
    assert_eq!(
        run(
            &mut child,
            "/bin/cat",
            &[],
            Some("cannot overwrite"),
            &memory,
            &storage,
            65536
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(child.child.as_ref().unwrap().id(), first);
    assert_eq!(&*output.stdout, b"actual result");
    drop(output);
    let output = run(
        &mut child,
        "/bin/cat",
        &[],
        Some("same source cleanup"),
        &memory,
        &storage,
        65536,
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(&*output.stdout, b"same source cleanup");
    assert!(child.reusable());
    drop(output);
    assert_eq!(root.driver_metadata().occupied, charged);
}
#[test]
fn actual_full_root_drains_original_child_and_returns_original_fds_before_small_cleanup_without_new_memory(
) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let storage = backing(&memory);
    let charged = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        PacketMetadataLayout::new(charged.allocations(), charged.bytes()),
    )
    .unwrap();
    let mut child = ChildOriginal::new();
    let input = "a".repeat(65536);
    let error = run(
        &mut child,
        "/bin/cat",
        &[],
        Some(&input),
        &memory,
        &storage,
        131072,
    )
    .unwrap_err();
    assert!(error
        .get_ref()
        .unwrap()
        .is::<crate::packet_memory::PacketMemoryError>());
    assert!(child.reaped());
    assert!(child.reusable());
    assert!(child
        .pipes
        .iter()
        .chain(&child.child_pipes)
        .chain(&child.preparing)
        .chain(&child.relocated)
        .flatten()
        .all(Original::closed));
    assert_eq!(root.driver_metadata().occupied, charged);
    let output = run(
        &mut child,
        "/bin/cat",
        &[],
        Some("cleanup after pressure"),
        &memory,
        &storage,
        131072,
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(&*output.stdout, b"cleanup after pressure");
    assert!(child.reusable());
    drop(output);
    assert_eq!(root.driver_metadata().occupied, charged);
}

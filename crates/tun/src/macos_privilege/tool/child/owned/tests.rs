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
    let owner =
        crate::macos_privilege::session::MacosHelperOwner::prepare(&memory, 131072, false, 65536)
            .unwrap();
    let _context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
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
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 65536,
        },
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(&*output.stdout, b"actual result");
    assert!(child.reusable());
    let first = mode.control().test_child_id().unwrap();
    assert_eq!(
        run(
            &mut child,
            "/bin/cat",
            &[],
            Some("cannot overwrite"),
            OutputStorage {
                memory: &memory,
                storage: &storage,
                mode: &mode,
                limit: 65536
            }
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(mode.control().test_child_id(), Some(first));
    assert!(mode.control().local_reaped());
    assert_eq!(&*output.stdout, b"actual result");
    drop(output);
    let output = run(
        &mut child,
        "/bin/cat",
        &[],
        Some("same source cleanup"),
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 65536,
        },
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
    let owner =
        crate::macos_privilege::session::MacosHelperOwner::prepare(&memory, 131072, false, 65536)
            .unwrap();
    let _context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
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
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 131072,
        },
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
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 131072,
        },
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(&*output.stdout, b"cleanup after pressure");
    assert!(child.reusable());
    drop(output);
    assert_eq!(root.driver_metadata().occupied, charged);
}

#[path = "tests/cancel.rs"]
mod cancel;

use super::*;
#[test]
fn source_paid_query_borrows_original_argv_and_returns_its_real_child_and_output_before_reuse() {
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
    let arguments = ["-n".to_owned(), "get".to_owned(), "127.0.0.1".to_owned()];
    let mut child = ChildOriginal::new();
    let output = run_query_owned(
        &mut child,
        super::super::super::super::Query::Route(&arguments),
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 65536,
        },
    )
    .unwrap();
    assert!(child.reaped());
    assert!(child.reusable());
    assert!(!output.stdout.is_empty() || !output.stderr.is_empty());
    assert_eq!(arguments, ["-n", "get", "127.0.0.1"]);
    drop(output);
    assert_eq!(root.driver_metadata().occupied, charged);
    let output = run_query_owned(
        &mut child,
        super::super::super::super::Query::SystemDns,
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 65536,
        },
    )
    .unwrap();
    assert!(child.reaped());
    assert!(child.reusable());
    drop(output);
    assert_eq!(root.driver_metadata().occupied, charged);
}

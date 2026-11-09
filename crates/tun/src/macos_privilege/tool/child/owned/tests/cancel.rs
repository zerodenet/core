//! Stop interrupts the real pending stdio operation; only its worker reaps.
use super::*;
use std::time::Duration;

#[test]
fn actual_source_stop_signals_original_pending_child_then_same_source_cleanup_survives() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    let owner =
        crate::macos_privilege::session::MacosHelperOwner::prepare(&memory, 131072, false, 65536)
            .unwrap();
    let lease = owner.lease_routes().unwrap();
    let storage = backing(&memory);
    let charged = root.driver_metadata().occupied;
    let (entered, pending) = std::sync::mpsc::sync_channel(0);
    let actual_owner = owner.clone();
    let actual_memory = memory.clone();
    let actual_storage = storage.clone();
    let worker = std::thread::spawn(move || {
        let _context = actual_owner.enter().unwrap();
        let mode = actual_owner.test_command_mode().unwrap();
        let mut child = ChildOriginal::new();
        child.pending = Some(entered);
        let result = run(
            &mut child,
            "/bin/sleep",
            &["60".into()],
            None,
            OutputStorage {
                memory: &actual_memory,
                storage: &actual_storage,
                mode: &mode,
                limit: 131072,
            },
        );
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
        result.unwrap_err()
    });
    // This rendezvous follows poll(0)==Pending on the actual original pipes.
    pending.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(owner.require_closed().is_err());
    owner.stop();
    let error = worker.join().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(root.driver_metadata().occupied, charged);
    assert!(owner.enter().is_err(), "new business remains fenced");
    let context = lease.prepare_cleanup().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let mut child = ChildOriginal::new();
    let output = run(
        &mut child,
        "/bin/cat",
        &[],
        Some("cleanup after real cancel"),
        OutputStorage {
            memory: &memory,
            storage: &storage,
            mode: &mode,
            limit: 131072,
        },
    )
    .unwrap();
    assert!(output.status.success());
    assert_eq!(&*output.stdout, b"cleanup after real cancel");
    assert!(child.reaped());
    assert!(child.reusable());
    owner.stop(); // Never signal the already reaped previous PID or this cleanup.
    drop(output);
    drop(child);
    drop(mode);
    drop(context);
    lease.finish_cleanup().unwrap();
    owner.require_closed().unwrap();
    drop(storage);
    drop(lease);
    drop(owner);
    assert_eq!(root.driver_metadata().occupied, baseline);
}

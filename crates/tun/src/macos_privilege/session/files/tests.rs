use super::*;
use crate::packet_memory::PacketReturnTotals;
use std::sync::Arc;
#[test]
fn actual_journal_files_close_before_return_and_stopped_source_can_only_use_its_retained_lock() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner = MacosHelperOwner::prepare(&read.memory_view().unwrap(), 4096, false, rpc::CAPACITY)
        .unwrap();
    let lease = owner.lease_routes().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("journal.json");
    let lock_path = directory.path().join("lock");
    let context = owner.enter().unwrap();
    let mut lock = journal_lock(&lock_path, false).unwrap().unwrap();
    let source = lock.access();
    journal_write(&file, b"actual journal").unwrap().unwrap();
    assert_eq!(journal_read(&file).unwrap().unwrap(), b"actual journal");
    assert!(owner.0.epochs[0]
        .lock()
        .unwrap()
        .local_file
        .as_ref()
        .unwrap()
        .closed());
    // Reading an oversized file still closes this same actual original.
    std::fs::write(&file, [1u8; 4097]).unwrap();
    assert!(journal_read(&file).unwrap().is_err());
    assert!(owner.0.epochs[0]
        .lock()
        .unwrap()
        .local_file
        .as_ref()
        .unwrap()
        .closed());
    owner.stop();
    assert!(journal_write(&file, b"unadmitted new IO").unwrap().is_err());
    drop(context);
    assert!(owner.access().enter().is_err());
    {
        let _cleanup = source.context().unwrap();
        journal_write(&file, b"same retained cleanup")
            .unwrap()
            .unwrap();
    }
    lock.close_retained().unwrap();
    assert!(lock.is_closed());
    assert!(source.context().is_err()); // cannot fall back to a library File
    lease.finish_cleanup().unwrap();
    owner.require_closed().unwrap();
}

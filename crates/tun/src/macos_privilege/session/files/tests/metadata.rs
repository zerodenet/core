use super::*;
use crate::packet_memory::PacketMetadataLayout;
#[test]
fn actual_journal_read_keeps_paid_bytes_after_file_close_and_refusal_preserves_old_material() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let owner = MacosHelperOwner::prepare(&memory, 4096, false, rpc::CAPACITY).unwrap();
    let _context = owner.enter().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.json");
    std::fs::write(&path, b"real paid journal material retained").unwrap();
    let baseline = root.driver_metadata().occupied;
    let bytes = journal_read(&path).unwrap().unwrap();
    assert_eq!(&*bytes, b"real paid journal material retained");
    assert!(root.driver_metadata().occupied.bytes() >= baseline.bytes() + bytes.len());
    assert!(owner.0.epochs[0]
        .lock()
        .unwrap()
        .local_file
        .as_ref()
        .unwrap()
        .closed());
    // Also retain one real allocation unit so a later growth peak can fit
    // after both original owners are actually released, without changing caps.
    let extra = memory
        .reserve_driver_metadata(PacketMetadataLayout::new(1, 0))
        .unwrap();
    let occupied = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        occupied,
    )
    .unwrap();
    std::fs::write(&path, b"small retry").unwrap();
    assert!(journal_read(&path).unwrap().is_err());
    assert_eq!(&*bytes, b"real paid journal material retained");
    assert_eq!(root.driver_metadata().occupied, occupied);
    assert!(owner.0.epochs[0]
        .lock()
        .unwrap()
        .local_file
        .as_ref()
        .unwrap()
        .closed());
    drop(bytes);
    drop(extra);
    let retry = journal_read(&path).unwrap().unwrap();
    assert_eq!(&*retry, b"small retry");
    drop(retry);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn actual_journal_serialization_uses_same_root_and_retains_exact_escaped_material_until_drop() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner = MacosHelperOwner::prepare(&read.memory_view().unwrap(), 4096, false, rpc::CAPACITY)
        .unwrap();
    let _context = owner.enter().unwrap();
    let value = ["original route with quote\" λ", "127.0.0.1/32"];
    let baseline = root.driver_metadata().occupied;
    let bytes = journal_encode(&value).unwrap().unwrap();
    assert_eq!(&*bytes, serde_json::to_vec(&value).unwrap());
    assert!(root.driver_metadata().occupied.bytes() >= baseline.bytes() + bytes.len());
    owner.stop();
    assert!(journal_encode(&value).unwrap().is_err());
    assert_eq!(&*bytes, serde_json::to_vec(&value).unwrap());
    drop(bytes);
    assert_eq!(root.driver_metadata().occupied, baseline);
}

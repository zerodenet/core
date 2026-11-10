use super::*;
use crate::packet_memory::PacketReturnTotals;
use std::sync::Arc;
#[test]
fn actual_transient_read_write_reuses_one_slot_and_retirement_revokes_io_without_allocating() {
    let root = Arc::new(PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = root.driver_metadata().occupied;
    let faults = RouteFileFaults::prepare(&memory).unwrap();
    let original = RouteFileIo::prepare(faults.clone(), &memory).unwrap();
    let stable = root.driver_metadata().occupied;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal");
    assert_eq!(
        original.read(&path).err().unwrap().kind(),
        io::ErrorKind::NotFound
    );
    assert!(
        !faults.has_failures(),
        "missing journal is a real no-native result"
    );
    for bytes in [&b"first journal"[..], &b"replacement journal bytes"[..]] {
        original.write(&path, bytes).unwrap();
        assert_eq!(
            root.driver_metadata().occupied,
            stable,
            "closed successful operation keeps no history row"
        );
        let received = original.read(&path).unwrap();
        assert_eq!(&*received, bytes);
        drop(received);
        assert_eq!(root.driver_metadata().occupied, stable);
    }
    let closed = root
        .pause_driver_metadata_for_limit(crate::packet_memory::PacketMetadataLayout::new(
            root.driver_metadata().limit.allocations(),
            root.driver_metadata().limit.bytes() - 1,
        ))
        .unwrap()
        .unwrap();
    original.close().unwrap();
    assert!(original.closed());
    // Cleanup uses no new metadata admission even while candidate pauses IO.
    drop(closed);
    assert!(original
        .write(&path, b"must not overwrite after retirement")
        .is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement journal bytes");
    drop(original);
    drop(faults);
    assert_eq!(root.driver_metadata().occupied, baseline);
}

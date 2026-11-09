use super::*;
use crate::packet_memory::PacketReturnTotals;
#[test]
fn actual_source_material_read_and_encode_use_the_same_root_on_every_platform() {
    let root = std::sync::Arc::new(PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = root.driver_metadata().occupied;
    let value = ["original\"escaped", "actual source journal input"];
    let encoded = encode(&value, Some(&memory)).unwrap();
    assert_eq!(&*encoded, serde_json::to_vec(&value).unwrap());
    assert!(root.driver_metadata().occupied.bytes() >= baseline.bytes() + encoded.len());
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("material.json");
    write(&file, &encoded).unwrap();
    let charged = root.driver_metadata().occupied;
    let read = read(&file, Some(&memory)).unwrap();
    assert_eq!(&*read, &*encoded);
    assert!(root.driver_metadata().occupied.bytes() >= charged.bytes() + read.len());
    drop(encoded);
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    drop(read);
    assert_eq!(root.driver_metadata().occupied, baseline);
}

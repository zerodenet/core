use super::*;
use crate::packet_memory::PacketReturnTotals;
#[test]
fn stale_cleanup_borrows_exact_recorded_scalars_through_original_callbacks_without_copying_them() {
    let root = std::sync::Arc::new(PacketReturnTotals::default());
    let bytes=br#"{"tun_name":"stale-tun","ipv6":false,"tun_index":7,"egress":{"name":"old-egress","index":9},"gateway":"192.0.2.7","excluded":["192.0.2.4"],"installed":["10.0.0.0/8"]}"#;
    let mut journal = super::super::decode::decode(bytes, root.driver_metadata_view()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    journal.path = journal
        ._metadata
        .path(&directory.path().join("journal.json"))
        .unwrap();
    let pointers = (
        journal.tun_name.as_ptr(),
        journal.egress.name().as_ptr(),
        journal.gateway.as_deref().unwrap().as_ptr(),
    );
    let occupied = root.driver_metadata().occupied;
    let mut routes = 0;
    let mut exclusions = 0;
    let error = journal
        .cleanup_record(
            |record, prefix| {
                assert_eq!(prefix, "10.0.0.0/8");
                assert_eq!(record.tun_name, "stale-tun");
                assert_eq!(record.tun_name.as_ptr(), pointers.0);
                routes += 1;
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            },
            |record, address| {
                assert_eq!(address, "192.0.2.4".parse::<IpAddr>().unwrap());
                assert_eq!(record.egress.name(), "old-egress");
                assert_eq!(record.egress.name().as_ptr(), pointers.1);
                assert_eq!(record.gateway.as_deref(), Some("192.0.2.7"));
                assert_eq!(record.gateway.as_deref().unwrap().as_ptr(), pointers.2);
                exclusions += 1;
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!((routes, exclusions), (1, 1));
    assert_eq!(root.driver_metadata().occupied, occupied);
    assert_eq!(journal.installed, ["10.0.0.0/8"]);
    assert_eq!(journal.excluded, ["192.0.2.4".parse::<IpAddr>().unwrap()]);
}

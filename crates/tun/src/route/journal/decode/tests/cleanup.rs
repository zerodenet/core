use super::*;
#[test]
fn allocation_free_cleanup_keeps_every_reverse_duplicate_callback_and_original_partial_journal() {
    let root = Arc::new(PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let bytes=br#"{"tun_name":"utun","ipv6":false,"tun_index":7,"egress":{"name":"physical0","index":9},"excluded":["192.0.2.1","192.0.2.1"],"installed":["same","fail","same"]}"#;
    let mut journal = decode(bytes, root.driver_metadata_view()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    journal.path = journal
        ._metadata
        .path(&directory.path().join("journal.json"))
        .unwrap();
    let mut routes = Vec::new();
    let mut addresses = Vec::new();
    let mut same = 0;
    let error = journal
        .cleanup(
            |route| {
                routes.push(route.to_owned());
                if route == "same" {
                    same += 1;
                }
                if route == "fail" || (route == "same" && same == 2) {
                    Err(std::io::Error::other("actual removal failure"))
                } else {
                    Ok(())
                }
            },
            |address| {
                addresses.push(address);
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(routes, ["same", "fail", "same"]);
    assert_eq!(addresses, ["192.0.2.1".parse::<IpAddr>().unwrap(); 2]);
    assert_eq!(journal.installed, ["fail"]);
    assert!(journal.excluded.is_empty());
    let persisted: RouteJournal =
        serde_json::from_slice(&std::fs::read(&journal.path).unwrap()).unwrap();
    assert_eq!(persisted.installed, ["fail"]);
    assert!(persisted.excluded.is_empty());
    drop(journal);
    assert_eq!(root.driver_metadata().occupied, baseline);
}

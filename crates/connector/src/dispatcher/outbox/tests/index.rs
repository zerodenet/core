use super::*;

#[test]
fn disk_index_keeps_exact_identity_order_and_removes_empty_sink_labels() {
    let mut index = PendingIndex::new(Path::new("test-journal")).unwrap();
    index.begin_batch().unwrap();
    // No delimiter concatenation or identity hashes: arbitrary UTF-8 keys
    // and the same event ID assigned to two sinks remain distinct.
    for (sink, event, offset) in [("a\0b", "c", 1), ("a", "b\0c", 2), ("另一个", "c", 3)] {
        index.insert(sink, event, offset).unwrap();
    }
    index.insert("a\0b", "c", 4).unwrap();
    index.remove("a", "b\0c").unwrap();
    index.remove("a", "b\0c").unwrap();
    index.finish_batch().unwrap();
    assert_eq!(index.len(), 2);
    assert!(!index.counts().unwrap().contains_key("a"));
    let mut offsets = Vec::new();
    index
        .visit(|offset, key| {
            offsets.push((offset, key));
            Ok(true)
        })
        .unwrap();
    assert_eq!(
        offsets,
        [
            (3, ("另一个".into(), "c".into())),
            (4, ("a\0b".into(), "c".into()))
        ]
    );
    assert!(matches!(
        index.insert("overflow", "offset", u64::MAX),
        Err(ConnectorError::OutboxIndex { .. })
    ));
    assert_eq!(index.len(), 2);
    // An unexpected duplicate offset is an index failure, never permission
    // to replace a different unconfirmed identity.
    assert!(matches!(
        index.insert("different", "identity", 3),
        Err(ConnectorError::OutboxIndex { .. })
    ));
    assert_eq!(index.len(), 2);
}

#[test]
fn disk_index_spills_pages_uses_offset_index_and_cleans_scratch_on_drop() {
    let mut index = PendingIndex::new(Path::new("test-journal")).unwrap();
    let scratch = index._scratch.path().to_owned();
    let cache: i64 = index
        .db
        .query_row("PRAGMA cache_size", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cache, -256);
    index.begin_batch().unwrap();
    for id in 0..10000 {
        index
            .insert(&format!("sink-{id:08}"), &format!("{id:08}"), id)
            .unwrap();
    }
    index.finish_batch().unwrap();
    assert!(
        std::fs::metadata(scratch.join("index.sqlite"))
            .unwrap()
            .len()
            > 256 * 1024
    );
    assert_eq!(
        index.counts_for(["sink-00000001", "absent"]).unwrap(),
        BTreeMap::from([("sink-00000001".into(), 1)])
    );
    let plan: String = index
        .db
        .query_row(
            "EXPLAIN QUERY PLAN SELECT offset,sink,event FROM pending ORDER BY offset",
            [],
            |r| r.get(3),
        )
        .unwrap();
    assert!(plan.contains("pending_offset"), "{plan}");
    drop(index);
    assert!(!scratch.exists());
}

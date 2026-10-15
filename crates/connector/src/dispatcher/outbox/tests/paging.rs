use super::*;

#[test]
fn waiting_for_delivery_bytes_does_not_rescan_a_large_frame_each_poll() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 1, 0).unwrap();
    let mut value = delivery("large-waiting", 0);
    value.event.payload = serde_json::json!({"padding":"x".repeat(65536)});
    outbox.put(&value).unwrap();
    for _ in 0..32 {
        assert!(outbox
            .load_pending_with_budget(&HashSet::new(), 4, 1024, false)
            .unwrap()
            .is_empty());
    }
    assert_eq!(outbox.frame_scans.get(), 1);
    assert_eq!(outbox.pending_counts()["receiver"], 1);
    assert_eq!(
        outbox
            .load_pending_with_budget(&HashSet::new(), 4, 1024, true)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(outbox.frame_scans.get(), 1);
}

#[test]
fn compaction_invalidates_cached_length_when_a_different_record_reuses_offset_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut first = delivery("first", 0);
    first.event.payload = serde_json::json!({"padding":"x".repeat(65536)});
    let mut bytes = serde_json::to_vec(&JournalRecord::Put {
        delivery: Box::new(first),
    })
    .unwrap();
    bytes.push(b'\n');
    bytes.extend(put_frame("second", 0));
    for _ in 0..1021 {
        bytes.extend(b"{\"op\":\"ack\",\"sink_tag\":\"receiver\",\"event_id\":\"unrelated\"}\n");
    }
    std::fs::write(&path, bytes).unwrap();
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 1, 0).unwrap();
    assert!(outbox
        .load_pending_with_budget(&HashSet::new(), 4, 1024, false)
        .unwrap()
        .is_empty());
    outbox.ack("receiver", "first").unwrap();
    assert_eq!(outbox.journal_records, 1);
    let pending = outbox
        .load_pending_with_budget(&HashSet::new(), 4, 1024, false)
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0.event.event_id, "second");
}

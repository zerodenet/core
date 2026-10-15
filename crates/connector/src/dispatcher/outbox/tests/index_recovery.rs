use super::*;

#[test]
fn scratch_sqlite_full_preserves_durable_event_and_recovers_from_jsonl() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    outbox.put(&delivery("existing", 1)).unwrap();
    outbox.pending.cap_disk_pages();
    let id = "large-id-".to_owned() + &"x".repeat(16 * 1024);
    let error = outbox.put(&delivery(&id, 2)).unwrap_err();
    assert!(matches!(error, ConnectorError::OutboxIndex { .. }));
    assert!(error.to_string().contains("full"), "{error}");
    assert_eq!(inspect_path(&path).unwrap().pending, 2);
    // ACK retry rebuilds scratch independently; the unconfirmed large event
    // remains intact even though the previous derived-index write ran out.
    outbox.ack("receiver", "existing").unwrap();
    let pending = outbox.load_pending_excluding(&HashSet::new(), 2).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, id);
    assert_eq!(pending[0].attempts, 2);
    assert!(outbox.recovery_status().is_none());
}

#[test]
fn durable_put_with_failed_index_is_rebuilt_before_retry_without_losing_facts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    outbox.put(&delivery("first", 0)).unwrap();
    outbox.pending.reject_writes();
    let error = outbox.put(&delivery("second", 3)).unwrap_err();
    assert!(matches!(error, ConnectorError::OutboxIndex { .. }));
    assert!(outbox.pending_counts().is_err());
    assert!(outbox.load_pending_excluding(&HashSet::new(), 4).is_err());
    // A scratch error must not quarantine or truncate accepted JSONL facts.
    assert_eq!(inspect_path(&path).unwrap().pending, 2);
    outbox.put(&delivery("second", 4)).unwrap();
    let pending = outbox.load_pending_excluding(&HashSet::new(), 4).unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].event.event_id, "first");
    assert_eq!(pending[1].attempts, 4);
    assert!(outbox.recovery_status().is_none());
}

#[test]
fn durable_ack_with_failed_index_survives_restart_and_ack_retry() {
    for restart in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outbox.jsonl");
        let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
        outbox.put(&delivery("acked", 1)).unwrap();
        outbox.put(&delivery("unconfirmed", 2)).unwrap();
        outbox.pending.reject_writes();
        assert!(matches!(
            outbox.ack("receiver", "acked"),
            Err(ConnectorError::OutboxIndex { .. })
        ));
        if restart {
            drop(outbox);
            outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
        }
        outbox.ack("receiver", "acked").unwrap();
        let pending = outbox.load_pending_excluding(&HashSet::new(), 4).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].event.event_id, "unconfirmed");
        assert_eq!(pending[0].attempts, 2);
    }
}

#[test]
fn disk_index_compaction_preserves_all_sinks_payloads_retries_and_latest_offset_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut bytes = Vec::new();
    for id in 0..257 {
        bytes.extend(put_frame(&format!("{id:04}"), id));
    }
    let mut other = delivery("0000", 7);
    other.sink_tag = "other".into();
    bytes.extend(
        serde_json::to_vec(&JournalRecord::Put {
            delivery: Box::new(other),
        })
        .unwrap(),
    );
    bytes.push(b'\n');
    bytes.extend(put_frame("0000", 99));
    for _ in 0..1100 {
        bytes.extend(b"{\"op\":\"ack\",\"sink_tag\":\"receiver\",\"event_id\":\"unknown\"}\n");
    }
    std::fs::write(&path, bytes).unwrap();
    for _ in 0..2 {
        let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
        assert_eq!(outbox.journal_records, 258);
        assert_eq!(outbox.pending_counts().unwrap()["receiver"], 257);
        assert_eq!(outbox.pending_counts().unwrap()["other"], 1);
        let pending = outbox.load_pending_excluding(&HashSet::new(), 300).unwrap();
        for (i, value) in pending[..256].iter().enumerate() {
            assert_eq!(value.event.event_id, format!("{:04}", i + 1));
            assert_eq!(value.event.payload, serde_json::json!({"attempt":i+1}));
        }
        assert_eq!(pending[256].sink_tag, "other");
        assert_eq!(pending[256].attempts, 7);
        assert_eq!(pending[257].event.event_id, "0000");
        assert_eq!(pending[257].attempts, 99);
        assert_eq!(
            pending[257].event.payload,
            serde_json::json!({"attempt":99})
        );
    }
}

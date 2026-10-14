use super::*;
use crate::{inspect_persistent_state, ConnectorStateStatus};
use zero_api::ApiEvent;

mod large;
mod validation;

fn delivery(id: &str, attempts: u32) -> OutboxDelivery {
    OutboxDelivery {
        sink_tag: "receiver".into(),
        event: ApiEvent::new(
            id,
            "flow.completed",
            1,
            serde_json::json!({"attempt": attempts}),
        ),
        attempts,
        message: Some(format!("retry {attempts}")),
    }
}

fn put_frame(id: &str, attempts: u32) -> Vec<u8> {
    let mut frame = serde_json::to_vec(&JournalRecord::Put {
        delivery: Box::new(delivery(id, attempts)),
    })
    .unwrap();
    frame.push(b'\n');
    frame
}

#[test]
fn retries_compact_without_ack_or_restart_and_preserve_latest_pending_deliveries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    outbox.put(&delivery("never-retried", 0)).unwrap();
    for attempt in 0..1030 {
        outbox.put(&delivery("retried", attempt)).unwrap();
    }
    assert!(
        outbox.journal_records < 1024,
        "PUT-only retries must trigger compaction"
    );
    assert_eq!(outbox.pending_counts()["receiver"], 2);
    let pending = outbox.load_pending_excluding(&HashSet::new(), 8).unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(
        pending
            .iter()
            .find(|d| d.event.event_id == "retried")
            .unwrap()
            .attempts,
        1029
    );
    assert_eq!(
        pending
            .iter()
            .find(|d| d.event.event_id == "never-retried")
            .unwrap()
            .event
            .payload,
        serde_json::json!({"attempt":0})
    );
    // Inspection of a live, exclusively leased journal is read-only.
    let before = std::fs::read(&path).unwrap();
    assert_eq!(inspect_path(&path).unwrap().pending, 2);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(outbox);
    let mut reopened = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    assert_eq!(
        reopened
            .load_pending_excluding(&HashSet::new(), 8)
            .unwrap()
            .len(),
        2
    );
    reopened.ack("receiver", "retried").unwrap();
    assert_eq!(reopened.pending_counts()["receiver"], 1);
}

#[test]
fn streaming_replay_preserves_partial_tail_evidence_offsets_and_pending_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut bytes = put_frame("first", 0);
    bytes.extend(b"\n");
    bytes.extend(put_frame("second", 1));
    bytes.extend(br#"{"op":"put","delivery":{"#);
    std::fs::write(&path, &bytes).unwrap();
    let inspection = inspect_path(&path).unwrap();
    assert!(inspection.recoverable_partial_tail);
    assert_eq!(inspection.pending, 2);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    let recovery = outbox.recovery_status().unwrap();
    assert_eq!(std::fs::read(recovery.preserved_path).unwrap(), bytes);
    let pending = outbox.load_pending_excluding(&HashSet::new(), 8).unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|d| d.event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert!(!inspect_path(&path).unwrap().recoverable_partial_tail);
}

#[test]
fn malformed_complete_frames_keep_corruption_class_and_do_not_modify_inspected_files() {
    for (suffix, class) in [
        (b"".as_slice(), "malformed_tail"),
        (b"\n".as_slice(), "malformed_middle"),
        (b"partial".as_slice(), "malformed_tail"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outbox.jsonl");
        let mut bytes = put_frame("valid", 0);
        bytes.extend(b"{bad}\n");
        bytes.extend(suffix);
        std::fs::write(&path, &bytes).unwrap();
        assert!(inspect_path(&path).unwrap_err().to_string().contains(class));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn dead_letter_inspection_streams_large_records_and_rejects_invalid_payload_utf8() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dead.jsonl");
    let mut bytes = br#"{"payload":""#.to_vec();
    bytes.extend(vec![b'x'; 256 * 1024]);
    bytes.extend(br#""}"#);
    std::fs::write(&path, &bytes).unwrap();
    let mut config = zero_config::RuntimeConfig::parse(
        r#"{"inbounds":[],"outbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    config.api.dead_letter_path = Some(path.display().to_string());
    let report = inspect_persistent_state(&config);
    assert!(report.compatible);
    assert_eq!(report.files[0].records, Some(1));
    bytes.insert(bytes.len() - 2, 0xff);
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(
        inspect_persistent_state(&config).files[0].status,
        ConnectorStateStatus::Incompatible
    );
}

#[test]
fn streaming_replay_accepts_reordered_headers_and_utf8_across_buffer_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut value = delivery("unicode", 3);
    value.event.payload = serde_json::json!({"large":"中".repeat(64*1024)});
    let frame = format!(
        "{{\"delivery\":{},\"op\":\"put\"}}\n",
        serde_json::to_string(&value).unwrap()
    );
    std::fs::write(&path, &frame).unwrap();
    assert_eq!(inspect_path(&path).unwrap().pending, 1);
    let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    assert_eq!(
        outbox.load_pending_excluding(&HashSet::new(), 1).unwrap()[0]
            .event
            .payload,
        value.event.payload
    );
}

#[test]
fn compaction_never_discards_distinct_unacknowledged_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut bytes = Vec::new();
    for id in 0..1100 {
        bytes.extend(put_frame(&id.to_string(), id));
    }
    std::fs::write(&path, &bytes).unwrap();
    let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    assert_eq!(outbox.pending_counts()["receiver"], 1100);
    assert_eq!(
        outbox
            .load_pending_excluding(&HashSet::new(), 1101)
            .unwrap()
            .len(),
        1100
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

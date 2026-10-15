use super::*;
use crate::{inspect_persistent_state, ConnectorStateStatus};
use std::collections::VecDeque;
use zero_api::ApiEvent;

mod index_recovery;
mod large;
mod paging;
mod validation;

#[test]
fn fresh_samples_do_not_evict_legacy_durable_ack_retries() {
    use super::super::{enqueue_sample, PendingDelivery};
    let mut legacy = delivery("legacy", 3);
    legacy.event.event_type = "stats.scopes_sampled".into();
    let mut queued = PendingDelivery::recovered(legacy);
    queued.awaiting_ack = true;
    let mut pending = VecDeque::from([queued]);
    let new_sample = ApiEvent::new(
        "new-sample",
        "stats.scopes_sampled",
        2,
        serde_json::json!({"value":2}),
    );
    enqueue_sample(
        &mut pending,
        PendingDelivery::new("receiver".into(), new_sample, false),
        1,
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event.event_id, "legacy");
    assert!(pending[0].uses_outbox && pending[0].awaiting_ack);
}

#[test]
fn every_declared_sample_event_has_best_effort_delivery() {
    for kind in zero_api::event_type::ALL {
        if kind.ends_with(".sampled") || kind.ends_with("_sampled") {
            let mut value = delivery(kind, 0);
            value.event.event_type = (*kind).into();
            assert!(super::super::is_discardable_sample(&value.event), "{kind}");
        }
    }
}

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
    assert_eq!(outbox.pending_counts().unwrap()["receiver"], 2);
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
    assert_eq!(reopened.pending_counts().unwrap()["receiver"], 1);
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
    assert_eq!(outbox.pending_counts().unwrap()["receiver"], 1100);
    assert_eq!(
        outbox
            .load_pending_excluding(&HashSet::new(), 1101)
            .unwrap()
            .len(),
        1100
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn replay_preserves_old_persisted_samples_until_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old-samples.jsonl");
    let kinds = [
        zero_api::event_type::FLOW_UPDATED,
        zero_api::event_type::STATS_SAMPLED,
        "stats.scopes_sampled",
        "stats.host_interfaces_sampled",
        "endpoint.stats_sampled",
        "stats.reset",
        zero_api::event_type::FLOW_COMPLETED,
        "endpoint.state_changed",
    ];
    let mut bytes = Vec::new();
    for kind in kinds {
        let mut value = delivery(kind, 2);
        value.event.event_type = kind.into();
        bytes.extend(
            serde_json::to_vec(&JournalRecord::Put {
                delivery: Box::new(value),
            })
            .unwrap(),
        );
        bytes.push(b'\n');
    }
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(inspect_path(&path).unwrap().pending, kinds.len());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 0, 0).unwrap();
    let pending = outbox.load_pending_excluding(&HashSet::new(), 10).unwrap();
    assert_eq!(pending.len(), kinds.len());
    for kind in &kinds {
        assert!(pending.iter().any(|value| value.event.event_type == *kind));
    }
    for attempt in 0..1030 {
        let mut sample = delivery(kinds[0], attempt);
        sample.event.event_type = kinds[0].into();
        outbox.put(&sample).unwrap();
    }
    assert!(outbox.journal_records < 1024);
    assert_eq!(
        outbox
            .load_pending_excluding(&HashSet::new(), 10)
            .unwrap()
            .len(),
        kinds.len()
    );
    outbox.ack("receiver", kinds[0]).unwrap();
    drop(outbox);
    assert_eq!(inspect_path(&path).unwrap().pending, kinds.len() - 1);
}

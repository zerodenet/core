mod heap;
use super::*;
use crate::dispatcher::{outbox::DeliveryOutbox, PendingDelivery};
use std::collections::HashSet;
use zero_api::ApiEvent;

#[test]
fn delivery_budget_counts_queued_inflight_retries_and_releases_on_drop() {
    let budget = DeliveryBudget::new(1024);
    let first = budget.reserve(700).unwrap();
    assert!(budget.reserve(325).is_none());
    let second = budget.reserve(324).unwrap();
    assert_eq!(budget.available(), 0);
    drop(first);
    assert_eq!(budget.available(), 700);
    drop(second);
    assert!(budget.is_empty());
}
#[test]
fn oversized_delivery_is_exclusive_and_lowered_reload_budget_preserves_facts() {
    let budget = DeliveryBudget::new(1024);
    let inherited = budget.inherit(4096);
    assert_eq!(budget.available(), 0);
    assert!(budget.reserve(1).is_none());
    drop(inherited);
    let oversized = budget.reserve(4096).unwrap();
    assert!(budget.reserve(1).is_none());
    drop(oversized);
    assert!(budget.reserve(512).is_some());
}
#[test]
fn large_event_encoding_and_delivery_shares_do_not_clone_payload() {
    let event = Arc::new(ApiEvent::new(
        "large",
        "flow.completed",
        1,
        serde_json::json!({"padding":"x".repeat(16 * 1024 * 1024)}),
    ));
    let delivery = PendingDelivery::new("receiver".into(), event.clone(), true);
    let worker_event = delivery.event.clone();
    assert!(Arc::ptr_eq(&delivery.event, &worker_event));
    assert!(Arc::ptr_eq(&event, &worker_event));
    let (body, peak) = heap::measure(|| crate::encoding::to_vec(&*worker_event).unwrap());
    assert!(
        peak <= body.len() + 64 * 1024,
        "plain 16 MiB encoding extra heap: {peak}"
    );
    eprintln!("plain 16 MiB encoding extra heap {peak}; not RSS");
    assert_eq!(body.len() as u64, delivery.byte_charge);
    // Prevent Vec's normal growth from allocating 32 MiB for a 16 MiB event.
    assert!(body.capacity() <= body.len() + 1);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["event_id"], "large");
}
#[test]
fn disk_paging_checks_frame_budget_before_deserialization_and_retains_all_ids() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 1, 0).unwrap();
    for id in ["first", "second", "third"] {
        let event = ApiEvent::new(
            id,
            "flow.completed",
            1,
            serde_json::json!({"padding":"x".repeat(65536)}),
        );
        outbox
            .put_ref("receiver", &event, 7, Some("retry"))
            .unwrap();
    }
    let first = outbox
        .load_pending_with_budget(&HashSet::new(), 3, 1024, true)
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].0.event.event_id, "first");
    assert_eq!(first[0].0.attempts, 7);
    assert!(first[0].1 > 65536);
    assert!(outbox
        .load_pending_with_budget(&HashSet::new(), 3, 1024, false)
        .unwrap()
        .is_empty());
    assert_eq!(outbox.pending_counts()["receiver"], 3);
    let fit = outbox
        .load_pending_with_budget(&HashSet::new(), 3, first[0].1, false)
        .unwrap();
    assert_eq!(fit.len(), 1);
    outbox.ack("receiver", "first").unwrap();
    drop(outbox);
    let outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 1, 0).unwrap();
    let remaining = outbox
        .load_pending_with_budget(&HashSet::new(), 3, 1024, true)
        .unwrap();
    assert_eq!(remaining[0].0.event.event_id, "second");
    assert_eq!(outbox.pending_counts()["receiver"], 2);
}

#[tokio::test]
async fn real_worker_and_async_sink_share_the_original_event_allocation() {
    use crate::dispatcher::worker::SinkWorker;
    use crate::registry::{AsyncDeliverySink, ConfiguredEventSink};
    struct Capture {
        expected: Arc<zero_api::RawApiEvent>,
    }
    #[async_trait::async_trait]
    impl AsyncDeliverySink for Capture {
        async fn publish(
            &self,
            event: Arc<zero_api::RawApiEvent>,
        ) -> zero_api::ApiResult<zero_api::PublishResult> {
            assert!(
                Arc::ptr_eq(&self.expected, &event),
                "worker/sink handoff deep-cloned the event"
            );
            Ok(zero_api::PublishResult::delivered())
        }
    }
    let event = Arc::new(ApiEvent::new(
        "worker",
        "flow.completed",
        1,
        serde_json::json!({"padding":"x".repeat(65536)}),
    ));
    let sink = ConfiguredEventSink::for_test(
        Arc::new(Capture {
            expected: event.clone(),
        }),
        None,
    );
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let mut worker = SinkWorker::spawn(sink, tx);
    let prepared = worker.prepare_event(&event);
    assert!(Arc::ptr_eq(&event, &prepared));
    assert!(worker
        .try_submit(PendingDelivery::new("receiver".into(), prepared, false))
        .is_none());
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(result.result.unwrap().delivered);
    worker.stop();
    worker.join().await;
}

#[test]
fn escaped_large_record_restore_avoids_full_frame_copy_and_encoder_growth() {
    const SIZE: usize = 4 * 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut outbox = DeliveryOutbox::open(path.to_str().unwrap(), None, 1, 0).unwrap();
    let event = ApiEvent::new(
        "escaped",
        "flow.completed",
        1,
        serde_json::json!({"padding":"\n".repeat(SIZE)}),
    );
    outbox.put_ref("receiver", &event, 3, None).unwrap();
    let (loaded, peak) = heap::measure(|| {
        outbox
            .load_pending_with_budget(&HashSet::new(), 4, 1024, true)
            .unwrap()
    });
    assert_eq!(loaded.len(), 1);
    assert_eq!(
        loaded[0].0.event.payload["padding"].as_str().unwrap().len(),
        SIZE
    );
    assert!(peak <= 2 * SIZE + 256 * 1024, "restore extra heap: {peak}");
    let event = Arc::new(event);
    let (encoded, encode_peak) = heap::measure(|| {
        let delivery = PendingDelivery::new("receiver".into(), event.clone(), true);
        let worker_event = delivery.event.clone();
        crate::encoding::to_vec(&*worker_event).unwrap()
    });
    assert!(
        encode_peak <= encoded.len() + 64 * 1024,
        "handoff/encode extra heap: {encode_peak}"
    );
    eprintln!("4 MiB decoded / {} encoded bytes: restore extra heap {peak}; shared handoff/encode extra heap {encode_peak}; not RSS", encoded.len());
    assert_eq!(outbox.pending_counts()["receiver"], 1);
}

#[test]
fn jsonl_delivery_does_not_materialize_an_entire_encoded_frame() {
    use zero_api::EventSink;
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let event = ApiEvent::new(
        "jsonl",
        "flow.completed",
        1,
        serde_json::json!({"padding":"x".repeat(16 * 1024 * 1024)}),
    );
    let size = serialized_bytes(&event) as usize;
    let sink = zero_api::JsonLineEventSink::new(Counter(0));
    let (result, peak) = heap::measure(|| sink.publish(&event).unwrap());
    assert!(result.delivered);
    assert_eq!(sink.into_inner().unwrap().0, size + 1);
    assert!(peak <= 64 * 1024, "JSONL extra heap: {peak}");
    eprintln!("16 MiB JSONL event extra heap: {peak}; not RSS");
}

#[test]
fn failed_jsonl_flush_is_not_retried_by_buffer_drop_or_reported_as_success() {
    use zero_api::EventSink;
    struct Fails(Arc<AtomicU64>);
    impl std::io::Write for Fails {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(std::io::Error::other("write blocked"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let calls = Arc::new(AtomicU64::new(0));
    let sink = zero_api::JsonLineEventSink::new(Fails(calls.clone()));
    let event = ApiEvent::new("fail", "flow.completed", 1, serde_json::json!({"value":1}));
    let error = sink.publish(&event).unwrap_err();
    assert_eq!(error.message, "failed to write event sink output");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn waiting_source_override_reuses_one_prepared_input_and_preserves_original() {
    use crate::dispatcher::{worker::SinkWorker, BlockedEvent};
    use crate::registry::{AsyncDeliverySink, ConfiguredEventSink};
    struct Unused;
    #[async_trait::async_trait]
    impl AsyncDeliverySink for Unused {
        async fn publish(
            &self,
            _: Arc<zero_api::RawApiEvent>,
        ) -> zero_api::ApiResult<zero_api::PublishResult> {
            unreachable!("preparation must not publish")
        }
    }
    let sink = ConfiguredEventSink::for_test(Arc::new(Unused), Some("override".into()));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut worker = SinkWorker::spawn(sink, tx);
    let original = ApiEvent::new(
        "waiting",
        "flow.completed",
        1,
        serde_json::json!({"padding":"x".repeat(65536)}),
    );
    let mut blocked = BlockedEvent::new(original);
    let (first, bytes) = blocked.prepare_for(&worker);
    for _ in 0..16 {
        let (next, next_bytes) = blocked.prepare_for(&worker);
        assert!(Arc::ptr_eq(&first, &next));
        assert_eq!(bytes, next_bytes);
    }
    assert_eq!(first.source_id.as_deref(), Some("override"));
    assert_eq!(blocked.event.source_id, None);
    assert_eq!(blocked.event.event_id, first.event_id);
    worker.stop();
    worker.join().await;
}

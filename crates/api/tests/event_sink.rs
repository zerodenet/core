use std::io::Cursor;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use zero_api::{
    event_type, ApiEvent, CallbackEventSink, EventSink, JsonLineEventSink, PublishResult,
    RawApiEvent,
};

fn event(id: &str, event_type: &str) -> RawApiEvent {
    ApiEvent::new(id, event_type, 1_760_000_000_000, json!({ "value": id }))
}

#[test]
fn callback_event_sink_publishes_to_in_process_callback() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_for_callback = Arc::clone(&seen);
    let sink = CallbackEventSink::new("test-callback", move |event: &RawApiEvent| {
        seen_for_callback
            .lock()
            .expect("seen lock")
            .push(event.event_id.clone());
        Ok(PublishResult::delivered())
    });

    let result = sink
        .publish(&event("event-1", event_type::FLOW_COMPLETED))
        .expect("publish");

    assert!(result.delivered);
    assert_eq!(seen.lock().expect("seen lock").as_slice(), ["event-1"]);
}

#[test]
fn json_line_event_sink_writes_normalized_events() {
    let sink = JsonLineEventSink::new(Cursor::new(Vec::new()));

    sink.publish(&event("event-1", event_type::FLOW_COMPLETED))
        .expect("publish first event");
    sink.publish(&event("event-2", event_type::STATS_SAMPLED))
        .expect("publish second event");

    let cursor = sink.into_inner().expect("sink writer");
    let written = String::from_utf8(cursor.into_inner()).expect("utf-8 output");
    let lines = written.lines().collect::<Vec<_>>();

    assert_eq!(lines.len(), 2);

    let first = serde_json::from_str::<Value>(lines[0]).expect("first event json");
    let second = serde_json::from_str::<Value>(lines[1]).expect("second event json");

    assert_eq!(first["event_id"], "event-1");
    assert_eq!(first["event_type"], "flow.completed");
    assert_eq!(first["payload"]["value"], "event-1");
    assert_eq!(second["event_id"], "event-2");
    assert_eq!(second["event_type"], "stats.sampled");
}

#[test]
fn poisoned_jsonl_writer_is_returned_with_failure_and_original_identity() {
    struct PanicWriter {
        identity: Arc<()>,
    }
    impl std::io::Write for PanicWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            panic!("actual writer panic while original lock is held")
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let identity = Arc::new(());
    let sink = JsonLineEventSink::new(PanicWriter {
        identity: identity.clone(),
    });
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || sink.publish(&event("panic", "test.fact"))
    ))
    .is_err());
    match sink.into_inner_recover() {
        Ok(_) => panic!("original poisoned mutex was reported as success"),
        Err((writer, error)) => {
            assert!(Arc::ptr_eq(&identity, &writer.identity));
            assert!(error.message.contains("poisoned"));
        }
    }
}

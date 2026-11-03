//! The same production worker is prepared once, then activated or actually joined.
use super::*;
use zero_connector::{prepare_event_dispatcher_paused_with_network, EventDispatcherNetwork};

fn api(path: &std::path::Path) -> ApiConfig {
    ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "prepared".into(),
            path: path.display().to_string(),
            events: vec![event_type::ENGINE_STARTED.into()],
            source_id: None,
        }],
        ..Default::default()
    }
}
fn source() -> StaticEventSource {
    let mut event = ApiEvent::new("paused-started", event_type::ENGINE_STARTED, 1, json!({}));
    event.sequence = Some(1);
    StaticEventSource {
        events: Arc::new(Mutex::new(vec![event])),
    }
}

#[tokio::test]
async fn prepared_worker_cannot_deliver_until_publication_and_handoff_retains_bootstrap() {
    let path = temp_path("prepared-publication.jsonl");
    let mut original = prepare_event_dispatcher_paused_with_network(
        source(),
        api(&path),
        None,
        EventDispatcherOptions::default(),
        EventDispatcherNetwork::system(),
        true,
        None,
    );
    assert!(original.wait_ready().await.unwrap());
    // The original joins all sink/source tasks while still unpublished. No
    // delivery is inferred from a sleep, abort, or a free port.
    let continuation = original.retire_prepared_retained().await.unwrap().unwrap();
    assert_eq!(original.original_child_failure_count(), 0);
    assert!(!fs::read_to_string(&path)
        .unwrap_or_default()
        .contains("paused-started"));
    let mut resumed = prepare_event_dispatcher_paused_with_network(
        source(),
        api(&path),
        None,
        EventDispatcherOptions::default(),
        EventDispatcherNetwork::system(),
        false,
        Some(continuation),
    );
    assert!(resumed.wait_ready().await.unwrap());
    resumed.activate().unwrap();
    wait_for_file_contains(&path, "paused-started").await;
    resumed.shutdown_retained().await.unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap().lines().count(), 1);
    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn failed_original_preparation_is_retained_and_joined_before_replacement() {
    let path = temp_path("prepared-invalid-directory");
    fs::create_dir(&path).unwrap();
    let mut original = prepare_event_dispatcher_paused_with_network(
        source(),
        api(&path),
        None,
        EventDispatcherOptions::default(),
        EventDispatcherNetwork::system(),
        false,
        None,
    );
    assert!(original.wait_ready().await.is_err());
    let failure = original.original_preparation_failure().unwrap();
    assert!(original.retire_prepared_retained().await.unwrap().is_none());
    assert!(Arc::ptr_eq(
        &failure,
        &original.original_preparation_failure().unwrap()
    ));
    assert_eq!(original.original_child_failure_count(), 0);
    fs::remove_dir(path).unwrap();
}

struct PanickingStream;
impl EventStream for PanickingStream {
    fn recv(&self) -> Option<RawApiEvent> {
        panic!("actual source worker failed")
    }
    fn try_recv(&self) -> Option<RawApiEvent> {
        panic!("actual source worker failed")
    }
}
#[derive(Clone)]
struct PanickingSource(StaticEventSource);
impl EventSource for PanickingSource {
    type Stream = PanickingStream;
    fn subscribe(&self, _: EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(PanickingStream)
    }
    fn latest(&self, limit: usize, filter: EventFilter) -> zero_api::ApiResult<Vec<RawApiEvent>> {
        self.0.latest(limit, filter)
    }
    fn since(
        &self,
        sequence: u64,
        limit: usize,
        filter: EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        self.0.since(sequence, limit, filter)
    }
}

#[tokio::test]
async fn real_source_thread_failure_remains_sticky_after_actual_join() {
    let path = temp_path("prepared-source-panic.jsonl");
    let mut original = prepare_event_dispatcher_paused_with_network(
        PanickingSource(source()),
        api(&path),
        None,
        EventDispatcherOptions::default(),
        EventDispatcherNetwork::system(),
        false,
        None,
    );
    assert!(original.wait_ready().await.unwrap());
    assert!(original.retire_prepared_retained().await.is_err());
    assert_eq!(original.original_child_failure_count(), 1);
    assert!(original.retire_prepared_retained().await.is_err());
    assert_eq!(original.original_child_failure_count(), 1);
    assert!(!fs::read_to_string(&path)
        .unwrap_or_default()
        .contains("paused-started"));
    let _ = fs::remove_file(path);
}

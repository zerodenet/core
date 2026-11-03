use super::*;
use zero_api::{ApiResult, EventReplay};
use zero_config::{ApiConfig, EventSinkConfig};
#[derive(Clone)]
struct EmptySource;
struct EmptyStream;
impl EventStream for EmptyStream {
    fn recv(&self) -> Option<RawApiEvent> {
        None
    }
    fn try_recv(&self) -> Option<RawApiEvent> {
        None
    }
}
impl EventSource for EmptySource {
    type Stream = EmptyStream;
    fn subscribe(&self, _: EventFilter) -> ApiResult<Self::Stream> {
        Ok(EmptyStream)
    }
    fn latest(&self, _: usize, _: EventFilter) -> ApiResult<Vec<RawApiEvent>> {
        Ok(Vec::new())
    }
    fn since(&self, sequence: u64, _: usize, _: EventFilter) -> ApiResult<EventReplay> {
        Ok(EventReplay {
            core_instance_id: "real-panic-core".into(),
            requested_after: sequence,
            actual_from: sequence.saturating_add(1),
            has_gap: false,
            events: Vec::new(),
        })
    }
}
#[tokio::test]
async fn parent_dispatcher_panic_retains_original_sink_and_source_handles_for_separate_join() {
    let directory = tempfile::tempdir().unwrap();
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "original".into(),
            path: directory.path().join("events").display().to_string(),
            events: Vec::new(),
            source_id: None,
        }],
        ..Default::default()
    };
    let mut original = prepare_event_dispatcher_paused_with_network(
        EmptySource,
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            ..Default::default()
        },
        EventDispatcherNetwork::system(),
        false,
        None,
    );
    assert!(original.wait_ready().await.unwrap());
    let children = original.children.clone();
    assert!(children
        .source
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .task
        .is_some());
    let sink = children.asynchronous.lock().unwrap()[0].clone();
    let sink_id = sink.state.lock().await.task.id();
    // Poison the actual statistics lock consumed by the real dispatcher loop.
    let stats = original.sink_stats.clone();
    assert!(std::thread::spawn(move || {
        let _guard = stats.lock().unwrap();
        panic!("original dispatcher loop poison")
    })
    .join()
    .is_err());
    let failure = tokio::time::timeout(Duration::from_secs(3), original.observe_shutdown())
        .await
        .unwrap()
        .err()
        .unwrap();
    assert!(failure.is_panic());
    assert!(children
        .source
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .task
        .is_some());
    assert!(sink.state.lock().await.exit.is_none()); // parent exit was not child join
    assert!(original.retire_prepared_retained().await.is_err());
    assert!(children
        .source
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .task
        .is_none());
    let state = sink.state.lock().await;
    assert_eq!(state.task.id(), sink_id);
    assert!(state.exit.is_some());
    drop(state);
    assert!(Arc::ptr_eq(
        &failure,
        &original.original_shutdown_failure().unwrap()
    ));
    original.resources.close().unwrap(); // all real views returned; distinct native receipts
}

#[tokio::test]
async fn cancelled_publish_waiter_keeps_actual_blocking_job_until_join_and_native_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-publish");
    let resources = DispatcherResources::default();
    let mut writer = resources.open(|| std::fs::File::create(&path)).unwrap();
    let children = Arc::new(DispatcherChildren::default());
    let (entered, received) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let owner = children.clone();
    let caller = tokio::spawn(async move {
        owner
            .blocking_publish(move || {
                let _ = entered.send(());
                released.recv().unwrap();
                use std::io::Write;
                writer.write_all(b"actual original job completed").unwrap();
                Ok(zero_api::PublishResult::delivered())
            })
            .await
    });
    received.await.unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    let child = children.asynchronous.lock().unwrap()[0].clone();
    assert!(child.state.lock().await.exit.is_none());
    assert_eq!(resources.original_file_count_for_test(), 1);
    release.send(()).unwrap();
    children.join_all().await;
    assert!(child.state.lock().await.exit.as_ref().unwrap().is_ok());
    resources.close().unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"actual original job completed"
    );
}

//! A late actual statistics charge cannot partially publish the control book.
use super::*;
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::{Context, Poll, Wake, Waker};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, PreparedConfiguration};
struct Reenter {
    engine: Engine,
    woke: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.engine.emit_event(zero_api::RawApiEvent::new(
            "actual-reentrant-wake",
            zero_api::event_type::STATS_RESET,
            0,
            serde_json::json!({"wake": true}),
        ));
        self.woke.store(true, Ordering::Release);
    }
}
fn config() -> RuntimeConfig {
    RuntimeConfig::parse(
        r#"{"runtime":{"control_max_pending_commands":1},"route":{"final":{"type":"direct"}}}"#,
    )
    .unwrap()
}
#[test]
fn late_statistics_original_refusal_keeps_control_metadata_and_actual_generation_then_recovers() {
    let engine = Engine::new(config()).unwrap();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&engine.config().runtime));
    let mut target = config();
    target.runtime.control_max_pending_commands = Some(2);
    target.runtime.event_log_stats_max_bytes = Some(1);
    let prepared =
        PreparedConfiguration::prepare_owned(target.clone(), engine.configuration_source_owner())
            .unwrap();
    let snapshot = engine.prepare_configuration_snapshot(prepared).unwrap();
    let candidate = book
        .prepare(ControlBudgetLimits::from_runtime(&target.runtime))
        .unwrap();
    let before = engine.latest_event_sequence();
    engine.emit_event(zero_api::RawApiEvent::new(
        "actual-late-stats",
        zero_api::event_type::STATS_RESET,
        0,
        serde_json::json!({"original":"original statistics payload"}),
    ));
    let pointer = {
        let history = engine.original_event_history();
        std::sync::Arc::as_ptr(history.next(before).2.unwrap())
    };
    assert!(engine
        .publish_prepared_snapshot_with_control(snapshot, Some(&candidate))
        .is_err());
    assert_eq!(
        engine.config().runtime.control_max_pending_commands,
        Some(1)
    );
    assert_eq!(book.limits().messages.pending_commands, 1);
    assert!(candidate.is_pending().unwrap() && !candidate.metadata_committed().unwrap());
    candidate.retire().unwrap();
    target.runtime.event_log_stats_max_bytes = None;
    let prepared =
        PreparedConfiguration::prepare_owned(target.clone(), engine.configuration_source_owner())
            .unwrap();
    let snapshot = engine.prepare_configuration_snapshot(prepared).unwrap();
    let candidate = book
        .prepare(ControlBudgetLimits::from_runtime(&target.runtime))
        .unwrap();
    let reenter = Arc::new(Reenter {
        engine: engine.clone(),
        woke: AtomicBool::new(false),
    });
    let waker = Waker::from(reenter.clone());
    let mut context = Context::from_waker(&waker);
    let changed = book.capacity_changed();
    tokio::pin!(changed);
    assert!(matches!(changed.as_mut().poll(&mut context), Poll::Pending));
    engine
        .publish_prepared_snapshot_with_control(snapshot, Some(&candidate))
        .unwrap();
    assert!(candidate.metadata_committed().unwrap());
    assert!(
        !reenter.woke.load(Ordering::Acquire),
        "pure metadata commit woke a potentially reentrant reader under Engine guards"
    );
    candidate.publish_committed().unwrap();
    assert!(reenter.woke.load(Ordering::Acquire));
    assert_eq!(book.limits().messages.pending_commands, 2);
    assert_eq!(
        engine.config().runtime.control_max_pending_commands,
        Some(2)
    );
    let history = engine.original_event_history();
    assert_eq!(
        std::sync::Arc::as_ptr(history.next(before).2.unwrap()),
        pointer,
        "valid actual history was not evicted to publish a smaller limit"
    );
}

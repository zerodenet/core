//! One original's concrete protocol-close phase, polled by its existing Root.
//! No new owner, task, Scope, native permission or admission queue is created.
use crate::contract::{lifecycle::CloseReport, CancellationToken, ChannelError, CloseReason};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
#[path = "original_protocol/handle.rs"]
mod handle;
#[path = "original_protocol/operation.rs"]
mod operation;
#[path = "original_protocol/polling.rs"]
mod polling;
#[path = "original_protocol/receiver.rs"]
mod receiver;
#[path = "original_protocol/report.rs"]
mod report;
use tokio::{sync::Mutex as AsyncMutex, time::Instant};

/// Request only this exact original's already-installed cleanup. It cannot
/// acquire, perform I/O, confirm users/native closure, or accept a task exit.
#[derive(Clone)]
pub struct CarrierProtocolRetirement {
    phase: Weak<Phase>,
    settled: Arc<AtomicBool>,
}
pub struct RejectedOriginalProtocolControl {
    pub error: ChannelError,
    pub control: crate::contract::lifecycle::BoxChannelControl,
}
pub(crate) struct Phase {
    root_bound: bool,
    instance: zero_core::component::ComponentInstanceId,
    requested: CancellationToken,
    waker: Mutex<Option<std::task::Waker>>,
    control: Arc<AsyncMutex<Option<crate::contract::lifecycle::BoxChannelControl>>>,
    operation: Mutex<operation::Operation>,
    operation_waker: std::task::Waker,
    changed: tokio::sync::Notify,
    settled: Arc<AtomicBool>,
    state: Mutex<State>,
    fault_sources: Arc<crate::contract::lifecycle::OriginalProtocolFailures>,
}
#[derive(Default)]
struct State {
    retirement_started: bool,
    receiver: Option<crate::controlled_carrier::ProtocolOriginalReceiver>,
    complete: bool,
    report: CloseReport,
    first_fault: Option<Arc<ChannelError>>,
    resource_fault: Option<Arc<crate::contract::lifecycle::ResourceFailure>>,
}

impl Phase {
    pub(crate) fn new(
        control: crate::contract::lifecycle::BoxChannelControl,
        instance: zero_core::component::ComponentInstanceId,
        root_bound: bool,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            instance,
            root_bound,
            requested: CancellationToken::new(),
            waker: Mutex::default(),
            control: Arc::new(AsyncMutex::new(Some(control))),
            operation: Mutex::default(),
            operation_waker: std::task::Waker::from(Arc::new(operation::OriginalWake(
                weak.clone(),
            ))),
            changed: tokio::sync::Notify::new(),
            settled: Arc::new(AtomicBool::new(false)),
            state: Mutex::default(),
            fault_sources: Arc::default(),
        })
    }
    pub(crate) fn root_bound(&self) -> bool {
        self.root_bound
    }
    pub(crate) fn handle(self: &Arc<Self>) -> CarrierProtocolRetirement {
        CarrierProtocolRetirement {
            phase: Arc::downgrade(self),
            settled: self.settled.clone(),
        }
    }
    pub(crate) fn request(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retirement_started = true;
        self.requested.cancel();
        self.wake();
    }
    pub(crate) fn register_waker(&self, waker: &std::task::Waker) {
        let replacement = waker.clone();
        let previous = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(replacement);
        drop(previous);
    }
    fn wake(&self) {
        self.operation_wake();
    }
    fn wake_now(&self) {
        self.changed.notify_waiters();
        let wake = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.requested.clone()
    }
    pub(crate) fn requested(&self) -> bool {
        self.requested.is_cancelled()
    }
    pub(crate) fn complete(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .complete
    }
    fn receive(&self, result: Result<CloseReport, ChannelError>) -> bool {
        self.receive_result(result, false)
    }
    fn receive_panic(&self, source: ChannelError) -> bool {
        self.receive_result(Err(source), true)
    }
    fn receive_result(&self, result: Result<CloseReport, ChannelError>, poll_panic: bool) -> bool {
        let (report, failure) = match result {
            Ok(report) => (Some(report), None),
            Err(source) => (None, Some(Arc::new(source))),
        };
        let mut retired_failures = Vec::new();
        let complete = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(report) = report {
                state.complete |= report.is_complete();
                report::merge(&mut state.report, report, &mut retired_failures);
            }
            if let Some(source) = &failure {
                self.fault_sources.record(poll_panic, source.clone());
                if state.first_fault.is_none() {
                    // This controller is installed in the same declared
                    // original's Host resource entry, not a synthetic task.
                    // Canonical row and Root failure observation share exactly
                    // this returned source Arc and the real Slot generation.
                    let actual = state
                        .resource_fault
                        .get_or_insert_with(|| {
                            Arc::new(crate::contract::lifecycle::ResourceFailure::new(
                                self.instance,
                                crate::contract::lifecycle::ResourceFailurePhase::DeclaredResources,
                                source.clone(),
                            ).with_original_sources(self.fault_sources.clone()))
                        })
                        .clone();
                    state.first_fault = Some(source.clone());
                    state.report.resource_failures.push(actual);
                }
            }
            state.complete
        };
        // Duplicate typed errors may own arbitrary original sources. Their
        // destructors, just like control Drop/wakers, run outside state locks.
        drop(retired_failures);
        drop(failure);
        complete
    }
    pub(crate) fn resource_failure(
        &self,
    ) -> Option<Arc<crate::contract::lifecycle::ResourceFailure>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resource_fault
            .clone()
    }
    pub(crate) fn fault_received(&self) -> bool {
        self.resource_failure()
            .is_none_or(|fault| fault.ownership_received())
    }
    pub(crate) fn failure(&self) -> Option<Arc<ChannelError>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .first_fault
            .clone()
    }
    pub(crate) fn take_report(&self) -> CloseReport {
        std::mem::take(
            &mut self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .report,
        )
    }
    /// Written only by the owning original book after its exact charged Slot
    /// was removed. Protocol completion alone cannot release this strong owner.
    pub(crate) fn settle(&self) {
        assert!(
            self.complete(),
            "protocol return precedes whole-original settlement"
        );
        let retired = self
            .control
            .try_lock()
            .expect("completed operation released control")
            .take();
        self.settled.store(true, Ordering::Release);
        self.wake();
        drop(retired);
    }
}

impl std::fmt::Debug for RejectedOriginalProtocolControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RejectedOriginalProtocolControl")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

/// Exact panic payload from this inline original operation, never a fake task
/// outcome. The Root fault cell retains it before any recovery attempt.
pub(crate) struct OriginalPollPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for OriginalPollPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f.debug_struct("OriginalPollPanic")
            .field("payload_type", &payload.as_ref().type_id())
            .finish()
    }
}
impl std::fmt::Display for OriginalPollPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original protocol cleanup poll panicked")
    }
}
impl std::error::Error for OriginalPollPanic {}
#[cfg(test)]
impl OriginalPollPanic {
    pub(crate) fn contains<T: std::any::Any>(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is::<T>()
    }
}

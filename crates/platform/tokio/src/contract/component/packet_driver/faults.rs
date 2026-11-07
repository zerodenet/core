//! Finite error custody of the actual originals; no IO/controller/Scope here.
use super::*;
use crate::contract::CleanupFailureReceipt;
use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicBool, Ordering},
};
#[derive(Debug, Default)]
pub(super) struct Phase(OnceLock<Arc<ChannelError>>);
impl Phase {
    #[cfg(test)]
    pub(super) fn actual(&self) -> Option<&Arc<ChannelError>> {
        self.0.get()
    }

    pub(super) fn failed(&self) -> bool {
        self.0.get().is_some()
    }
}
#[derive(Debug, Default)]
pub(super) struct PendingFailures {
    pub(super) native: Phase,
    pub(super) logical: Phase,
}
#[derive(Debug, Default)]
pub(super) struct RecordFailures {
    pub(super) final_control: Phase,
    pending: Mutex<Vec<Arc<PendingFailures>>>,
    streams: Mutex<Vec<Arc<Phase>>>,
    failed: AtomicBool,
}
impl RecordFailures {
    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
    pub(super) fn add_pending(&self, actual: Arc<PendingFailures>) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(actual);
    }
    pub(super) fn add_stream(&self, actual: Arc<Phase>) {
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(actual);
    }
    pub(super) fn transferred(&self, actual: &Arc<PendingFailures>) {
        // Handoff removes only empty Source-local metadata. Already delivered
        // faults stay in the same finite payload held by the real receiver.
        if actual.native.failed() || actual.logical.failed() {
            return;
        }
        let removed = {
            let mut rows = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.iter()
                .position(|same| Arc::ptr_eq(same, actual))
                .map(|at| rows.remove(at))
        };
        drop(removed);
    }
}
#[derive(Default)]
pub(super) struct ActorFailures {
    pub(super) result: Phase,
    pub(super) acceptance: Phase,
}
impl ActorFailures {
    pub(super) fn failed(&self) -> bool {
        self.result.failed() || self.acceptance.failed()
    }
}
#[derive(Default)]
struct Payload {
    first: OnceLock<Arc<ChannelError>>,
    failed: AtomicBool,
    records: Mutex<Vec<Arc<RecordFailures>>>,
    actors: Mutex<Vec<ActorFailuresRef>>,
}
mod format;
impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actual Packet driver cleanup phase failure")
    }
}
impl Error for Payload {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.first
            .get()
            .map(|same| same.as_ref() as &(dyn Error + 'static))
    }
}
/// Receipt owns only the finite pure-metadata payload, never this wrapper or
/// any actual controller. The outgoing Error and receipt cannot form a cycle.
pub(super) struct Delivery {
    payload: Arc<Payload>,
    receipt: CleanupFailureReceipt,
    report: OnceLock<Arc<ChannelError>>,
}
impl Delivery {
    pub(super) fn new() -> Arc<Self> {
        let payload = Arc::new(Payload::default());
        let receipt = CleanupFailureReceipt::new();
        let erased: Arc<dyn Error + Send + Sync> = payload.clone();
        assert!(receipt.bind_payload(erased));
        Arc::new(Self {
            payload,
            receipt,
            report: OnceLock::new(),
        })
    }
    pub(super) fn register(&self, actual: Arc<RecordFailures>) {
        self.payload
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(actual);
    }
    pub(super) fn remove_empty(&self, actual: &Arc<RecordFailures>) {
        if actual.failed() {
            return;
        }
        let removed = {
            let mut rows = self
                .payload
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.iter()
                .position(|same| Arc::ptr_eq(same, actual))
                .map(|at| rows.remove(at))
        };
        drop(removed);
    }
    pub(super) fn register_actor(&self, actual: ActorFailuresRef) {
        self.payload
            .actors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(actual);
    }
    pub(super) fn remove_empty_actor(&self, actual: &ActorFailuresRef) {
        if actual.failed() {
            return;
        }
        let removed = {
            let mut rows = self
                .payload
                .actors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.iter()
                .position(|same| ActorFailuresRef::ptr_eq(same, actual))
                .map(|at| rows.remove(at))
        };
        drop(removed);
    }
    pub(super) fn retain_actor(&self, phase: &Phase, source: Arc<ChannelError>) {
        if let Some(previous) = phase.0.get() {
            assert!(
                Arc::ptr_eq(previous, &source),
                "same actual Task result must remain stable"
            );
            return;
        }
        self.payload.failed.store(true, Ordering::Release);
        let _ = phase.0.set(source.clone());
        let _ = self.payload.first.set(source.clone());
        if let Some(receipt) = source.cleanup_failure_receipt() {
            let _ = receipt.acknowledge_retained(&source);
        }
    }
    pub(super) fn failed(&self) -> bool {
        self.payload.failed.load(Ordering::Acquire)
    }
    pub(super) fn acknowledged_or_empty(&self) -> bool {
        !self.failed() || self.acknowledged()
    }
    pub(super) fn acknowledged(&self) -> bool {
        self.receipt.is_acknowledged()
    }
    pub(super) fn report(&self) -> Option<ChannelError> {
        let first = self.payload.first.get()?;
        let actual = self.report.get_or_init(|| {
            Arc::new(
                ChannelError::retained(first.clone())
                    .with_source(self.payload.clone())
                    .with_cleanup_failure_receipt(self.receipt.clone()),
            )
        });
        Some(ChannelError::retained(actual.clone()))
    }
    pub(super) fn receive(
        &self,
        record: &RecordFailures,
        phase: &Phase,
        source: ChannelError,
    ) -> ChannelError {
        // A nested retained wrapper for this same report must never be put back
        // into its own payload. Ordinary graph cycles remain rejected by Host.
        if source
            .cleanup_failure_receipt()
            .is_some_and(|same| same.same_receipt(&self.receipt))
        {
            return self.report().expect("same issued cleanup report");
        }
        if let Some(previous) = phase.0.get() {
            if previous
                .cleanup_failure_receipt()
                .zip(source.cleanup_failure_receipt())
                .is_some_and(|(old, actual)| old.same_receipt(actual))
            {
                return self
                    .report()
                    .expect("same retained controller cleanup payload");
            }
            unreachable!("an unbound failed phase cannot execute another cleanup action");
        }
        let source = Arc::new(source);
        self.payload.failed.store(true, Ordering::Release);
        record.failed.store(true, Ordering::Release);
        phase
            .0
            .set(source.clone())
            .unwrap_or_else(|_| unreachable!("failed phase is never invoked twice"));
        record.failed.store(true, Ordering::Release);
        let _ = self.payload.first.set(source.clone());
        // First persist the exact original Arc in the bound stable payload;
        // this local receiver may now acknowledge that exact child receipt.
        // Native/users/task registrations still need their separate evidence.
        if let Some(receipt) = source.cleanup_failure_receipt() {
            let _ = receipt.acknowledge_retained(&source);
        }
        self.report().expect("new actual phase failure")
    }
}
impl Record {
    pub(super) fn receive_phase(
        &self,
        phase: &Phase,
        result: Result<CloseReport, ChannelError>,
    ) -> Result<CloseReport, ChannelError> {
        result.map_err(|source| {
            let original = self.delivery.receive(&self.failures, phase, source);
            self.source_failures.receive(&self.forwarded, original)
        })
    }
    pub(super) fn failed_phase(&self, phase: &Phase) -> Option<ChannelError> {
        phase.failed().then(|| {
            self.source_failures
                .report()
                .unwrap_or_else(|| self.delivery.report().expect("retained actual failure"))
        })
    }
}

#[cfg(test)]
#[path = "tests/faults.rs"]
mod tests;

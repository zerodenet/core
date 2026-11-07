//! The actual Source's stable pure payload. Per-attempt/actor receipts are
//! accepted only after their exact Arc enters this payload. This owns no IO,
//! controller, TaskScope, TaskJoinWitness, or Source/Host back edge.
use super::*;
use crate::contract::CleanupFailureReceipt;
use std::{error::Error, fmt};
#[derive(Default)]
pub(super) struct Forwarded(OnceLock<Arc<ChannelError>>);
#[derive(Default)]
struct Payload {
    first: OnceLock<Arc<ChannelError>>,
    rows: Mutex<Vec<Arc<Forwarded>>>,
}
impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ActualSourceCleanupFailures[")?;
        let count = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        for index in 0..count {
            let row = self
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(index)
                .cloned();
            if let Some(source) = row.as_ref().and_then(|row| row.0.get()) {
                write!(f, "{source:?};")?;
            }
        }
        f.write_str("]")
    }
}
impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same actual Source cleanup failures")
    }
}
impl Error for Payload {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.first
            .get()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}
pub(super) struct SourceDelivery {
    payload: Arc<Payload>,
    receipt: CleanupFailureReceipt,
    report: OnceLock<Arc<ChannelError>>,
}
impl SourceDelivery {
    pub(super) fn new() -> Self {
        let payload = Arc::new(Payload::default());
        let receipt = CleanupFailureReceipt::new();
        let erased: Arc<dyn Error + Send + Sync> = payload.clone();
        assert!(receipt.bind_payload(erased));
        Self {
            payload,
            receipt,
            report: OnceLock::new(),
        }
    }
    /// The original caller installs this cell before its first callee/actor IO.
    /// Its owning metadata reservation must cover the node and table capacity.
    pub(super) fn prepare(&self) -> Arc<Forwarded> {
        let actual = Arc::new(Forwarded::default());
        self.payload
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(actual.clone());
        actual
    }
    pub(super) fn remove_empty(&self, actual: &Arc<Forwarded>) {
        if actual.0.get().is_some() {
            return;
        }
        let removed = {
            let mut rows = self
                .payload
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.iter()
                .position(|same| Arc::ptr_eq(same, actual))
                .map(|index| rows.remove(index))
        };
        drop(removed);
    }
    pub(super) fn receive(&self, row: &Forwarded, source: ChannelError) -> ChannelError {
        if self.owns(&source) {
            return self.report().expect("same issued Source report");
        }
        let actual = Arc::new(source);
        let retained = row.0.get_or_init(|| actual.clone());
        let matches = Arc::ptr_eq(retained, &actual)
            || retained
                .cleanup_failure_receipt()
                .zip(actual.cleanup_failure_receipt())
                .is_some_and(|(old, incoming)| old.same_receipt(incoming));
        if !matches {
            // No spare phase was declared for a different producer. It remains
            // retained by that producer and its receipt is never acknowledged.
            return ChannelError::retained(actual);
        }
        let _ = self.payload.first.set(retained.clone());
        // The pure payload already holds this exact report Arc. Do not infer
        // this child receipt's acceptance from an older outer Resource ACK.
        if let Some(receipt) = retained.cleanup_failure_receipt() {
            receipt.acknowledge_retained(retained);
        }
        self.report().expect("new actual Source cause")
    }
    pub(super) fn owns(&self, source: &ChannelError) -> bool {
        source
            .cleanup_failure_receipt()
            .is_some_and(|actual| actual.same_receipt(&self.receipt))
    }
    pub(super) fn report(&self) -> Option<ChannelError> {
        let first = self.payload.first.get()?;
        let source = self.report.get_or_init(|| {
            Arc::new(
                ChannelError::retained(first.clone())
                    .with_source(self.payload.clone())
                    .with_cleanup_failure_receipt(self.receipt.clone()),
            )
        });
        Some(ChannelError::retained(source.clone()))
    }
    #[cfg(test)]
    pub(super) fn retains_child(&self, actual: &ChannelError) -> bool {
        self.payload
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|row| {
                row.0.get().is_some_and(|same| {
                    same.cleanup_failure_receipt()
                        .zip(actual.cleanup_failure_receipt())
                        .is_some_and(|(stored, issued)| stored.same_receipt(issued))
                })
            })
    }
    pub(super) fn accepted_or_empty(&self) -> bool {
        self.payload.first.get().is_none() || self.receipt.is_acknowledged()
    }
}

impl PacketDriverCalls {
    /// The retaining Source owner may forward its own actual listener-close
    /// error through the same pure payload. This executes no close and makes
    /// no native/user/Scope completion claim.
    pub fn receive_source_native_failure(
        &self,
        actual: &CarrierOrigin,
        source: ChannelError,
    ) -> ChannelError {
        if !self.original.same_original(actual) {
            return failure(Reason::StalePlan).with_source(source);
        }
        self.source_failures.receive(&self.native_failure, source)
    }
}

#[cfg(test)]
#[path = "tests/source_failures.rs"]
mod tests;

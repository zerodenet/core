//! Bounded concrete virtual Stream controllers retained before their first SYN.
use super::*;
use crate::controlled_carrier::loan::{LoanOwner, StreamLoan};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};
pub(super) struct RetainedStream {
    record: RecordWeak,
    original: CarrierOrigin,
    failure: Arc<faults::Phase>,
    owner: OnceLock<Arc<AsyncMutex<LoanOwner>>>,
    cancel: OnceLock<crate::controlled_carrier::loan::packet_driver::OriginalCancellation>,
    abandoned: AtomicBool,
    complete: AtomicBool,
}
pub struct PacketDriverStreamGrant {
    actual: Arc<RetainedStream>,
}
/// Read-only exact custody, not IO, new acquisition or Source/Scope-close proof.
#[derive(Clone)]
pub struct PacketDriverStreamRetained(Weak<RetainedStream>);
#[derive(Clone)]
pub(crate) struct StreamCancellation(Arc<RetainedStream>);
impl StreamCancellation {
    pub(crate) fn cancel(&self, _: CloseReason) {
        self.0.request_retirement();
    }
}
impl super::attempt::PacketDriverPendingCustody {
    /// Reserve this concrete donor before constructing/polling its cold opening.
    /// The finite set is the actual host-resolved dependency providers, not a
    /// caller-supplied capacity or an arbitrary same-Scope identity.
    pub fn reserve_stream(
        &self,
        original: &CarrierOrigin,
    ) -> Result<PacketDriverStreamGrant, ChannelError> {
        let record = self.record()?;
        if record.call.cancel.is_cancelled()
            || record.caller.is_joined()
            || original.is_retired()
            || !record
                .packet_originals
                .iter()
                .any(|actual| actual.same_original(original))
        {
            return Err(failure(Reason::StalePlan));
        }
        let mut streams = record
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if record.call.cancel.is_cancelled() {
            return Err(failure(Reason::Revoked));
        }
        if streams.len() >= record.packet_originals.len()
            || streams
                .iter()
                .any(|actual| actual.original.same_original(original))
        {
            return Err(failure(Reason::Capacity));
        }
        let failure = Arc::new(faults::Phase::default());
        record.failures.add_stream(failure.clone());
        let actual = Arc::new(RetainedStream {
            record: RecordRef::downgrade(&record),
            original: original.clone(),
            failure,
            owner: OnceLock::new(),
            cancel: OnceLock::new(),
            abandoned: false.into(),
            complete: false.into(),
        });
        streams.push(actual.clone());
        Ok(PacketDriverStreamGrant { actual })
    }
}
impl PacketDriverStreamGrant {
    /// Strong-install the whole original before any view escapes or first SYN.
    /// Even a mismatching failed candidate stays under the same Record cleanup.
    pub fn install(self, loan: StreamLoan) -> Result<StreamLoan, ChannelError> {
        let StreamLoan { io, mut owner } = loan;
        let matching = owner.original().same_original(&self.actual.original);
        owner.observe_packet_driver(PacketDriverStreamRetained(Arc::downgrade(&self.actual)));
        let cancel = owner.packet_driver_cancel();
        let owner = Arc::new(AsyncMutex::new(owner));
        // No await and no external caller can have this fresh owner locked.
        let view = owner
            .try_lock()
            .expect("not yet published original LoanOwner")
            .packet_driver_view(
                Arc::downgrade(&owner),
                StreamCancellation(self.actual.clone()),
            );
        self.actual
            .cancel
            .set(cancel)
            .unwrap_or_else(|_| unreachable!("unique Stream grant"));
        self.actual
            .owner
            .set(owner)
            .unwrap_or_else(|_| unreachable!("unique Stream owner"));
        if !matching
            || self
                .actual
                .record
                .upgrade()
                .is_none_or(|record| record.call.cancel.is_cancelled())
        {
            self.actual.request_retirement();
            drop(io);
            drop(view);
            return Err(failure(Reason::StalePlan));
        }
        Ok(StreamLoan { io, owner: view })
    }
}
impl Drop for PacketDriverStreamGrant {
    fn drop(&mut self) {
        // This branch has never escaped an IO view or polled a SYN. Installed
        // owners retain their actual control even when a waiter is cancelled.
        if self.actual.owner.get().is_none() {
            self.actual.abandoned.store(true, Ordering::Release);
        }
    }
}
impl RetainedStream {
    pub(super) fn request_retirement(&self) {
        if let Some(cancel) = self.cancel.get() {
            cancel.cancel();
        }
    }
    pub(super) fn complete(&self) -> bool {
        if self.abandoned.load(Ordering::Acquire) || self.complete.load(Ordering::Acquire) {
            return true;
        }
        let confirmed = self.owner.get().is_some_and(|owner| {
            owner
                .try_lock()
                .is_ok_and(|owner| owner.closed().is_some_and(|same| same.matches(&owner)))
        });
        if confirmed {
            self.complete.store(true, Ordering::Release);
        }
        confirmed
    }
    pub(super) fn try_close(&self) -> Result<CloseReport, ChannelError> {
        self.request_retirement();
        if self.complete() {
            return Ok(Default::default());
        }
        let Some(owner) = self.owner.get() else {
            return Ok(pending());
        };
        let Ok(mut owner) = owner.try_lock() else {
            return Ok(pending());
        };
        let record = self
            .record
            .upgrade()
            .ok_or_else(|| failure(Reason::Revoked))?;
        if let Some(source) = record
            .failed_phase(&self.failure)
            .filter(|_| !owner.logical_cleanup_continuation())
        {
            return Err(source);
        }
        let report = record.receive_phase(&self.failure, owner.try_close())?;
        if report.is_complete() && owner.closed().is_some_and(|closed| closed.matches(&owner)) {
            self.complete.store(true, Ordering::Release);
        }
        Ok(report)
    }
    pub(super) async fn close(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.request_retirement();
        if self.complete() {
            return Ok(Default::default());
        }
        let Some(owner) = self.owner.get() else {
            return Ok(pending());
        };
        let Ok(mut owner) = tokio::time::timeout_at(end, owner.lock()).await else {
            return Ok(pending());
        };
        let record = self
            .record
            .upgrade()
            .ok_or_else(|| failure(Reason::Revoked))?;
        if let Some(source) = record
            .failed_phase(&self.failure)
            .filter(|_| !owner.logical_cleanup_continuation())
        {
            return Err(source);
        }
        let report = record.receive_phase(&self.failure, owner.close(end).await)?;
        if report.is_complete() && owner.closed().is_some_and(|closed| closed.matches(&owner)) {
            self.complete.store(true, Ordering::Release);
        }
        Ok(report)
    }
}
impl PacketDriverStreamRetained {
    pub fn retained(&self) -> bool {
        self.0
            .upgrade()
            .is_some_and(|actual| actual.owner.get().is_some() && !actual.complete())
    }
    pub fn same_retainer(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.0, &other.0)
    }
    pub fn original(&self) -> Option<CarrierOrigin> {
        Some(self.0.upgrade()?.original.clone())
    }
    pub fn source(&self) -> Option<CarrierOrigin> {
        Some(self.0.upgrade()?.record.upgrade()?.original.clone())
    }
    pub fn receiver(&self) -> Option<TaskJoinWitness> {
        Some(
            self.0
                .upgrade()?
                .record
                .upgrade()?
                .installed
                .get()?
                .receiver
                .get()?
                .clone(),
        )
    }
    pub fn caller(&self) -> Option<TaskJoinWitness> {
        Some(self.0.upgrade()?.record.upgrade()?.caller.clone())
    }
    pub fn retains_origin(&self, original: &CarrierOrigin) -> bool {
        self.0.upgrade().is_some_and(|actual| {
            actual.original.same_original(original) && actual.owner.get().is_some()
        })
    }
}
fn pending() -> CloseReport {
    CloseReport {
        unfinished: vec![Unfinished::Resource(
            "Packet Source retains its original virtual Stream loan".into(),
        )],
        ..Default::default()
    }
}

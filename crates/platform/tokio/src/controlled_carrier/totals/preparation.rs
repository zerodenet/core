//! Job/request credit in the same actual original map, never a second pool.
use super::*;
use std::sync::atomic::AtomicUsize;
#[derive(Default)]
pub(crate) struct PreparationUsage {
    job: AtomicBool,
    requests: AtomicUsize,
}
impl PreparationUsage {
    pub(crate) fn snapshot(&self) -> [usize; 2] {
        [
            usize::from(self.job.load(Ordering::Acquire)),
            self.requests.load(Ordering::Acquire),
        ]
    }
}
/// Read-only accounting, with no native, task or closure capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarrierPreparationTotalsSnapshot {
    pub max_jobs: NonZeroUsize,
    pub max_requests: NonZeroUsize,
    pub jobs: usize,
    pub requests: usize,
}
/// This is one exact original's accounting cell. Job credit deliberately has no
/// Drop refund: it remains in the original map until actual native/users/driver
/// registration closure removes that original. Request credit ends with the
/// actual request/body future. Neither kind of credit is an I/O permission.
pub(crate) struct PreparationBudget {
    id: usize,
    slot: std::sync::Weak<Slot>,
    instance: zero_core::component::ComponentInstanceId,
    root: Arc<CarrierOriginalTotals>,
    original: Arc<PreparationUsage>,
}
pub struct CarrierPreparationRequest {
    root: Arc<CarrierOriginalTotals>,
    original: Arc<PreparationUsage>,
}
impl CarrierPreparationRequest {
    /// One request reserves this existing hard response-body payload allowance.
    /// It covers the one body Vec through return; this is not an RSS, header,
    /// H2 codec, native FD or whole-transport memory claim.
    pub fn body_payload_limit(&self) -> usize {
        4 * 1024 * 1024
    }
}
impl Drop for CarrierPreparationRequest {
    fn drop(&mut self) {
        let previous = self.original.requests.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "one return per actual preparation request");
        self.root.changed.notify_waiters();
    }
}
impl State {
    fn preparation_usage(&self) -> [usize; 2] {
        self.originals.values().fold([0, 0], |mut total, original| {
            let usage = original.preparation_usage();
            total[0] = total[0].saturating_add(usage[0]);
            total[1] = total[1].saturating_add(usage[1]);
            total
        })
    }
}
impl CarrierOriginalTotals {
    pub fn preparation_snapshot(&self) -> CarrierPreparationTotalsSnapshot {
        let state = self.lock();
        let [jobs, requests] = state.preparation_usage();
        CarrierPreparationTotalsSnapshot {
            max_jobs: state.preparation_limits[0],
            max_requests: state.preparation_limits[1],
            jobs,
            requests,
        }
    }
    pub fn check_preparation_limits_after_retirement(
        &self,
        max_jobs: NonZeroUsize,
        max_requests: NonZeroUsize,
    ) -> std::io::Result<()> {
        let state = self.lock();
        let next = [max_jobs, max_requests];
        if state.preparation_limits != next && state.preparation_usage() != [0, 0] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "original preparation jobs/requests remain retained",
            ));
        }
        Ok(())
    }
    pub fn install_preparation_limits_after_retirement(
        &self,
        max_jobs: NonZeroUsize,
        max_requests: NonZeroUsize,
    ) -> std::io::Result<()> {
        let mut state = self.lock();
        let next = [max_jobs, max_requests];
        if state.preparation_limits != next && state.preparation_usage() != [0, 0] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "original preparation jobs/requests remain retained",
            ));
        }
        state.preparation_limits = next;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn reserve_preparation(
        self: &Arc<Self>,
        slot: &Arc<Slot>,
    ) -> Result<PreparationBudget, ChannelError> {
        // The caller validates the actual original's preparation/accepted Aux
        // and keeps its original close gate. Root inspection only reads the
        // existing custody rows' atomics, never takes another Slot/owner lock.
        let state = self.lock();
        let id = Arc::as_ptr(slot) as usize;
        let failure = || {
            error(
                slot.instance,
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            )
        };
        if !state.originals.contains_key(&id)
            || state.preparation_usage()[0] >= state.preparation_limits[0].get()
            || slot.preparation_usage.job.swap(true, Ordering::AcqRel)
        {
            return Err(failure());
        }
        Ok(PreparationBudget {
            id,
            slot: Arc::downgrade(slot),
            instance: slot.instance,
            root: self.clone(),
            original: slot.preparation_usage.clone(),
        })
    }
}
impl PreparationBudget {
    pub(crate) async fn request(&self) -> Result<CarrierPreparationRequest, ChannelError> {
        loop {
            let changed = self.root.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.root.lock();
                if self
                    .slot
                    .upgrade()
                    .is_none_or(|slot| slot.removed.load(Ordering::Acquire))
                    || !state.originals.contains_key(&self.id)
                {
                    return Err(error(
                        self.instance,
                        Stage::Prepare,
                        Reason::Revoked,
                        DeliveryState::NotSubmitted,
                    ));
                }
                if state.preparation_usage()[1] < state.preparation_limits[1].get() {
                    self.original.requests.fetch_add(1, Ordering::AcqRel);
                    return Ok(CarrierPreparationRequest {
                        root: self.root.clone(),
                        original: self.original.clone(),
                    });
                }
            }
            changed.await;
        }
    }
}

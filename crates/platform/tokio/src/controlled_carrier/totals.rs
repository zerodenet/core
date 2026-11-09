//! One Host's finite custody of exact Carrier originals, not another carrier pool.
use super::{error, owner::Shared, slot::Slot};
use crate::contract::{BoxFuture, ChannelError, DeliveryState, Reason, Stage};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::Context,
};

/// Original fault identity from its retained provider, never reconstructed from
/// a component-only error. None is unknown ownership, not an absent fault or a
/// successful close. No task/native capability or Scope is issued here.
#[derive(Clone, Debug)]
pub struct CarrierOriginalFailure {
    pub instance: Option<zero_core::component::ComponentInstanceId>,
    pub source: crate::contract::RetainedError,
}
/// Counts original reservation/custody records. An adopted logical LoanOwner
/// is a record too; this count must never be advertised as native FD count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarrierOriginalTotalsSnapshot {
    pub limit: NonZeroUsize,
    pub charged: usize,
}
pub struct CarrierOriginalTotals {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
    buffers: super::CarrierBufferBudget,
}
struct State {
    limit: NonZeroUsize,
    preparation_limits: [NonZeroUsize; 2],
    originals: BTreeMap<usize, Arc<fault::Retained<dyn custody::CarrierCustody>>>,
    metadata: Vec<Arc<fault::Retained<dyn MetadataCustody>>>,
    bound: Vec<Arc<fault::Retained<dyn BoundCustody>>>,
    source: Vec<Arc<fault::Retained<dyn SourceCustody>>>,
}
struct Original {
    // Strong custody is deliberate until actual native/users/registration
    // settlement. Owner Drop or a disappearing observer cannot refund credit.
    owner: Arc<Shared>,
    slot: Arc<Slot>,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
}
impl Default for CarrierOriginalTotals {
    fn default() -> Self {
        Self {
            changed: tokio::sync::Notify::new(),
            buffers: Default::default(),
            state: Mutex::new(State {
                limit: NonZeroUsize::new(1024).expect("nonzero kernel policy"),
                preparation_limits: [
                    NonZeroUsize::new(64).unwrap(),
                    NonZeroUsize::new(32).unwrap(),
                ],
                originals: BTreeMap::new(),
                metadata: Vec::new(),
                bound: Vec::new(),
                source: Vec::new(),
            }),
        }
    }
}
impl std::fmt::Debug for CarrierOriginalTotals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CarrierOriginalTotals")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}
impl CarrierOriginalTotals {
    pub fn new(limit: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            changed: tokio::sync::Notify::new(),
            buffers: Default::default(),
            state: Mutex::new(State {
                limit,
                preparation_limits: [
                    NonZeroUsize::new(64).unwrap(),
                    NonZeroUsize::new(32).unwrap(),
                ],
                originals: BTreeMap::new(),
                metadata: Vec::new(),
                bound: Vec::new(),
                source: Vec::new(),
            }),
        })
    }
    /// All actual protocol queues receive this one pure byte book.
    pub fn buffer_budget(&self) -> super::CarrierBufferBudget {
        self.buffers.clone()
    }
    pub fn check_buffer_limits_after_retirement(
        &self,
        limits: super::CarrierBufferLimits,
    ) -> std::io::Result<()> {
        self.reconcile();
        let state = self.lock();
        if self.buffers.limits() != limits && state.charged() != 0 {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        self.buffers.check_limits(limits)
    }
    pub fn install_buffer_limits_after_retirement(
        &self,
        limits: super::CarrierBufferLimits,
    ) -> std::io::Result<()> {
        self.reconcile();
        let state = self.lock();
        if self.buffers.limits() != limits && state.charged() != 0 {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        let wake = self.buffers.install_limits_deferred(limits)?;
        drop(state);
        self.buffers.publish_capacity(wake);
        Ok(())
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn originals(&self) -> Vec<(usize, Arc<fault::Retained<dyn custody::CarrierCustody>>)> {
        self.lock()
            .originals
            .iter()
            .map(|(id, value)| (*id, value.clone()))
            .collect()
    }
    /// Only the existing StopOld transaction may install a changed limit.
    /// This native custody check is necessary, never substitute StopOld proof.
    pub fn install_after_retirement(&self, limit: NonZeroUsize) -> std::io::Result<()> {
        self.reconcile();
        let mut state = self.lock();
        if state.limit != limit
            && (!state.originals.is_empty()
                || !state.metadata.is_empty()
                || !state.bound.is_empty()
                || !state.source.is_empty())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "original Carrier native/users/registration debts remain retained",
            ));
        }
        state.limit = limit;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    /// Read-only actual original retirement check; Host must retain its
    /// existing acquisition fence through the later installation.
    pub fn check_install_after_retirement(&self, limit: NonZeroUsize) -> std::io::Result<()> {
        self.reconcile();
        let state = self.lock();
        if state.limit != limit
            && (!state.originals.is_empty()
                || !state.metadata.is_empty()
                || !state.bound.is_empty()
                || !state.source.is_empty())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "original Carrier native/users/registration debts remain retained",
            ));
        }
        Ok(())
    }
    pub fn snapshot(&self) -> CarrierOriginalTotalsSnapshot {
        self.reconcile();
        let state = self.lock();
        CarrierOriginalTotalsSnapshot {
            limit: state.limit,
            charged: state.charged(),
        }
    }
    pub(super) fn reserve(
        &self,
        owner: &Arc<Shared>,
        slot: &Arc<Slot>,
    ) -> Result<(), ChannelError> {
        // Caller holds the actual owner lock. No witness getter, external wake,
        // source Drop or close operation occurs while this root lock is held.
        let mut state = self.lock();
        if state.charged() >= state.limit.get() {
            return Err(error(
                owner.instance,
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        let id = Arc::as_ptr(slot) as usize;
        assert!(
            !state.originals.contains_key(&id),
            "one root charge per actual original"
        );
        state.originals.insert(
            id,
            fault::Retained::new(
                Arc::new(Original {
                    owner: owner.clone(),
                    slot: slot.clone(),
                    waiting: Mutex::default(),
                    polling: AtomicBool::new(false),
                    repoll: AtomicBool::new(false),
                    first_fault: Mutex::default(),
                }) as Arc<dyn custody::CarrierCustody>,
                Some(owner.instance),
            ),
        );
        Ok(())
    }
    fn release(&self, id: usize, original: &Arc<fault::Retained<dyn custody::CarrierCustody>>) {
        let released = {
            let mut state = self.lock();
            if state
                .originals
                .get(&id)
                .is_some_and(|actual| Arc::ptr_eq(actual, original))
            {
                state.originals.remove(&id)
            } else {
                None
            }
        };
        // The exact original's controllers, typed faults and captured close
        // continuation may have destructors/wakers. Never drop under root lock.
        let changed = released.is_some();
        drop(released);
        if changed {
            self.changed.notify_waiters();
        }
    }
    pub(crate) fn reconcile(&self) {
        for (id, original) in self.originals() {
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release(id, &original);
            }
        }
        let metadata = self.lock().metadata.clone();
        for original in metadata {
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release_metadata(&original);
            }
        }
        let source = self.lock().source.clone();
        for original in source {
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release_source(&original);
            }
        }
        let bound = self.lock().bound.clone();
        for original in bound {
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release_bound(&original);
            }
        }
    }
}
mod failures;
mod fault;
mod original;
mod retirement;

mod datagram;

mod custody;
pub(crate) use custody::{BoundCustody, MetadataCustody};

mod source;
pub(crate) use source::SourceCustody;

mod capacity;

pub(crate) mod preparation;

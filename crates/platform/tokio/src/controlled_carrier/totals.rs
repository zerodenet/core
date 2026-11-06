//! One Host's finite custody of exact Carrier originals, not another carrier pool.
use super::{error, owner::Shared, slot::Slot, CarrierOwnerRegistration};
use crate::contract::{BoxFuture, ChannelError, DeliveryState, Reason, Stage};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::Context,
    time::Duration,
};

/// Counts original reservation/custody records. An adopted logical LoanOwner
/// is a record too; this count must never be advertised as native FD count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarrierOriginalTotalsSnapshot {
    pub limit: NonZeroUsize,
    pub charged: usize,
}
pub struct CarrierOriginalTotals {
    state: Mutex<State>,
}
struct State {
    limit: NonZeroUsize,
    originals: BTreeMap<usize, Arc<Original>>,
    metadata: Vec<Arc<crate::controlled_datagram::metadata_pool::MetadataTotalsOriginal>>,
    bound: Vec<Arc<crate::controlled_datagram::BoundTotalsOriginal>>,
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
            state: Mutex::new(State {
                limit: NonZeroUsize::new(1024).expect("nonzero kernel policy"),
                originals: BTreeMap::new(),
                metadata: Vec::new(),
                bound: Vec::new(),
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
            state: Mutex::new(State {
                limit,
                originals: BTreeMap::new(),
                metadata: Vec::new(),
                bound: Vec::new(),
            }),
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn originals(&self) -> Vec<(usize, Arc<Original>)> {
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
                || !state.bound.is_empty())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "original Carrier native/users/registration debts remain retained",
            ));
        }
        state.limit = limit;
        Ok(())
    }
    pub fn snapshot(&self) -> CarrierOriginalTotalsSnapshot {
        self.reconcile();
        let state = self.lock();
        CarrierOriginalTotalsSnapshot {
            limit: state.limit,
            charged: state.originals.len() + state.metadata.len() + state.bound.len(),
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
        if state.originals.len() + state.metadata.len() + state.bound.len() >= state.limit.get() {
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
            Arc::new(Original {
                owner: owner.clone(),
                slot: slot.clone(),
                waiting: Mutex::default(),
                polling: AtomicBool::new(false),
                repoll: AtomicBool::new(false),
                first_fault: Mutex::default(),
            }),
        );
        Ok(())
    }
    fn release(&self, id: usize, original: &Arc<Original>) {
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
        drop(released);
    }
    pub(crate) fn reconcile(&self) {
        for (id, original) in self.originals() {
            if original.released() {
                self.release(id, &original);
            }
        }
        let metadata = self.lock().metadata.clone();
        for original in metadata {
            if original.released() {
                self.release_metadata(&original);
            }
        }
        let bound = self.lock().bound.clone();
        for original in bound {
            if original.released() {
                self.release_bound(&original);
            }
        }
    }
    /// Called inline by the existing Host supervisor. Cancelled originals use
    /// their existing persistent retirement continuation and immutable deadline.
    /// The root never revokes a live sibling, creates a task, or ACKs an exit.
    pub fn poll_cancelled_retirements(&self, cx: &mut Context<'_>) -> bool {
        let mut changed = false;
        let metadata = self.lock().metadata.clone();
        for original in metadata {
            changed |= original.poll_retirement(cx);
            if original.released() {
                self.release_metadata(&original);
                changed = true;
            }
        }
        let bound = self.lock().bound.clone();
        for original in bound {
            changed |= original.poll_retirement(cx);
            if original.released() {
                self.release_bound(&original);
                changed = true;
            }
        }
        let originals = self.originals();
        let mut owners = Vec::new();
        for (_, original) in &originals {
            if !owners
                .iter()
                .any(|known| Arc::ptr_eq(known, &original.owner))
            {
                owners.push(original.owner.clone());
                let (progress, fault) = CarrierOwnerRegistration::new(&original.owner)
                    .poll_cancelled_retirements(cx, Duration::from_secs(5));
                changed |= progress;
                if let Some(fault) = fault {
                    original
                        .first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert(fault);
                }
            }
        }
        for (id, original) in originals {
            if original.released() {
                self.release(id, &original);
                changed = true;
                continue;
            }
            if !original.slot.removed.load(Ordering::Acquire) {
                continue;
            }
            let Some(_polling) = crate::original_poll::OriginalPoll::enter(
                &original.polling,
                &original.repoll,
                cx.waker(),
            ) else {
                continue;
            };
            let mut waiting = original
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| {
                    let witnesses = original.witnesses();
                    Box::pin(async move {
                        for witness in witnesses {
                            witness.wait_registration_released().await;
                        }
                    })
                });
            let ready = waiting.as_mut().poll(cx).is_ready();
            if !ready {
                *original
                    .waiting
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
            }
            if ready && original.released() {
                self.release(id, &original);
                changed = true;
            }
        }
        changed
    }
    /// Original typed failures stay owned even if the protocol Owner is lost.
    /// This does not accept an error on behalf of its actual task/receiver.
    pub fn failures(&self) -> Vec<Arc<ChannelError>> {
        let metadata = self.lock().metadata.clone();
        let bound = self.lock().bound.clone();
        self.originals()
            .into_iter()
            .filter_map(|(_, original)| {
                original
                    .first_fault
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .chain(
                metadata
                    .into_iter()
                    .filter_map(|original| original.failure()),
            )
            .chain(bound.into_iter().filter_map(|original| original.failure()))
            .collect()
    }
}
impl Original {
    fn witnesses(&self) -> Vec<crate::contract::scope::TaskJoinWitness> {
        let state = self.slot.state();
        state
            .driver
            .iter()
            .map(|(_, witness)| witness.clone())
            .chain(state.auxiliary.iter().cloned())
            .collect()
    }
    fn released(&self) -> bool {
        // Set only by Slot::remove after matching native and user settlement,
        // plus the actual acquired caller's Host publication ACK.
        let users_confirmed = {
            let state = self.slot.state();
            match &state.users {
                Some(users) => users.ready(),
                None => state.control.is_none() && state.packet.is_none() && state.loan.is_none(),
            }
        };
        self.slot.removed.load(Ordering::Acquire)
            && users_confirmed
            && self.witnesses().iter().all(|witness| {
                witness.try_joined().is_some_and(|joined| {
                    witness.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
}

mod datagram;

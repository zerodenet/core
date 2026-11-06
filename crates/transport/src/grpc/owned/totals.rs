//! Actual cached-H2 registry totals: no task, pool or generic scheduler.
use super::{budget::Record, state::Physical, *};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};
mod factory;
pub use factory::TotalsFactory;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolBudgets {
    pub pool_carriers: NonZeroUsize,
    pub pool_streams: NonZeroUsize,
    pub total_carriers: NonZeroUsize,
    pub total_streams: NonZeroUsize,
}
#[derive(Default)]
pub struct PoolTotals {
    identity: Mutex<Option<u64>>,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    generation: u64,
    owner: Weak<()>,
    active: bool,
    budgets: Option<PoolBudgets>,
    physical: Vec<Arc<Charge<Physical>>>,
    logical: Vec<Arc<Charge<Record>>>,
}
#[derive(Clone)]
pub struct TotalsOptions {
    root: Arc<PoolTotals>,
    budgets: PoolBudgets,
    identity: Arc<[u8]>,
}
#[derive(Clone)]
pub struct TotalsProposal {
    root: Weak<PoolTotals>,
    budgets: PoolBudgets,
    identity: Arc<[u8]>,
}
#[derive(Clone)]
pub(super) struct Active {
    root: Weak<PoolTotals>,
    generation: u64,
}
struct Original<T> {
    value: Option<Arc<T>>,
    unused: bool,
}
pub(super) struct Charge<T>(Mutex<Original<T>>);
impl<T> Charge<T> {
    fn new() -> Self {
        Self(Mutex::new(Original {
            value: None,
            unused: false,
        }))
    }
    pub(super) fn install(&self, actual: Arc<T>) -> Result<(), ChannelError> {
        let mut state = self.0.lock().unwrap();
        if state.unused || state.value.is_some() {
            return Err(cold(Reason::StalePlan));
        }
        state.value = Some(actual);
        Ok(())
    }
    /// Explicitly before creating any native/logical original. Drop never
    /// changes this fact, so interrupted/unknown assignments remain charged.
    pub(super) fn refuse_unassigned(&self) -> Result<(), ChannelError> {
        let mut state = self.0.lock().unwrap();
        if state.value.is_some() {
            return Err(cold(Reason::StalePlan));
        }
        state.unused = true;
        Ok(())
    }
    fn complete(&self, check: impl FnOnce(&T) -> bool) -> bool {
        let (unused, value) = {
            let state = self.0.lock().unwrap();
            (state.unused, state.value.clone())
        };
        unused || value.is_some_and(|value| check(&value))
    }
}
fn cold(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
    error
}
impl std::fmt::Debug for TotalsOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRootBudgets")
            .field("budgets", &self.budgets)
            .finish_non_exhaustive()
    }
}
impl PoolTotals {
    #[cfg(test)]
    pub(super) fn charged_for_test(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.physical.len(), state.logical.len())
    }
    pub fn options(self: &Arc<Self>, budgets: PoolBudgets) -> Result<TotalsOptions, ChannelError> {
        if budgets.pool_carriers.get() > 4096
            || budgets.total_carriers.get() > 4096
            || budgets.pool_streams.get() > 65536
            || budgets.total_streams.get() > 65536
        {
            return Err(cold(Reason::InvalidConfig));
        }
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = {
            let mut identity = self.identity.lock().unwrap();
            match *identity {
                Some(id) => id,
                None => {
                    let id = NEXT
                        .try_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                            old.checked_add(1)
                        })
                        .map_err(|_| cold(Reason::Capacity))?;
                    *identity = Some(id);
                    id
                }
            }
        };
        let mut identity = nonce.to_le_bytes().to_vec();
        for value in [
            budgets.pool_carriers,
            budgets.pool_streams,
            budgets.total_carriers,
            budgets.total_streams,
        ] {
            identity.extend_from_slice(&(value.get() as u64).to_le_bytes());
        }
        Ok(TotalsOptions {
            root: self.clone(),
            budgets,
            identity: identity.into(),
        })
    }
    pub(super) fn harvest(&self) {
        let (physical, logical) = {
            let state = self.state.lock().unwrap();
            (state.physical.clone(), state.logical.clone())
        };
        let physical = physical
            .into_iter()
            .filter(|charge| charge.complete(Physical::closed))
            .collect::<Vec<_>>();
        let logical = logical
            .into_iter()
            .filter(|charge| charge.complete(Record::complete))
            .collect::<Vec<_>>();
        let mut removed_physical = Vec::new();
        let mut removed_logical = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            for index in (0..state.physical.len()).rev() {
                if physical
                    .iter()
                    .any(|actual| Arc::ptr_eq(actual, &state.physical[index]))
                {
                    removed_physical.push(state.physical.swap_remove(index));
                }
            }
            for index in (0..state.logical.len()).rev() {
                if logical
                    .iter()
                    .any(|actual| Arc::ptr_eq(actual, &state.logical[index]))
                {
                    removed_logical.push(state.logical.swap_remove(index));
                }
            }
        }
        // Final native/controller/source destruction never runs under root locks.
        drop((removed_physical, removed_logical));
    }
    fn physical(
        &self,
        view: &Active,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Charge<Physical>>, ChannelError> {
        self.harvest();
        let mut state = self.state.lock().unwrap();
        if !state.active || state.generation != view.generation {
            return Err(error(instance, Stage::Prepare, Reason::Revoked));
        }
        if state.physical.len() >= state.budgets.unwrap().total_carriers.get() {
            return Err(error(instance, Stage::Prepare, Reason::Capacity));
        }
        state.physical.try_reserve(1).map_err(|source| {
            error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
        let charge = Arc::new(Charge::new());
        state.physical.push(charge.clone());
        Ok(charge)
    }
    fn logical(
        &self,
        view: &Active,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Charge<Record>>, ChannelError> {
        self.harvest();
        let mut state = self.state.lock().unwrap();
        if !state.active || state.generation != view.generation {
            return Err(error(instance, Stage::Prepare, Reason::Revoked));
        }
        if state.logical.len() >= state.budgets.unwrap().total_streams.get() {
            return Err(error(instance, Stage::Prepare, Reason::Capacity));
        }
        state.logical.try_reserve(1).map_err(|source| {
            error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
        let charge = Arc::new(Charge::new());
        state.logical.push(charge.clone());
        Ok(charge)
    }
}

impl TotalsOptions {
    pub fn proposal(&self) -> TotalsProposal {
        TotalsProposal {
            root: Arc::downgrade(&self.root),
            budgets: self.budgets,
            identity: self.identity.clone(),
        }
    }
    pub fn budgets(&self) -> PoolBudgets {
        self.budgets
    }
}
impl TotalsProposal {
    pub(super) fn matches_limits(&self, carriers: NonZeroUsize, streams: NonZeroUsize) -> bool {
        self.budgets.pool_carriers == carriers && self.budgets.pool_streams == streams
    }
    pub(super) fn identity(&self) -> &[u8] {
        &self.identity
    }
    pub(super) fn activate(&self) -> Result<Active, ChannelError> {
        let root = self.root.upgrade().ok_or_else(|| cold(Reason::Revoked))?;
        let state = root.state.lock().unwrap();
        if !state.active || state.budgets != Some(self.budgets) {
            return Err(cold(Reason::StalePlan));
        }
        Ok(Active {
            root: Arc::downgrade(&root),
            generation: state.generation,
        })
    }
}
impl Active {
    pub(super) fn live(&self) -> bool {
        self.root.upgrade().is_some_and(|root| {
            let state = root.state.lock().unwrap();
            state.active && state.generation == self.generation
        })
    }
    pub(super) fn physical(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Charge<Physical>>, ChannelError> {
        self.root
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?
            .physical(self, instance)
    }
    pub(super) fn logical(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<Arc<Charge<Record>>, ChannelError> {
        self.root
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?
            .logical(self, instance)
    }
}

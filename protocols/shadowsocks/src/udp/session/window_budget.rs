//! One actual adapter's distinct protocol-state units; never a native IO budget.
use std::sync::{Arc, Mutex, Weak};
use zero_core::Error;

/// Kernel policy defaults, distinct from the reference's expiry-only histories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UdpBudgetLimits {
    pub receiver_windows: usize,
    pub senders: usize,
    pub associations: usize,
    pub return_bindings: usize,
    pub authenticated_codecs: usize,
}
impl Default for UdpBudgetLimits {
    fn default() -> Self {
        Self {
            receiver_windows: 8192,
            senders: 8192,
            associations: 8192,
            return_bindings: 65536,
            authenticated_codecs: 4096,
        }
    }
}
impl UdpBudgetLimits {
    fn counts(self) -> [usize; 5] {
        [
            self.receiver_windows,
            self.senders,
            self.associations,
            self.return_bindings,
            self.authenticated_codecs,
        ]
    }
    pub fn validate(self) -> Result<Self, Error> {
        if self
            .counts()
            .into_iter()
            .any(|count| count == 0 || count.checked_add(1).is_none())
        {
            return Err(Error::Config(
                "SS UDP state budgets must be positive and countable",
            ));
        }
        Ok(self)
    }
}
#[derive(Debug, Clone, Copy)]
pub(crate) enum Unit {
    Window,
    Sender,
    Association,
    ReturnBinding,
    AuthenticatedCodec,
}
impl Unit {
    fn index(self) -> usize {
        self as usize
    }
}
#[derive(Debug, Default)]
struct State {
    limits: [Option<usize>; 5],
    retained: [usize; 5],
}
#[derive(Debug)]
pub struct UdpReplayBudget {
    state: Mutex<State>,
    // Strong originals, weak backreferences: valid security history survives a
    // dropped codec/source view without a copied membership table or cycle.
    books: Mutex<Vec<Arc<Mutex<super::receive::Book>>>>,
    responses: Mutex<Vec<Arc<Mutex<super::responses::Book>>>>,
}
impl UdpReplayBudget {
    /// Standalone window-only policy; production uses all five finite units.
    pub fn new(max_windows: usize) -> Result<Arc<Self>, Error> {
        if max_windows == 0 || max_windows.checked_add(1).is_none() {
            return Err(Error::Config(
                "SS UDP receiver-window budget must be positive and countable",
            ));
        }
        let budget = Arc::new(Self::default());
        budget.state.lock().unwrap().limits[Unit::Window.index()] = Some(max_windows);
        Ok(budget)
    }
    pub fn with_limits(limits: UdpBudgetLimits) -> Result<Arc<Self>, Error> {
        let budget = Arc::new(Self::default());
        budget.install(limits)?;
        Ok(budget)
    }
    /// Cold read-only validation, including a candidate removing all consumers.
    pub fn validate(&self, limits: UdpBudgetLimits) -> Result<(), Error> {
        let limits = limits.validate()?.counts();
        self.prune();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .retained
            .iter()
            .zip(limits)
            .any(|(retained, limit)| *retained > limit)
        {
            return Err(Error::Config(
                "retained SS UDP state exceeds the proposed budget",
            ));
        }
        Ok(())
    }
    /// Only the existing real component's start after confirmed StopOld calls
    /// this; cold projection never changes live admission or security history.
    pub(crate) fn install(&self, limits: UdpBudgetLimits) -> Result<(), Error> {
        self.validate(limits)?;
        let limits = limits.counts();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .retained
            .iter()
            .zip(limits)
            .any(|(retained, limit)| *retained > limit)
        {
            return Err(Error::Config(
                "SS UDP state grew before replacement admission paused",
            ));
        }
        state.limits = limits.map(Some);
        Ok(())
    }
    pub fn retained(&self) -> usize {
        self.occupancy()[Unit::Window.index()]
    }
    /// Windows, senders, associations, return bindings, authenticated codecs.
    pub fn occupancy(&self) -> [usize; 5] {
        self.prune();
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retained
    }
    pub(super) fn attach(&self, book: &Arc<Mutex<super::receive::Book>>) {
        self.prune();
        self.books
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(book.clone());
    }
    pub(super) fn attach_responses(&self, book: &Arc<Mutex<super::responses::Book>>) {
        self.prune();
        self.responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(book.clone());
    }
    fn reserve_unit(&self, unit: Unit) -> Result<(), Error> {
        for pressure in [false, true] {
            if pressure {
                self.prune();
            }
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let index = unit.index();
            let next = state.retained[index]
                .checked_add(1)
                .ok_or(Error::Protocol("SS UDP state count overflow"))?;
            if state.limits[index].is_none_or(|limit| next <= limit) {
                state.retained[index] = next;
                return Ok(());
            }
        }
        Err(Error::Protocol("SS UDP state total exhausted"))
    }
    pub(crate) fn claim(self: &Arc<Self>, unit: Unit) -> Result<Charge, Error> {
        self.reserve_unit(unit)?;
        Ok(Charge {
            budget: Arc::downgrade(self),
            unit,
        })
    }
    pub(super) fn reserve(&self) -> Result<(), Error> {
        self.reserve_unit(Unit::Window)
    }
    pub(super) fn expire(&self) {
        self.release(Unit::Window);
    }
    fn release(&self, unit: Unit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = &mut state.retained[unit.index()];
        *count = count.checked_sub(1).expect("original SS UDP state charge");
    }
    fn prune(&self) {
        let responses = self
            .responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for original in responses {
            if let Ok(mut book) = original.try_lock() {
                book.prune();
            }
        }
        self.responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|book| {
                Arc::strong_count(book) != 1
                    || book
                        .try_lock()
                        .map_or(true, |book| !book.entries.is_empty())
            });
        let books = self
            .books
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for original in books {
            if let Ok(mut book) = original.try_lock() {
                book.prune();
            }
        }
        self.books
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|book| {
                Arc::strong_count(book) != 1
                    || book
                        .try_lock()
                        .map_or(true, |book| !book.entries.is_empty())
            });
    }
}
impl Default for UdpReplayBudget {
    fn default() -> Self {
        Self {
            state: Default::default(),
            books: Default::default(),
            responses: Default::default(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct Charge {
    budget: Weak<UdpReplayBudget>,
    unit: Unit,
}
impl Drop for Charge {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.upgrade() {
            budget.release(self.unit);
        }
    }
}

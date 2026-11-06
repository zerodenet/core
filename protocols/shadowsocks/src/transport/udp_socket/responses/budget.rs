//! One actual adapter's response registrations, separate from replay history.
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShadowsocksUdpPendingLimits {
    pub per_cache: usize,
    pub total: usize,
}
impl Default for ShadowsocksUdpPendingLimits {
    fn default() -> Self {
        Self {
            per_cache: 4096,
            total: 4096,
        }
    }
}
impl ShadowsocksUdpPendingLimits {
    pub fn validate(self) -> io::Result<()> {
        if !(1..=65536).contains(&self.per_cache) || !(1..=65536).contains(&self.total) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SS UDP pending response limits must be in 1..=65536",
            ));
        }
        Ok(())
    }
}
#[derive(Debug)]
struct State {
    limit: usize,
    held: usize,
}
#[derive(Debug)]
pub(crate) struct PendingBudget(Mutex<State>);
impl Default for PendingBudget {
    fn default() -> Self {
        Self(Mutex::new(State {
            limit: 4096,
            held: 0,
        }))
    }
}
impl PendingBudget {
    pub(super) fn claim(self: &Arc<Self>) -> io::Result<Charge> {
        let mut state = self.0.lock().unwrap();
        if state.held >= state.limit {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        state.held += 1;
        Ok(Charge(self.clone()))
    }
    fn activate(&self, limit: usize) -> io::Result<()> {
        let mut state = self.0.lock().unwrap();
        if state.limit != limit {
            if state.held != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "the same actual SS UDP pending responses have not returned",
                ));
            }
            state.limit = limit;
        }
        Ok(())
    }
}
pub(super) struct Charge(Arc<PendingBudget>);
impl Drop for Charge {
    fn drop(&mut self) {
        let mut state = self.0 .0.lock().unwrap();
        state.held = state
            .held
            .checked_sub(1)
            .expect("each actual response charge returns once");
    }
}
#[derive(Clone)]
pub(crate) struct PendingProposal {
    budget: Weak<PendingBudget>,
    pub(crate) limits: ShadowsocksUdpPendingLimits,
}
impl PendingProposal {
    pub(crate) fn new(
        budget: &Arc<PendingBudget>,
        limits: ShadowsocksUdpPendingLimits,
    ) -> io::Result<Self> {
        limits.validate()?;
        Ok(Self {
            budget: Arc::downgrade(budget),
            limits,
        })
    }
    pub(crate) fn activate(&self) -> io::Result<()> {
        self.budget
            .upgrade()
            .ok_or_else(|| io::Error::from(io::ErrorKind::Interrupted))?
            .activate(self.limits.total)
    }
    pub(crate) fn actual(&self) -> io::Result<Arc<PendingBudget>> {
        self.budget
            .upgrade()
            .ok_or_else(|| io::ErrorKind::Interrupted.into())
    }
}

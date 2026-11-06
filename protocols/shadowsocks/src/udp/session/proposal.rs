//! A cold protocol-private proposal over the caller's one actual budget.
use super::{UdpBudgetLimits, UdpReplayBudget};
use std::sync::{Arc, Weak};
use zero_core::Error;

#[derive(Clone, Debug)]
pub struct UdpBudgetProposal {
    original: Weak<UdpReplayBudget>,
    limits: UdpBudgetLimits,
}
impl UdpBudgetProposal {
    pub(crate) fn new(
        original: &Arc<UdpReplayBudget>,
        limits: UdpBudgetLimits,
    ) -> Result<Self, Error> {
        original.validate(limits)?;
        Ok(Self {
            original: Arc::downgrade(original),
            limits,
        })
    }
    pub(crate) fn identity(&self) -> (usize, UdpBudgetLimits) {
        (self.original.as_ptr() as usize, self.limits)
    }
    // Called by the existing real Plugin Owner start, after the host's actual
    // replacement barrier and before that owner starts any child or probe IO.
    pub(crate) fn activate(&self) -> Result<(), Error> {
        self.original
            .upgrade()
            .ok_or(Error::Protocol("SS UDP budget owner was released"))?
            .install(self.limits)
    }
}

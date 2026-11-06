//! Protocol-owned sender identity, monotonic counters and replay admission.
use crate::{shared::random_u64, shared::ReplayWindow};
use std::collections::HashMap;
#[cfg(test)]
use std::time::Duration;
use tokio::time::Instant;
use zero_core::Error;

#[cfg(test)]
pub(crate) const RETENTION: Duration = Duration::from_secs(300);
#[cfg(test)]
pub(crate) const CAPACITY: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct SenderSession {
    id: Option<u64>,
    packet: u64,
    budget: Option<std::sync::Weak<UdpReplayBudget>>,
    _charge: Option<window_budget::Charge>,
}
impl SenderSession {
    pub(crate) fn use_budget(
        &mut self,
        budget: &std::sync::Arc<UdpReplayBudget>,
    ) -> Result<(), Error> {
        if self
            .budget
            .as_ref()
            .is_some_and(|existing| !existing.ptr_eq(&std::sync::Arc::downgrade(budget)))
        {
            return Err(Error::Config("cannot fork an original SS UDP sender"));
        }
        self.budget = Some(std::sync::Arc::downgrade(budget));
        Ok(())
    }
    pub(crate) fn next(&mut self) -> Result<(u64, u64), Error> {
        let id = match self.id {
            Some(id) => id,
            None => {
                let charge = self
                    .budget
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .map(|budget| budget.claim(window_budget::Unit::Sender))
                    .transpose()?;
                let id = random_u64()?;
                self._charge = charge;
                self.id = Some(id);
                id
            }
        };
        // A caller must recreate the flow on exhaustion; never wrap/reuse a nonce.
        self.packet = self
            .packet
            .checked_add(1)
            .filter(|packet| *packet < u64::MAX)
            .ok_or(Error::Protocol("ss: UDP packet counter exhausted"))?;
        Ok((id, self.packet))
    }
    pub(crate) fn matches(&self, id: u64) -> bool {
        self.id == Some(id)
    }
}

mod receive;
pub(crate) use receive::ReceiveWindows;
mod window_budget;
pub(crate) use window_budget::{Charge, Unit as BudgetUnit};
pub use window_budget::{UdpBudgetLimits, UdpReplayBudget};

mod proposal;
pub use proposal::UdpBudgetProposal;

mod responses;
pub(crate) use responses::ResponseSessions;

#[cfg(test)]
#[path = "../../tests/udp_session/state.rs"]
mod tests;

#[cfg(test)]
#[path = "../../tests/udp_session/window_budget.rs"]
mod window_budget_tests;

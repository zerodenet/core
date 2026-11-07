//! Observe actual strong receiver installation; no IO, close or task capability.
use super::{DatagramLoan, LoanOwner, StreamLoan};
use crate::controlled_carrier::{slot::Slot, CarrierAdoption, CarrierLoanRetained};
use std::sync::{Arc, Mutex, Weak};
#[derive(Default)]
pub(super) struct Observation(Mutex<Option<Installed>>);
#[derive(Clone)]
struct Installed {
    slot: Weak<Slot>,
    owner: Weak<tokio::sync::Mutex<LoanOwner>>,
}
/// Weak observation of this exact returned loan's actual installed controller.
/// Never adopted, retired, replaced or unknown state cannot provide a receiver.
#[derive(Clone)]
pub struct CarrierLoanReceiver(Weak<Observation>);
impl LoanOwner {
    fn receiver_observation(&mut self) -> CarrierLoanReceiver {
        CarrierLoanReceiver(Arc::downgrade(
            self.receiver
                .get_or_init(|| Arc::new(Observation::default())),
        ))
    }
}
impl StreamLoan {
    pub fn receiver_observation(&mut self) -> CarrierLoanReceiver {
        self.owner.receiver_observation()
    }
}
impl DatagramLoan {
    pub fn receiver_observation(&mut self) -> CarrierLoanReceiver {
        self.owner.receiver_observation()
    }
}
pub(super) fn install_owner(
    slot: &Arc<Slot>,
    owner: LoanOwner,
) -> Arc<tokio::sync::Mutex<LoanOwner>> {
    let observation = owner.receiver.get().cloned();
    let owner = Arc::new(tokio::sync::Mutex::new(owner));
    if let Some(observation) = observation {
        let mut installed = observation
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            installed.is_none(),
            "one actual receiver per unique loan controller"
        );
        *installed = Some(Installed {
            slot: Arc::downgrade(slot),
            owner: Arc::downgrade(&owner),
        });
    }
    owner
}
impl CarrierLoanReceiver {
    /// Return the existing sealed proof only while the exact actual receiver
    /// still strongly stores this same LoanOwner. No IDs supplied by a caller.
    pub fn retained(&self) -> Option<CarrierLoanRetained> {
        let observation = self.0.upgrade()?;
        let installed = observation
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()?;
        let slot = installed.slot.upgrade()?;
        {
            let state = slot.state();
            if state.closed
                || !state
                    .loan
                    .as_ref()
                    .is_some_and(|owner| Weak::ptr_eq(&Arc::downgrade(owner), &installed.owner))
            {
                return None;
            }
        }
        CarrierAdoption { slot }.retained_loan()
    }
}

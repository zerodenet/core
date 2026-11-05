//! Sealed evidence of an actual stored logical controller, not native closure.
use super::{slot::Slot, CarrierAdoption, CarrierIo, CarrierOrigin, CarrierOwnerRegistration};
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct CarrierLoanRetained {
    receiver: Weak<Slot>,
    loan: Weak<tokio::sync::Mutex<super::loan::LoanOwner>>,
    origin: CarrierOrigin,
}
impl CarrierAdoption {
    /// Available even after a post-adoption validation error. Cleanup can find
    /// the exact controller installed before that error or any suspension.
    pub fn retained_loan(&self) -> Option<CarrierLoanRetained> {
        let state = self.slot.state();
        Some(CarrierLoanRetained {
            receiver: Arc::downgrade(&self.slot),
            loan: Arc::downgrade(state.loan.as_ref()?),
            origin: state.loan_origin.as_ref()?.clone(),
        })
    }
}
impl CarrierIo {
    pub fn retained_loan(&self) -> Option<CarrierLoanRetained> {
        CarrierAdoption {
            slot: self.slot.clone(),
        }
        .retained_loan()
    }
}
impl CarrierLoanRetained {
    fn receiver_slot(&self) -> Option<Arc<Slot>> {
        let receiver = self.receiver.upgrade()?;
        let valid = {
            let state = receiver.state();
            !state.closed
                && state
                    .loan
                    .as_ref()
                    .is_some_and(|loan| Weak::ptr_eq(&Arc::downgrade(loan), &self.loan))
                && state
                    .loan_origin
                    .as_ref()
                    .is_some_and(|origin| origin.same_original(&self.origin))
        };
        valid.then_some(receiver)
    }
    pub fn receiver_adoption(&self) -> Option<CarrierAdoption> {
        self.receiver_slot().map(|slot| CarrierAdoption { slot })
    }
    pub fn receiver_owner(&self) -> Option<CarrierOwnerRegistration> {
        let owner = self.receiver_slot()?.owner.upgrade()?;
        Some(CarrierOwnerRegistration::new(&owner))
    }
    /// Proves only this exact receiver still stores the actual LoanOwner with
    /// the supplied original in its sealed lineage. It cannot authorize I/O,
    /// register a driver, or prove that either original has closed.
    pub fn retains(&self, pending: &CarrierAdoption) -> bool {
        if self.receiver_slot().is_none() {
            return false;
        }
        let mut current = Some(self.origin.clone());
        let mut seen: Vec<CarrierOrigin> = Vec::new();
        while let Some(origin) = current {
            if origin.names_adoption(pending) {
                return true;
            }
            if seen.iter().any(|previous| previous.same_original(&origin)) {
                return false;
            }
            current = origin.parent().cloned();
            seen.push(origin);
        }
        false
    }
}

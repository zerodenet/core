//! Sealed evidence of an actual stored logical controller, not native closure.
use super::{slot::Slot, CarrierAdoption, CarrierIo, CarrierOrigin, CarrierOwnerRegistration};
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct CarrierLoanRetained {
    receiver: Weak<Slot>,
    loan: Weak<tokio::sync::Mutex<super::loan::LoanOwner>>,
    origin: CarrierOrigin,
    related: Option<Weak<super::loan::RelatedControl>>,
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
            related: state.loan_related.clone(),
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
    /// Read-only custody in the actual installed loan's sealed parent chain.
    /// This neither closes a Source nor substitutes for its driver/user joins.
    pub fn retains_origin(&self, expected: &CarrierOrigin) -> bool {
        if self.receiver_slot().is_none() {
            return false;
        }
        let mut current = Some(self.origin.clone());
        let mut seen: Vec<CarrierOrigin> = Vec::new();
        while let Some(origin) = current {
            if origin.same_original(expected) {
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
    pub(crate) fn retains_protocol_original(&self, expected: &super::ProtocolOriginal) -> bool {
        if self.receiver_slot().is_none() {
            return false;
        }
        // Parent links are immutable sealed observations. Floyd's cursor also
        // keeps cleanup fail-closed for any repeated original without a Vec.
        let mut current = Some(&self.origin);
        let mut slow = current;
        let mut fast = current;
        while let Some(original) = current {
            let matches = expected.accepted().is_some_and(|expected| {
                original
                    .accepted()
                    .is_some_and(|actual| actual.same_original(expected))
            }) || expected.metadata().is_some_and(|expected| {
                original
                    .metadata()
                    .is_some_and(|actual| actual.same_original(expected))
            }) || expected
                .adoption()
                .is_some_and(|expected| original.names_adoption(expected));
            if matches {
                return true;
            }
            current = original.parent();
            slow = slow.and_then(CarrierOrigin::parent);
            fast = fast
                .and_then(CarrierOrigin::parent)
                .and_then(CarrierOrigin::parent);
            if let (Some(slow), Some(fast)) = (slow, fast) {
                if slow.same_original(fast) {
                    return false;
                }
            }
        }
        false
    }
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
        let Some(receiver) = self.receiver_slot() else {
            return false;
        };
        if let Some(related) = &self.related {
            let matches = receiver
                .state()
                .loan_related
                .as_ref()
                .is_some_and(|actual| Weak::ptr_eq(actual, related));
            if matches
                && related
                    .upgrade()
                    .is_some_and(|actual| actual.names(pending))
            {
                return true;
            }
        }
        let mut current = Some(self.origin.clone());
        let mut seen: Vec<CarrierOrigin> = Vec::new();
        while let Some(origin) = current {
            if origin.names_adoption(pending) || origin.retains_related_control(pending) {
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

impl CarrierOwnerRegistration {
    /// Custody only, in this exact original owner's existing slots. A child
    /// protocol's metadata row may already have returned to its real cached
    /// receiver; the installed LoanOwner and actual live driver still retain
    /// the donor. This grants no I/O and proves no native or Scope closure.
    pub fn retains_cached_carrier_loan(&self, input: &CarrierAdoption) -> bool {
        self.retained_cached_input(input).is_some()
    }

    /// Exact cached receiver of an original that need not have its own driver.
    /// The LoanOwner and the actual cached driver's IO must both still exist in
    /// this registered owner. This is custody transfer, never native closure.
    pub(crate) fn retained_cached_input(
        &self,
        input: &CarrierAdoption,
    ) -> Option<(CarrierLoanRetained, super::CarrierDriverRetained)> {
        let owner = self.owner.upgrade()?;
        for index in 0..self.limit().get() {
            let slot = {
                let state = owner.state();
                if !state.live || state.stopped {
                    return None;
                }
                state.slots.get(index).cloned()
            };
            let Some(slot) = slot else { break };
            let receiver = CarrierAdoption { slot };
            let Some(loan) = receiver.retained_loan() else {
                continue;
            };
            let Some(driver) = receiver.retained_driver() else {
                continue;
            };
            if driver
                .owner()
                .is_some_and(|actual| self.same_owner(&actual))
                && driver.retains(&receiver)
                && loan.retains(input)
            {
                return Some((loan, driver));
            }
        }
        None
    }
}

mod accepted;
pub(crate) use accepted::AcceptedLoanReceiver;

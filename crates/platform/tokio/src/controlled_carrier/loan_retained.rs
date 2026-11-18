//! Sealed evidence of an actual stored logical controller, not native closure.
use super::{slot::Slot, CarrierAdoption, CarrierIo, CarrierOrigin, CarrierOwnerRegistration};
use std::sync::{Arc, Weak};

// Only weak allocation identities live on the donor Slot. Keep the recursive
// CarrierOrigin/LoanOwner types outside this passive observation boundary.
#[derive(Clone)]
pub(super) struct InstalledLoanReceiver {
    pub(super) receiver: Weak<dyn std::any::Any + Send + Sync>,
    pub(super) loan: Weak<dyn std::any::Any + Send + Sync>,
    pub(super) input_alive: Option<Weak<super::loan::State>>,
}
impl InstalledLoanReceiver {
    fn observation(&self) -> Option<CarrierLoanRetained> {
        let slot = self.receiver.upgrade()?.downcast::<Slot>().ok()?;
        {
            let state = slot.state();
            let owner = state.loan.as_ref()?;
            let actual: Weak<dyn std::any::Any + Send + Sync> =
                Arc::<tokio::sync::Mutex<super::loan::LoanOwner>>::downgrade(owner);
            if !actual.ptr_eq(&self.loan) {
                return None;
            }
        }
        let mut proof = CarrierAdoption { slot }.retained_loan()?;
        proof.input_alive = self.input_alive.clone();
        proof.receiver_slot()?;
        Some(proof)
    }
}

#[derive(Clone)]
pub struct CarrierLoanRetained {
    pub(super) receiver: Weak<dyn std::any::Any + Send + Sync>,
    pub(super) loan: Weak<dyn std::any::Any + Send + Sync>,
    pub(super) origin: CarrierOrigin,
    pub(super) related: Option<Weak<super::loan::RelatedControl>>,
    pub(super) input_alive: Option<Weak<super::loan::State>>,
}
impl CarrierAdoption {
    /// The exact NativeControl was moved into an actual retained receiver.
    /// Keep this weak observation on the donor independently of a caller's
    /// reclaimed protocol rows. It grants no close or replacement evidence.
    pub fn retained_loan_receiver(&self) -> Option<CarrierLoanRetained> {
        fn next(donor: &CarrierAdoption) -> Option<(CarrierLoanRetained, CarrierAdoption)> {
            let installed = donor.slot.state().loan_receiver.clone()?;
            let proof = installed.observation()?;
            let receiver = proof.receiver_adoption()?;
            proof.retains(donor).then_some((proof, receiver))
        }
        let mut current = self.clone();
        let mut slow = Some(self.clone());
        let mut fast = slow.clone();
        loop {
            let (proof, receiver) = next(&current)?;
            if receiver.retained_driver().is_some_and(|driver| {
                driver.retains(&receiver)
                    && driver
                        .adoption()
                        .is_some_and(|actual| actual.same_slot(&receiver))
            }) {
                return proof.retains(self).then_some(proof);
            }
            current = receiver;
            slow = slow.and_then(|actual| next(&actual).map(|(_, receiver)| receiver));
            fast = fast.and_then(|actual| {
                next(&actual)
                    .and_then(|(_, receiver)| next(&receiver))
                    .map(|(_, receiver)| receiver)
            });
            if slow
                .as_ref()
                .zip(fast.as_ref())
                .is_some_and(|(slow, fast)| slow.same_slot(fast))
            {
                return None;
            }
        }
    }
    /// Available even after a post-adoption validation error. Cleanup can find
    /// the exact controller installed before that error or any suspension.
    pub fn retained_loan(&self) -> Option<CarrierLoanRetained> {
        let state = self.slot.state();
        Some(CarrierLoanRetained {
            receiver: Arc::<Slot>::downgrade(&self.slot),
            loan: Arc::<tokio::sync::Mutex<super::loan::LoanOwner>>::downgrade(
                state.loan.as_ref()?,
            ),
            origin: state.loan_origin.as_ref()?.clone(),
            related: state.loan_related.clone(),
            input_alive: None,
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
        if self.input_alive.as_ref().is_some_and(|original| {
            original.upgrade().is_none_or(|original| {
                !original
                    .control_alive
                    .load(std::sync::atomic::Ordering::Acquire)
            })
        }) {
            return None;
        }
        let receiver = self.receiver.upgrade()?.downcast::<Slot>().ok()?;
        let valid = {
            let state = receiver.state();
            !state.closed
                && state.loan.as_ref().is_some_and(|loan| {
                    let actual: Weak<dyn std::any::Any + Send + Sync> =
                        Arc::<tokio::sync::Mutex<super::loan::LoanOwner>>::downgrade(loan);
                    actual.ptr_eq(&self.loan)
                })
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

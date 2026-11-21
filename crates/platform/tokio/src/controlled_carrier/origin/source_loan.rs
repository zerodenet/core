//! A transferred source retains its real accepted LoanOwner without a private driver.
use super::*;
impl CarrierOrigin {
    /// Read-only custody of this exact transferred original and its direct
    /// accepted parent. This grants no native IO, driver or close evidence.
    pub(in crate::controlled_carrier) fn retains_accepted_stream_loan(&self) -> bool {
        let OriginalRef::Carrier(original) = self.observation() else {
            return false;
        };
        if original.driver.is_some() {
            return false;
        }
        let Some(parent) = original.parent.as_ref() else {
            return false;
        };
        let Some(source) = parent.accepted() else {
            return false;
        };
        if source.is_retired() || !source.scope_identity().resource_open() {
            return false;
        }
        let Some(slot) = original.slot.upgrade() else {
            return false;
        };
        let retained = {
            let state = slot.state();
            !state.closed
                && state.transferred_origin
                && state.view_live
                && state.driver.is_none()
                && state.control.is_none()
                && state.packet.is_none()
                && state.loan.is_some()
                && state
                    .loan_origin
                    .as_ref()
                    .is_some_and(|held| held.same_original(parent))
        };
        if !retained {
            return false;
        }
        let adoption = CarrierAdoption { slot };
        adoption.retained_loan().is_some_and(|loan| {
            loan.receiver_adoption()
                .is_some_and(|actual| self.names_adoption(&actual))
                && loan
                    .origin
                    .accepted()
                    .is_some_and(|actual| actual.same_original(source))
        })
    }
}

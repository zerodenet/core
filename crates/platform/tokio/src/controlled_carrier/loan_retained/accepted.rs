//! Only the exact installed logical controller and its actual parent driver.
//! This record contains weak parent IO identities, never a strong child backedge.
use super::*;
use crate::controlled_carrier::{CarrierDriverRetained, ProtocolOriginalReceiver};
use crate::controlled_listener::accepted::AcceptedTcpCarrierOrigin;
#[derive(Clone)]
pub(crate) struct AcceptedLoanReceiver {
    receiver: Weak<Slot>,
    // Same original weak allocation, erased only at this passive storage
    // boundary to stop the Source/loan/packet-record auto-trait expansion.
    // No upgrade, clone of the controller, new allocation or executable SPI.
    loan: Weak<dyn std::any::Any + Send + Sync>,
    keeper: CarrierDriverRetained,
}
impl CarrierLoanRetained {
    pub(crate) fn accepted_receiver(
        &self,
        original: &AcceptedTcpCarrierOrigin,
        keeper: CarrierDriverRetained,
    ) -> Option<AcceptedLoanReceiver> {
        let parent = self.receiver_adoption()?;
        if !parent.has_original_root()
            || !keeper.retains(&parent)
            || !self.retains_origin(&CarrierOrigin::from_accepted(original.clone()))
        {
            return None;
        }
        Some(AcceptedLoanReceiver {
            receiver: self.receiver.clone(),
            loan: self.loan.clone(),
            keeper,
        })
    }
}
impl AcceptedLoanReceiver {
    pub(crate) fn observation(
        &self,
        original: &AcceptedTcpCarrierOrigin,
    ) -> Option<ProtocolOriginalReceiver> {
        let parent = self.keeper.adoption()?;
        if !self.receiver.ptr_eq(&Arc::downgrade(&parent.slot)) {
            return None;
        }
        let loan = parent.retained_loan()?;
        let actual: Weak<dyn std::any::Any + Send + Sync> = loan.loan.clone();
        if !actual.ptr_eq(&self.loan)
            || !loan.retains_origin(&CarrierOrigin::from_accepted(original.clone()))
        {
            return None;
        }
        Some(ProtocolOriginalReceiver::accepted_loan(
            original.clone(),
            loan,
            self.keeper.clone(),
        ))
    }
}

//! Weak observation on the original charged row, never another loan owner.
use super::*;
use std::sync::Weak;
use zero_platform_tokio::controlled_carrier::{loan::CarrierLoanReceiver, CarrierOrigin};

pub(crate) struct FlowReceiverBinding(Weak<Record>);
impl FlowUser {
    pub(crate) fn receiver_binding(&self) -> FlowReceiverBinding {
        FlowReceiverBinding(Arc::downgrade(&self.record))
    }
}
impl FlowReceiverBinding {
    pub(crate) fn bind(self, receiver: CarrierLoanReceiver) -> Result<(), ChannelError> {
        let record = self.0.upgrade().ok_or_else(revoked)?;
        record
            .receiver
            .set(receiver)
            .map_err(|_| ChannelError::new(Stage::Prepare, Reason::Protocol, Impact::Component))
    }
}
impl FlowUsers {
    /// Only a preflight classification of exact cached borrowers which the
    /// existing transaction will stop. The row and capacity remain unchanged;
    /// actual view return AND caller join are still required by normal cleanup.
    pub(crate) fn replacement_receivers_retained(
        &self,
        stopping: &crate::component::catalog::ReplacementUsersPause,
        source: &CarrierOrigin,
    ) -> bool {
        self.reap();
        let records = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.unaccounted || state.first.is_some() {
                return false;
            }
            state.records.clone()
        };
        // No FlowUsers lock may surround an actual receiver/driver observation.
        records.iter().all(|record| {
            if record.packet_closed.is_some() {
                return false;
            }
            let Some(caller) = &record.caller else {
                return false;
            };
            let Some(receiver) = record.receiver.get() else {
                return false;
            };
            let Some(loan) = receiver.retained() else {
                return false;
            };
            let Some(adoption) = loan.receiver_adoption() else {
                return false;
            };
            let exact_driver_scope =
                adoption.retained_driver().is_some_and(|proof| {
                    proof.retains(&adoption)
                        && caller
                            .scope_identity()
                            .same_scope(&proof.driver().scope_identity())
                }) || adoption.retained_preparation_job().is_some_and(|proof| {
                    proof.retains(&adoption)
                        && caller
                            .scope_identity()
                            .same_scope(&proof.driver().scope_identity())
                });
            exact_driver_scope && stopping.retains_source_receiver(receiver, source)
        })
    }
}

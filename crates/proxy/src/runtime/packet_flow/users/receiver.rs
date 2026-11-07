//! Weak observation on the original charged row, never another loan owner.
use super::*;
use std::sync::Weak;
use zero_platform_tokio::controlled_carrier::{loan::CarrierLoanReceiver, CarrierOrigin};

pub(crate) struct FlowReceiverBinding {
    record: Weak<Record>,
    logical: Weak<
        Mutex<
            Option<Arc<zero_platform_tokio::controlled_carrier::loan::LogicalLoanCloseParticipant>>,
        >,
    >,
}
impl FlowUser {
    pub(crate) fn receiver_binding(&self) -> FlowReceiverBinding {
        FlowReceiverBinding {
            record: Arc::downgrade(&self.record),
            logical: Arc::downgrade(&self.logical_publisher),
        }
    }
}
impl FlowReceiverBinding {
    pub(crate) fn bind(
        self,
        receiver: CarrierLoanReceiver,
        owner: &mut zero_platform_tokio::controlled_carrier::loan::LoanOwner,
    ) -> Result<(), ChannelError> {
        let record = self.record.upgrade().ok_or_else(revoked)?;
        let logical = self.logical.upgrade().ok_or_else(revoked)?;
        let participant = Arc::new(owner.take_logical_close_participant()?);
        {
            let mut installed = logical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if installed.is_some() {
                drop(installed);
                drop(participant);
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::Protocol,
                    Impact::Component,
                ));
            }
            *installed = Some(participant);
        }
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
        let (count, revision) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.unaccounted || state.first.is_some() {
                return false;
            }
            (state.records.len(), state.revision)
        };
        // Inspect only one existing row outside the owner mutex. The mutation
        // epoch below rejects a changed table, including remove+append at equal
        // length; it grants no identity, IO, admission or closure authority.
        for index in 0..count {
            let record = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .records
                .get(index)
                .cloned();
            let Some(record) = record else { return false };
            if record.packet_closed.is_some() {
                return false;
            }
            let Some(caller) = &record.caller else {
                return false;
            };
            let Some(receiver) = record.receiver.get() else {
                return false;
            };
            let source_receiver = || {
                receiver.packet_driver_retained().is_some_and(|actual| {
                    actual
                        .caller()
                        .is_some_and(|driver| driver.same_registration(caller))
                        && actual.retains_origin(source)
                        && stopping.retains_packet_driver_receiver(receiver, source)
                })
            };
            let Some(loan) = receiver.retained() else {
                if !source_receiver() {
                    return false;
                }
                continue;
            };
            let Some(adoption) = loan.receiver_adoption() else {
                if !source_receiver() {
                    return false;
                }
                continue;
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
            if !((exact_driver_scope && stopping.retains_source_receiver(receiver, source))
                || source_receiver())
            {
                return false;
            }
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        !state.unaccounted && state.first.is_none() && state.revision == revision
    }
}

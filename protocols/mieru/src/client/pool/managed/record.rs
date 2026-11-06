//! A slot survives future exit and real task join until physical closure is proven.
use super::ManagedState;
use std::sync::{Arc, Mutex};
use zero_platform_tokio::contract::scope::TaskJoinWitness;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;

pub(super) struct CarrierRecord(
    Mutex<Record>,
    zero_platform_tokio::contract::CancellationToken,
    usize,
);
#[derive(Default)]
struct Record {
    attempted: bool,
    finished: bool,
    registering: bool,
    join: Option<TaskJoinWitness>,
    adoption: Option<CarrierAdoption>,
    pending: Vec<CarrierAdoption>,
    cleanup: Option<(
        zero_platform_tokio::contract::network::UpstreamTransport,
        zero_platform_tokio::contract::network::UpstreamBinding,
    )>,
}
pub(crate) struct CarrierLease {
    pub(crate) owner: Arc<ManagedState>,
    record: Arc<CarrierRecord>,
    pub(crate) view: Option<zero_platform_tokio::controlled_carrier::CarrierUserView>,
}
impl CarrierRecord {
    pub(super) fn new(
        cancel: zero_platform_tokio::contract::CancellationToken,
        pending_limit: usize,
    ) -> Self {
        Self(Mutex::new(Record::default()), cancel, pending_limit)
    }
    fn release_installed_pending(&self) {
        let adoption = self.adoption();
        let installed = adoption.and_then(|adoption| {
            let proof = adoption.retained_loan()?;
            proof
                .receiver_adoption()
                .is_some_and(|receiver| receiver.same_slot(&adoption))
                .then_some(proof)
        });
        // Inspect actual slot/Scope facts outside the record lock. The
        // persistent pool may already have registered the donor driver before
        // this request's logical CONNECT has received its reply.
        let pending = self.pending();
        let cleanup = self.0.lock().unwrap().cleanup.clone();
        let transferred = pending
            .iter()
            .filter(|pending| {
                installed
                    .as_ref()
                    .is_some_and(|proof| proof.retains(pending))
                    || pending.retained_driver().is_some_and(|proof| {
                        proof.retains(pending)
                            && cleanup.as_ref().is_some_and(|(provider, binding)| {
                                provider.validate_pending_carrier(binding, pending).is_ok()
                            })
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        {
            let mut record = self.0.lock().unwrap();
            record.pending.retain(|pending| {
                !transferred
                    .iter()
                    .any(|original| original.same_slot(pending))
            });
        }
        // Original adoptions are released outside the record mutex. Neither
        // handoff claims native closure, driver exit or a refunded pool slot.
    }
    pub(super) fn pending(&self) -> Vec<CarrierAdoption> {
        self.0.lock().unwrap().pending.clone()
    }
    pub(super) fn request_retirement(&self) {
        self.1.cancel();
    }
    pub(super) fn io_attempted(&self) -> bool {
        self.0.lock().unwrap().attempted
    }
    pub(super) fn body_finished(&self) -> bool {
        let record = self.0.lock().unwrap();
        record.finished && !record.registering
    }
    pub(super) async fn joined(&self) {
        let join = self.0.lock().unwrap().join.clone();
        if let Some(join) = join {
            let _ = join.wait().await;
        }
    }
    pub(super) fn adoption(&self) -> Option<CarrierAdoption> {
        self.0.lock().unwrap().adoption.clone()
    }
    pub(super) fn closed(&self, instance: zero_core::component::ComponentInstanceId) -> bool {
        let record = self.0.lock().unwrap();
        if !record.finished
            || record.registering
            || record.pending.iter().any(|pending| {
                pending
                    .closed()
                    .is_none_or(|receipt| !receipt.matches(pending))
            })
        {
            return false;
        }
        let Some(adoption) = &record.adoption else {
            return false;
        };
        let Some(receipt) = adoption.closed() else {
            return false;
        };
        if receipt.instance() != instance || !receipt.matches(adoption) {
            return false;
        }
        match (&record.join, receipt.driver_exit()) {
            (None, None) => true,
            (Some(witness), Some(exit)) => witness
                .try_joined()
                .is_some_and(|joined| joined.id() == exit.id() && joined.class() == exit.class()),
            _ => false,
        }
    }
}
impl CarrierLease {
    pub(crate) fn cancellation(&self) -> zero_platform_tokio::contract::CancellationToken {
        self.record.1.clone()
    }
    pub(super) fn new(owner: Arc<ManagedState>, record: Arc<CarrierRecord>) -> Self {
        Self {
            owner,
            record,
            view: None,
        }
    }
    pub(crate) fn attempting_io(&self) {
        self.record.0.lock().unwrap().attempted = true;
    }
    pub(crate) fn release_installed_pending(&self) {
        self.record.release_installed_pending();
    }
    pub(crate) fn pending_carriers(&self) -> PendingCarriers {
        PendingCarriers {
            record: self.record.clone(),
        }
    }
    pub(crate) fn adopt(
        &self,
        adoption: CarrierAdoption,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        if adoption.instance() != self.owner.instance {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        let mut record = self.record.0.lock().unwrap();
        if record.registering
            || record.finished
            || record.join.is_some()
            || (record.attempted && record.adoption.is_none())
            || record.adoption.as_ref().is_some_and(|prior| {
                prior.closed().is_none_or(|receipt| {
                    !receipt.matches(prior) || receipt.driver_exit().is_some()
                })
            })
        {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        // A previous candidate may be replaced only after its same-original
        // closure was received. Keep no unbounded completed-native history.
        let previous = record.adoption.replace(adoption);
        drop(record);
        drop(previous);
        Ok(())
    }
    pub(crate) fn registering(&self) -> Registration {
        self.record.0.lock().unwrap().registering = true;
        Registration {
            owner: self.owner.clone(),
            record: self.record.clone(),
        }
    }
}
pub(crate) struct Registration {
    owner: Arc<ManagedState>,
    record: Arc<CarrierRecord>,
}
impl Registration {
    pub(crate) fn registered(&self, witness: TaskJoinWitness) {
        self.record.0.lock().unwrap().join = Some(witness);
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.record.0.lock().unwrap().registering = false;
        self.owner.changed.notify_waiters();
    }
}
impl Drop for CarrierLease {
    fn drop(&mut self) {
        // Also recover installation after an error or cancellation before the
        // caller could observe the prepared receiver.
        self.record.release_installed_pending();
        let attempted = {
            let mut record = self.record.0.lock().unwrap();
            record.finished = true;
            record.attempted
        };
        if !attempted {
            self.owner.release_unattempted(&self.record);
        }
        self.owner.changed.notify_waiters();
    }
}

#[derive(Clone)]
pub(crate) struct PendingCarriers {
    record: Arc<CarrierRecord>,
}
impl PendingCarriers {
    pub(crate) fn bind_cleanup_provider(
        &self,
        provider: &zero_platform_tokio::contract::network::UpstreamTransport,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let mut record = self.record.0.lock().unwrap();
        if record
            .cleanup
            .as_ref()
            .is_some_and(|(_, original)| original != binding)
        {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        if record.cleanup.is_none() {
            record.cleanup = Some((provider.clone(), binding.clone()));
        }
        Ok(())
    }
    pub(crate) fn capacity(&self) -> Option<usize> {
        let record = self.record.0.lock().unwrap();
        (!record.finished && record.attempted && record.adoption.is_some()).then_some(self.record.2)
    }
    /// Actual foreign cleanup authority is bounded by this selected prefix's
    /// role count. It never substitutes for this pool record's root original.
    pub(crate) fn retain_pending(
        &self,
        adoption: CarrierAdoption,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let mut record = self.record.0.lock().unwrap();
        if record
            .pending
            .iter()
            .any(|prior| prior.same_slot(&adoption))
        {
            return Ok(());
        }
        let reason = if record.finished || !record.attempted || record.adoption.is_none() {
            Some(zero_platform_tokio::contract::Reason::StalePlan)
        } else if record.pending.len() >= self.record.2 {
            Some(zero_platform_tokio::contract::Reason::Capacity)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                reason,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        record.pending.push(adoption);
        Ok(())
    }
}

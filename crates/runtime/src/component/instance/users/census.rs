//! Index only the actual Lease objects already charged by this Users root.
use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierAdoption, CarrierLoanRetained};
#[derive(Default)]
pub(super) struct Admission {
    pub(super) paused: usize,
    pub(super) leases: Vec<std::sync::Weak<Lease>>,
}
impl Users {
    pub(in crate::component) fn try_pause_receivers(
        self: &Arc<Self>,
        receiver: impl Fn(&Lease) -> Option<CarrierAdoption>,
    ) -> Result<PausedUsers, ChannelError> {
        let busy = || {
            invalid(
                Reason::Capacity,
                "replacement retains active component users",
            )
        };
        // No scope/provider/callback inspection under the admission mutex.
        let leases = {
            let mut admission = self
                .admission_paused
                .lock()
                .expect("user admission poisoned");
            admission.leases.retain(|lease| lease.strong_count() != 0);
            let mut leases = Vec::new();
            for actual in &admission.leases {
                if let Some(lease) = actual.upgrade() {
                    if !leases.iter().any(|previous| Arc::ptr_eq(previous, &lease)) {
                        leases.push(lease);
                    }
                }
            }
            leases
        };
        let mut receivers = Vec::new();
        for lease in &leases {
            receivers.push(receiver(lease).ok_or_else(busy)?);
        }
        let mut admission = self
            .admission_paused
            .lock()
            .expect("user admission poisoned");
        // These exact strong snapshot references keep their existing credits
        // stable during classification. A new or partially indexed acquire can
        // only add credit; it cannot disappear behind an expired Weak/count.
        if self.active.load(Ordering::Acquire) != leases.len() {
            return Err(busy());
        }
        admission.paused = admission
            .paused
            .checked_add(1)
            .ok_or_else(|| invalid(Reason::Capacity, "user pause count exhausted"))?;
        drop(admission);
        // Never store a strong Lease in the pause guard: IO must be able to
        // release its original counter while StopOld is still holding the guard.
        drop(leases);
        Ok(PausedUsers {
            actual_instance: None,
            owner: self.clone(),
            _receivers: receivers,
        })
    }
}
impl Lease {
    pub(in crate::component) fn bind_receiver(
        &self,
        receiver: zero_platform_tokio::controlled_carrier::loan::CarrierLoanReceiver,
    ) {
        assert!(
            self.receiver.set(receiver).is_ok(),
            "one returned carrier loan per actual lease"
        );
    }
    pub(in crate::component) fn retained_receiver(&self) -> Option<CarrierLoanRetained> {
        if self.session.is_some() {
            return None;
        }
        self.receiver.get()?.retained()
    }
}

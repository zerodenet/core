//! Finite internal type erasure, only for the three exact original record types.
//! No factory, config, scope, native-I/O grant, scheduler or external extension.
use super::*;
use std::time::Duration;
pub(super) trait CarrierCustody: Send + Sync {
    /// Same concrete original/binding copy; no generation from error metadata.
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId>;
    fn released(&self) -> bool;
    fn preparation_usage(&self) -> [usize; 2];
    fn owner_identity(&self) -> usize;
    fn poll_cancelled_owner(&self, cx: &mut Context<'_>) -> bool;
    fn poll_ack(&self, cx: &mut Context<'_>) -> bool;
    fn failure(&self) -> Option<Arc<ChannelError>>;
}
pub(crate) trait MetadataCustody: Send + Sync {
    /// Same concrete original/binding copy; no generation from error metadata.
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId>;
    fn released(&self) -> bool;
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool;
    fn failure(&self) -> Option<Arc<ChannelError>>;
}
pub(crate) trait BoundCustody: Send + Sync {
    /// Same concrete original/binding copy; no generation from error metadata.
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId>;
    fn released(&self) -> bool;
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool;
    fn failure(&self) -> Option<Arc<ChannelError>>;
}
impl CarrierCustody for Original {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(self.owner.instance)
    }
    fn released(&self) -> bool {
        Original::released(self)
    }
    fn preparation_usage(&self) -> [usize; 2] {
        self.slot.preparation_usage.snapshot()
    }
    fn owner_identity(&self) -> usize {
        Arc::as_ptr(&self.owner) as usize
    }
    fn poll_cancelled_owner(&self, cx: &mut Context<'_>) -> bool {
        let (progress, fault) = super::super::CarrierOwnerRegistration::new(&self.owner)
            .poll_cancelled_retirements(cx, Duration::from_secs(5));
        if let Some(fault) = fault {
            self.first_fault
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(fault);
        }
        progress
    }
    fn poll_ack(&self, cx: &mut Context<'_>) -> bool {
        if !self.slot.removed.load(Ordering::Acquire) {
            return false;
        }
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return false;
        };
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                let witnesses = self.witnesses();
                Box::pin(async move {
                    for witness in witnesses {
                        witness.wait_registration_released().await;
                    }
                })
            });
        let ready = waiting.as_mut().poll(cx).is_ready();
        if !ready {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
        }
        ready
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        self.first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .or_else(|| {
                self.slot
                    .state()
                    .protocol
                    .as_ref()
                    .and_then(|phase| phase.failure())
            })
    }
}

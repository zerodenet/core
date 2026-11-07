//! Exact aggregate custody of this same pool original, including all owned probes.
use super::*;
use crate::contract::BoxFuture;
use std::{sync::atomic::AtomicBool, task::Context};

pub(crate) struct MetadataTotalsOriginal {
    // The real MetadataOwner and its main/probe controllers stay strong via
    // the exact Slot, even when its protocol's sole pool Owner disappears.
    owner: Arc<Shared>,
    slot: Arc<Slot>,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
}
impl MetadataTotalsOriginal {
    pub(super) fn new(owner: Arc<Shared>, slot: Arc<Slot>) -> Self {
        Self {
            owner,
            slot,
            waiting: Mutex::default(),
            polling: AtomicBool::new(false),
            repoll: AtomicBool::new(false),
            first_fault: Mutex::default(),
        }
    }
    pub(crate) fn instance(&self) -> ComponentInstanceId {
        self.slot.instance
    }
    fn witnesses(
        &self,
    ) -> Option<(
        TaskJoinWitness,
        Option<super::super::metadata::users::MetadataUsersClosedReceipt>,
    )> {
        let native = self.slot.native().ok()?;
        Some((self.slot.caller.clone(), native.confirmed.clone()))
    }
    pub(crate) fn released(&self) -> bool {
        // Only close_stored publishes this after exact original main+probe
        // native receipts and the original returned/unbound users graph.
        if self.slot.closed.get().is_none() {
            return false;
        }
        MetadataPoolPermit(self.slot.clone()).release_registration_credit()
    }
    pub(crate) fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let mut changed = false;
        // A successful bootstrap query's join does not revoke a cached QUIC
        // original. Only the actual provider/permit's cancellation does so.
        if self.slot.closed.get().is_none()
            && (self.owner.cancel.is_cancelled() || self.slot.retired.is_cancelled())
        {
            match MetadataPoolPermit(self.slot.clone()).close_stored() {
                Ok(Some(_)) => changed = true,
                Ok(None) => {}
                Err(source) => {
                    self.first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| Arc::new(source));
                }
            }
        }
        if self.slot.closed.get().is_none() {
            return changed;
        }
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return changed;
        };
        let waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let mut waiting = waiting.or_else(|| {
            self.witnesses().map(|(caller, users)| {
                Box::pin(async move {
                    caller.wait_registration_released().await;
                    if let Some(users) = users {
                        for witness in users
                            .registrations()
                            .iter()
                            .chain(users.related_registrations())
                        {
                            witness.wait_registration_released().await;
                        }
                    }
                }) as BoxFuture<'static, ()>
            })
        });
        let ready = waiting
            .as_mut()
            .is_some_and(|waiting| waiting.as_mut().poll(cx).is_ready());
        changed |= ready;
        if !ready {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = waiting;
        }

        changed
    }
    pub(crate) fn failure(&self) -> Option<Arc<ChannelError>> {
        self.first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl crate::controlled_carrier::MetadataCustody for MetadataTotalsOriginal {
    fn released(&self) -> bool {
        MetadataTotalsOriginal::released(self)
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        MetadataTotalsOriginal::poll_retirement(self, cx)
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        MetadataTotalsOriginal::failure(self)
    }
}

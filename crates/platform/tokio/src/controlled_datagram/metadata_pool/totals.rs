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
    requested: Mutex<Option<BoxFuture<'static, ()>>>,
    protocol_end: OnceLock<tokio::time::Instant>,
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
            requested: Mutex::default(),
            protocol_end: OnceLock::new(),
        }
    }
    pub(crate) fn instance(&self) -> ComponentInstanceId {
        self.slot.instance
    }
    fn witnesses(
        &self,
    ) -> Option<(
        Option<TaskJoinWitness>,
        Option<super::super::metadata::users::MetadataUsersClosedReceipt>,
    )> {
        let native = self.slot.native().ok()?;
        Some((self.slot.caller.get().cloned(), native.confirmed.clone()))
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
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return false;
        };
        let replacement = cx.waker().clone();
        let previous = self
            .slot
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(replacement);
        drop(previous);
        let mut changed = false;
        // Keep the original retirement wake registered even before native I/O
        // or an actor exists. Abandoning a cold guard requires no next dial.
        let requested = self
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let mut requested = requested.unwrap_or_else(|| {
            let retired = self.slot.retired.clone();
            Box::pin(async move { retired.cancelled().await })
        });
        if requested.as_mut().poll(cx).is_pending() {
            *self
                .requested
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(requested);
        }
        let phase = self
            .slot
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let host_cancelled = self
            .owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .as_ref()
            .is_some_and(|bound| bound.host_cancel.is_cancelled());
        let requested =
            self.owner.cancel.is_cancelled() || self.slot.retired.is_cancelled() || host_cancelled;
        if let Some(phase) = &phase {
            phase.register_waker(cx.waker());
            if requested && !phase.requested() {
                phase.request();
            }
            if phase.requested() && !phase.complete() {
                let end = *self.protocol_end.get_or_init(|| {
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5)
                });
                changed |= matches!(phase.poll_operation(cx, end), std::task::Poll::Ready(true));
                if let Some(source) = phase.failure() {
                    self.first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert(source);
                }
            }
        }
        // A successful bootstrap query's join does not revoke a cached QUIC
        // original. Only the actual provider/permit's cancellation does so.
        if self.slot.closed.get().is_none()
            && (requested || phase.as_ref().is_some_and(|phase| phase.requested()))
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
        let waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let mut waiting = waiting.or_else(|| {
            self.witnesses().map(|(caller, users)| {
                let scope = self
                    .slot
                    .registration_uncertain
                    .load(std::sync::atomic::Ordering::Acquire)
                    .then(|| self.slot.registration_scope.clone())
                    .flatten();
                Box::pin(async move {
                    if let Some(scope) = scope {
                        scope.wait_drained().await;
                    }
                    if let Some(caller) = caller {
                        caller.wait_registration_released().await;
                    }
                    if let Some(users) = users {
                        for witness in users.registrations().chain(users.related_registrations()) {
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
            .or_else(|| {
                self.slot
                    .protocol
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .and_then(|phase| phase.failure())
            })
    }
}

impl crate::controlled_carrier::MetadataCustody for MetadataTotalsOriginal {
    fn resource_failure(&self) -> Option<Arc<crate::contract::lifecycle::ResourceFailure>> {
        self.slot
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|phase| phase.resource_failure())
    }
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(MetadataTotalsOriginal::instance(self))
    }
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

//! A Source's same actual UDP Entry, views and real Scope census.
use super::*;
use crate::{
    contract::{
        scope::ScopeRetirementObservation, BoxFuture, CancellationToken, ChannelError,
        DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierOriginalTotals, SourceCustody},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Weak,
    },
    task::{Context, Poll},
};
use zero_core::component::ComponentInstanceId;
#[derive(Clone)]
pub struct SourceDatagramTotalsBinding {
    root: Weak<CarrierOriginalTotals>,
    instance: ComponentInstanceId,
    scope: ScopeRetirementObservation,
    stop: CancellationToken,
}
impl SourceDatagramTotalsBinding {
    pub fn new(
        root: &Arc<CarrierOriginalTotals>,
        instance: ComponentInstanceId,
        scope: ScopeRetirementObservation,
        stop: CancellationToken,
    ) -> Self {
        Self {
            root: Arc::downgrade(root),
            instance,
            scope,
            stop,
        }
    }
    fn same(&self, other: &Self) -> bool {
        self.root.ptr_eq(&other.root)
            && self.instance == other.instance
            && self
                .scope
                .scope_identity()
                .same_scope(&other.scope.scope_identity())
    }
    pub(super) fn reserve(&self, entry: &Arc<Entry>) -> Result<Charge, ChannelError> {
        let root = self
            .root
            .upgrade()
            .ok_or_else(|| failure(self.instance, Reason::Revoked))?;
        let original: Arc<dyn SourceCustody> = Arc::new(Original {
            entry: entry.clone(),
            binding: self.clone(),
            waiting: Mutex::default(),
            polling: AtomicBool::new(false),
            repoll: AtomicBool::new(false),
            first_fault: Mutex::default(),
            wait_finished: AtomicBool::new(false),
        });
        root.reserve_source(self.instance, original.clone())?;
        Ok(Charge {
            root: Arc::downgrade(&root),
            original: Arc::downgrade(&original),
        })
    }
}
impl ControlledDatagramOwner {
    pub fn bind_source_totals(
        &mut self,
        binding: SourceDatagramTotalsBinding,
    ) -> std_io::Result<()> {
        if let Some(actual) = &self.source_totals {
            return if actual.same(&binding) {
                Ok(())
            } else {
                Err(std_io::Error::other(
                    "Source UDP cannot change sameRoot/instance/Scope",
                ))
            };
        }
        if self.next != 0 || !self.entries.is_empty() || self.totals.is_some() {
            return Err(std_io::Error::other(
                "Source UDP root must bind on its actual cold owner",
            ));
        }
        self.source_totals = Some(binding);
        Ok(())
    }
}
pub(super) struct Charge {
    root: Weak<CarrierOriginalTotals>,
    original: Weak<dyn SourceCustody>,
}
impl Entry {
    pub(super) fn source_failed_return(&self) {
        self.source_failed.store(true, Ordering::Release);
        if let Some(charge) = self.source_charge.get() {
            if let (Some(root), Some(original)) = (charge.root.upgrade(), charge.original.upgrade())
            {
                root.release_source_if_settled(&original);
            }
        }
    }
}
struct Original {
    entry: Arc<Entry>,
    binding: SourceDatagramTotalsBinding,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
    wait_finished: AtomicBool,
}
impl Original {
    fn users_returned(&self) -> bool {
        self.entry.listener_users.get().map_or_else(
            || !self.entry.view_live.load(Ordering::Acquire),
            |users| users.returned(),
        )
    }
    fn native_closed(&self) -> bool {
        let native = match self.entry.native.try_lock() {
            Ok(native) => native,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return false,
        };
        matches!(
            &*native,
            NativeHandleState::Closed | NativeHandleState::NoNative
        )
    }
}
impl SourceCustody for Original {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(self.binding.instance)
    }
    fn released(&self) -> bool {
        if !self.native_closed() || !self.users_returned() {
            return false;
        }
        if self.entry.source_failed.load(Ordering::Acquire)
            && !self.entry.source_published.load(Ordering::Acquire)
        {
            return true;
        }
        !self.entry.creation_running.load(Ordering::Acquire) && self.binding.scope.is_drained()
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let mut progress = false;
        if (self.binding.stop.is_cancelled() || self.binding.scope.is_drained())
            && !self.native_closed()
        {
            self.entry.retire_io();
            match self.entry.try_close() {
                Some(Ok(_)) => progress = true,
                Some(Err(source)) => {
                    self.first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| {
                            let mut error =
                                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                    .with_source(source);
                            error.component = Some(self.binding.instance.component);
                            error.delivery = DeliveryState::Unknown;
                            Arc::new(error)
                        });
                }
                None => {}
            }
        }
        if self.wait_finished.load(Ordering::Acquire) {
            return progress;
        }
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return progress;
        };
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                let entry = self.entry.clone();
                let scope = self.binding.scope.clone();
                Box::pin(async move {
                    loop {
                        let changed = entry.source_changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        if !entry.creation_running.load(Ordering::Acquire)
                            && entry.listener_users.get().map_or_else(
                                || !entry.view_live.load(Ordering::Acquire),
                                |users| users.returned(),
                            )
                        {
                            break;
                        }
                        changed.await;
                    }
                    scope.wait_drained().await;
                })
            });
        match waiting.as_mut().poll(cx) {
            Poll::Pending => {
                *self
                    .waiting
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
            }
            Poll::Ready(()) => {
                self.wait_finished.store(true, Ordering::Release);
                progress = true;
            }
        }
        progress
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        let mut stored = self
            .first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(source) = self.entry.first_close_failure.get() {
            stored.get_or_insert_with(|| {
                let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(source.clone());
                error.component = Some(self.binding.instance.component);
                error.delivery = DeliveryState::Unknown;
                Arc::new(error)
            });
        }
        stored.clone()
    }
}
fn failure(instance: ComponentInstanceId, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.component = Some(instance.component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}

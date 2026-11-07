//! Exact Source metadata owner, main/probe originals and its original user book.
//! One custody reservation covers this actual owner; it is not a socket count.
mod users;
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
pub use users::MetadataSourceUsers;
use zero_core::component::ComponentInstanceId;

pub struct MetadataSourceTotals {
    root: Weak<CarrierOriginalTotals>,
    original: Arc<Original>,
    reserved: Mutex<bool>,
}
struct Original {
    owner: Arc<Mutex<MetadataOwner>>,
    users: Arc<MetadataSourceUsers>,
    instance: ComponentInstanceId,
    scope: ScopeRetirementObservation,
    stop: CancellationToken,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    wait_finished: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
    changed: Arc<tokio::sync::Notify>,
}
impl MetadataSourceTotals {
    /// Cold identity/custody binding only. No original is created or charged.
    pub fn new(
        root: &Arc<CarrierOriginalTotals>,
        instance: ComponentInstanceId,
        scope: ScopeRetirementObservation,
        stop: CancellationToken,
        owner: Arc<Mutex<MetadataOwner>>,
        users: Arc<MetadataSourceUsers>,
    ) -> Self {
        let changed = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .source_changed
            .clone();
        Self {
            root: Arc::downgrade(root),
            original: Arc::new(Original {
                owner,
                users,
                instance,
                scope,
                stop,
                changed,
                waiting: Mutex::default(),
                polling: AtomicBool::new(false),
                repoll: AtomicBool::new(false),
                wait_finished: AtomicBool::new(false),
                first_fault: Mutex::default(),
            }),
            reserved: Mutex::new(false),
        }
    }
    /// Called before the actual owner's first native main or capability probe.
    /// The same owner remains strongly retained across failed/retiring attempts.
    pub fn reserve(&self) -> Result<(), ChannelError> {
        let mut reserved = self
            .reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *reserved {
            return Ok(());
        }
        let root = self.root.upgrade().ok_or_else(|| {
            let mut error = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
            error.component = Some(self.original.instance.component);
            error.delivery = DeliveryState::NotSubmitted;
            error
        })?;
        root.reserve_source(self.original.instance, self.original.clone())?;
        *reserved = true;
        Ok(())
    }
}
impl MetadataOwner {
    fn source_settled(&self) -> bool {
        let settled = |owner: &ControlledDatagramOwner| {
            owner.entries.values().all(|entry| {
                if entry.creation_running.load(Ordering::Acquire) {
                    return false;
                }
                let native = match entry.native.try_lock() {
                    Ok(native) => native,
                    Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                    Err(std::sync::TryLockError::WouldBlock) => return false,
                };
                matches!(
                    &*native,
                    NativeHandleState::Closed | NativeHandleState::NoNative
                )
            })
        };
        settled(&self.main)
            && settled(&self.probes.owner)
            && self.views.values().all(|shared| shared.views.empty())
    }
    /// Harvest only after the actual native operations ran outside this owner
    /// mutex. Preserve every original report delta for the Source consumer.
    fn source_capture_reports(&mut self) {
        for probe in [false, true] {
            let (owner, pending) = if probe {
                (&mut self.probes.owner, &mut self.pending_probes)
            } else {
                (&mut self.main, &mut self.pending_main)
            };
            if let Some(report) = owner.try_take_report() {
                if let Some(old) = pending.as_mut() {
                    old.confirmed_closed += report.confirmed_closed;
                    old.known_no_native += report.known_no_native;
                    old.retained_live = report.retained_live;
                    old.unconfirmed = report.unconfirmed;
                } else {
                    *pending = Some(report);
                }
            }
        }
    }
}
impl SourceCustody for Original {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(self.instance)
    }
    fn released(&self) -> bool {
        let Some(owner) = try_owner(&self.owner) else {
            return false;
        };
        owner.source_settled() && self.users.pending() == 0 && self.scope.is_drained()
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        if self.stop.is_cancelled() || self.scope.is_drained() {
            let close = try_owner(&self.owner).map(|owner| owner.prepare_native_close());
            if let Some(close) = close {
                // Native deregistration may invoke arbitrary IO wakers. No
                // MetadataOwner or Runtime Native state lock is held here.
                if let Some(source) = close.try_close() {
                    self.first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| {
                            let mut failure =
                                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                    .with_source(source);
                            failure.component = Some(self.instance.component);
                            failure.delivery = DeliveryState::Unknown;
                            Arc::new(failure)
                        });
                }
                if let Some(mut owner) = try_owner(&self.owner) {
                    owner.source_capture_reports();
                }
                // These strong original snapshots also keep any final Entry/
                // poller destructor outside the owner's bookkeeping mutex.
                drop(close);
            }
        }
        if self.wait_finished.load(Ordering::Acquire) {
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
                let scope = self.scope.clone();
                let users = self.users.clone();
                let changed = self.changed.clone();
                let owner = self.owner.clone();
                Box::pin(async move {
                    scope.wait_drained().await;
                    loop {
                        let views_changed = changed.notified();
                        let users_changed = users.changed.notified();
                        tokio::pin!(views_changed, users_changed);
                        views_changed.as_mut().enable();
                        users_changed.as_mut().enable();
                        if users.pending() == 0
                            && try_owner(&owner).is_some_and(|owner| {
                                owner.views.values().all(|shared| shared.views.empty())
                            })
                        {
                            break;
                        }
                        tokio::select! { _ = views_changed => {}, _ = users_changed => {} }
                    }
                })
            });
        match waiting.as_mut().poll(cx) {
            Poll::Pending => {
                *self
                    .waiting
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
                false
            }
            Poll::Ready(()) => {
                self.wait_finished.store(true, Ordering::Release);
                true
            }
        }
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        self.first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

fn try_owner(owner: &Mutex<MetadataOwner>) -> Option<MutexGuard<'_, MetadataOwner>> {
    match owner.try_lock() {
        Ok(owner) => Some(owner),
        Err(std::sync::TryLockError::Poisoned(error)) => Some(error.into_inner()),
        Err(std::sync::TryLockError::WouldBlock) => None,
    }
}

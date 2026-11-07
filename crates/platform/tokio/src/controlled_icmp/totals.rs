//! Exact ICMP original custody; probe ACK and startup Scope are distinct units.
use super::*;
use crate::{
    contract::{
        scope::{ScopeRetirementObservation, TaskJoinWitness},
        BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
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
pub struct IcmpTotalsBinding {
    root: Weak<CarrierOriginalTotals>,
    instance: ComponentInstanceId,
    scope: ScopeRetirementObservation,
    stop: CancellationToken,
}
impl IcmpTotalsBinding {
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
    pub(super) fn reserve(
        &self,
        entry: &Arc<Entry>,
        caller: Option<TaskJoinWitness>,
    ) -> Result<(), ChannelError> {
        let fail = |reason| {
            let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
            error.component = Some(self.instance.component);
            error.delivery = DeliveryState::NotSubmitted;
            error
        };
        if self.stop.is_cancelled()
            || !self.scope.scope_identity().resource_open()
            || caller.as_ref().is_some_and(|caller| {
                caller.is_joined()
                    || !caller
                        .scope_identity()
                        .same_scope(&self.scope.scope_identity())
            })
        {
            return Err(fail(Reason::StalePlan));
        }
        let root = self.root.upgrade().ok_or_else(|| fail(Reason::Revoked))?;
        root.reserve_source(
            self.instance,
            Arc::new(Original {
                entry: entry.clone(),
                binding: self.clone(),
                caller,
                waiting: Mutex::default(),
                polling: AtomicBool::new(false),
                repoll: AtomicBool::new(false),
                waited: AtomicBool::new(false),
                first_fault: Mutex::default(),
            }),
        )
    }
}
impl ControlledIcmpOwner {
    pub fn bind_original_totals(&mut self, binding: IcmpTotalsBinding) -> std_io::Result<()> {
        if let Some(actual) = &self.totals {
            return if actual.root.ptr_eq(&binding.root)
                && actual.instance == binding.instance
                && actual
                    .scope
                    .scope_identity()
                    .same_scope(&binding.scope.scope_identity())
            {
                Ok(())
            } else {
                Err(std_io::Error::other(
                    "ICMP original root/instance/Scope differs",
                ))
            };
        }
        if self.next != 0 || !self.entries.is_empty() {
            return Err(std_io::Error::other(
                "ICMP total must bind on the original cold owner",
            ));
        }
        self.totals = Some(binding);
        Ok(())
    }
}
pub(super) struct Creation(pub(super) Arc<Entry>);
impl Drop for Creation {
    fn drop(&mut self) {
        // This is only the creator's synchronous return/unwind, never closure.
        self.0.creation_running.store(false, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
struct Original {
    entry: Arc<Entry>,
    binding: IcmpTotalsBinding,
    caller: Option<TaskJoinWitness>,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    waited: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
}
impl Original {
    fn native_closed(&self) -> bool {
        let Some(native) = relay::NativeGuard::try_lock(&self.entry) else {
            return false;
        };
        matches!(&*native, Native::Closed | Native::NeverAcquired)
    }
}
impl SourceCustody for Original {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(self.binding.instance)
    }
    fn released(&self) -> bool {
        if self.entry.creation_running.load(Ordering::Acquire)
            || self.entry.view_live.load(Ordering::Acquire)
            || !self.native_closed()
        {
            return false;
        }
        if let Some(caller) = &self.caller {
            caller.try_joined().is_some_and(|joined| {
                caller.matches_joined(&joined) && joined.registration_released()
            })
        } else {
            self.binding.scope.is_drained()
        }
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let mut progress = false;
        if (self.entry.cancel.is_cancelled()
            || self.binding.stop.is_cancelled()
            || self.binding.scope.is_drained())
            && !self.native_closed()
        {
            match self.entry.try_close() {
                Some(Ok(_)) => progress = true,
                Some(Err(source)) => {
                    self.first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| {
                            let mut failure =
                                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                    .with_source(source);
                            failure.component = Some(self.binding.instance.component);
                            failure.delivery = DeliveryState::Unknown;
                            Arc::new(failure)
                        });
                }
                None => {}
            }
        }
        if self.waited.load(Ordering::Acquire) {
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
                let caller = self.caller.clone();
                let scope = self.binding.scope.clone();
                Box::pin(async move {
                    loop {
                        let changed = entry.changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        if !entry.creation_running.load(Ordering::Acquire)
                            && !entry.view_live.load(Ordering::Acquire)
                        {
                            break;
                        }
                        changed.await;
                    }
                    if let Some(caller) = caller {
                        caller.wait_registration_released().await;
                    } else {
                        scope.wait_drained().await;
                    }
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
                self.waited.store(true, Ordering::Release);
                progress = true;
            }
        }
        progress
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        self.first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

//! Original Bound UDP Entry custody in the same actual Host aggregate.
use super::*;
use crate::contract::{
    scope::TaskJoinWitness, BoxFuture, CancellationToken, ChannelError, DeliveryState, Impact,
    Reason, Stage,
};
use crate::controlled_carrier::CarrierOriginalTotals;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    task::Context,
};
use zero_core::component::ComponentInstanceId;
#[derive(Clone)]
pub struct BoundDatagramTotalsBinding {
    pub(super) root: Arc<CarrierOriginalTotals>,
    instance: ComponentInstanceId,
    caller: TaskJoinWitness,
    cancel: CancellationToken,
}
pub(crate) struct BoundTotalsOriginal {
    entry: Arc<Entry>,
    binding: BoundDatagramTotalsBinding,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
    first_fault: Mutex<Option<Arc<ChannelError>>>,
}
impl BoundDatagramTotalsBinding {
    /// Actual Scope getters may wake/drop external owners; prepare outside
    /// every provider/native bookkeeping lock. No IO capability is produced.
    pub fn prepare(
        root: &Arc<CarrierOriginalTotals>,
        instance: ComponentInstanceId,
        caller: &TaskJoinWitness,
        cancel: &CancellationToken,
    ) -> std_io::Result<Self> {
        if caller.is_joined() || !caller.scope_identity().resource_open() || cancel.is_cancelled() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::NotConnected,
                "original UDP caller was stopped",
            ));
        }
        Ok(Self {
            root: root.clone(),
            instance,
            caller: caller.clone(),
            cancel: cancel.clone(),
        })
    }
}
impl ControlledDatagramOwner {
    /// Install the same cold accounting facts before any original creation.
    /// No original Scope/native getter runs under the containing owner lock.
    pub fn bind_bound_totals(&mut self, binding: BoundDatagramTotalsBinding) -> std_io::Result<()> {
        let BoundDatagramTotalsBinding {
            root,
            instance,
            caller,
            ..
        } = &binding;
        if let Some(actual) = &self.totals {
            if !Arc::ptr_eq(&actual.root, root)
                || actual.instance != *instance
                || !actual.caller.same_registration(caller)
            {
                return Err(std_io::Error::new(
                    std_io::ErrorKind::InvalidInput,
                    "UDP original cannot change actual Host root/caller",
                ));
            }
            return Ok(());
        }
        if self.next != 0 || !self.entries.is_empty() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "UDP total must bind before first original",
            ));
        }
        self.totals = Some(binding);
        Ok(())
    }
}
impl BoundTotalsOriginal {
    pub(super) fn new(entry: Arc<Entry>, binding: BoundDatagramTotalsBinding) -> Self {
        Self {
            entry,
            binding,
            waiting: Mutex::default(),
            polling: AtomicBool::new(false),
            repoll: AtomicBool::new(false),
            first_fault: Mutex::default(),
        }
    }
    pub(crate) fn instance(&self) -> ComponentInstanceId {
        self.binding.instance
    }
    fn native_settled(&self) -> bool {
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
    pub(crate) fn released(&self) -> bool {
        if !self.native_settled() || self.entry.view_live.load(Ordering::Acquire) {
            return false;
        }
        self.binding.caller.try_joined().is_some_and(|joined| {
            self.binding.caller.matches_joined(&joined) && joined.registration_released()
        })
    }
    pub(crate) fn failure(&self) -> Option<Arc<ChannelError>> {
        self.first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(crate) fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let mut changed = false;
        if self.binding.cancel.is_cancelled() && !self.native_settled() {
            // Same Entry's existing native-close claim prevents duplicate OS
            // close. Unknown retains its original source and whole root debt.
            match self.entry.try_close() {
                Some(Ok(_)) => changed = true,
                Some(Err(source)) => {
                    self.first_fault
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get_or_insert_with(|| {
                            let mut failure =
                                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                    .with_source(OriginalSource(source));
                            failure.component = Some(self.binding.instance.component);
                            failure.delivery = DeliveryState::Unknown;
                            Arc::new(failure)
                        });
                }
                None => {}
            }
        }
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return changed;
        };
        let caller = self.binding.caller.clone();
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                Box::pin(async move {
                    caller.wait_registration_released().await;
                })
            });
        if waiting.as_mut().poll(cx).is_pending() {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
        }

        changed
    }
}
// Unique IO view return is independent of native close and original Scope ACK.
impl Drop for ControlledDatagramSocket {
    fn drop(&mut self) {
        self.entry.view_live.store(false, Ordering::Release);
    }
}
#[derive(Debug)]
struct OriginalSource(Arc<std_io::Error>);
impl fmt::Display for OriginalSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl crate::controlled_carrier::BoundCustody for BoundTotalsOriginal {
    fn released(&self) -> bool {
        BoundTotalsOriginal::released(self)
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        BoundTotalsOriginal::poll_retirement(self, cx)
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        BoundTotalsOriginal::failure(self)
    }
}

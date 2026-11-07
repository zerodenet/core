//! Synchronous wake deferral for an actual codec operation. Accounting and
//! unique workspace return happen immediately; no task or wake queue exists.
use super::*;
#[derive(Default)]
pub(super) struct Notifications(Mutex<NotificationState>);
#[derive(Default)]
struct NotificationState {
    guards: usize,
    pending: bool,
}
impl Notifications {
    fn hold(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.guards = state
            .guards
            .checked_add(1)
            .expect("finite synchronous memory operations");
    }
    pub(super) fn request(&self) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.guards == 0 {
            true
        } else {
            state.pending = true;
            false
        }
    }
    fn release(&self) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.guards = state
            .guards
            .checked_sub(1)
            .expect("matching actual operation guard");
        if state.guards == 0 && state.pending {
            state.pending = false;
            true
        } else {
            false
        }
    }
}
/// Declare before locking the concrete codec owner; Drop then runs after every
/// owner lock has unwound. This !Send guard must never cross an await. It does
/// not claim to prevent unrelated threads from waking other work later.
#[must_use]
pub struct PacketMemoryNotifications {
    original: NativeReadGrant,
    _synchronous: std::marker::PhantomData<*mut ()>,
}
/// Same Root bookkeeping for a synchronous Host return/cache operation. This
/// handle cannot allocate, change limits or authorize IO, and never crosses an
/// await. The concrete Host already owns this original Root.
#[must_use]
pub struct PacketRootNotifications {
    root: Arc<PacketReturnTotals>,
    _synchronous: std::marker::PhantomData<*mut ()>,
}
impl PacketReturnTotals {
    pub fn defer_notifications(self: &Arc<Self>) -> PacketRootNotifications {
        self.notifications.hold();
        PacketRootNotifications {
            root: self.clone(),
            _synchronous: std::marker::PhantomData,
        }
    }
}
impl Drop for PacketRootNotifications {
    fn drop(&mut self) {
        if self.root.notifications.release() {
            self.root.notify_now();
        }
    }
}
impl NativeReadGrant {
    pub fn defer_notifications(&self) -> PacketMemoryNotifications {
        self.0.root.notifications.hold();
        self.0.notifications.hold();
        PacketMemoryNotifications {
            original: self.clone(),
            _synchronous: std::marker::PhantomData,
        }
    }
}
impl PacketMemoryView {
    pub fn defer_notifications(&self) -> PacketMemoryNotifications {
        self.original.defer_notifications()
    }
}
impl Drop for PacketMemoryNotifications {
    fn drop(&mut self) {
        // Release both tiny bookkeeping locks before any external waker. The
        // original allocation/credit was already released by its true owner.
        let local = self.original.0.notifications.release();
        let root = self.original.0.root.notifications.release();
        if local {
            self.original.0.notify_now();
        }
        if root {
            self.original.0.root.notify_now();
        }
    }
}
#[cfg(test)]
mod tests;

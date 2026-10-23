//! Protocol cancellation never runs under the owner's State/Entry locks.
use super::*;
use std::sync::Mutex;
impl Entry {
    pub(in crate::component::datagram_retirement) fn cancel_pending(entry: &Arc<Mutex<Self>>) {
        {
            let mut state = entry.lock().expect("Datagram entry poisoned");
            if state.protocol_closed {
                state.pending_cancel = None;
                return;
            }
        }
        let control = entry
            .lock()
            .expect("Datagram entry poisoned")
            .control
            .clone();
        let Some(control) = control else { return };
        let Ok(actual) = control.try_lock() else {
            return;
        };
        let reason = entry
            .lock()
            .expect("Datagram entry poisoned")
            .pending_cancel
            .take();
        let Some(reason) = reason else { return };
        let mut delivery = CancelDelivery {
            entry: entry.clone(),
            reason: Some(reason),
        };
        if let Some(actual) = actual.as_ref() {
            actual.cancel(reason);
        }

        delivery.reason = None;
    }
}
// If an actual callback unwinds, preserve the original request and controller;
// propagate the same panic. Never infer cancellation or closure from unwind.
struct CancelDelivery {
    entry: Arc<Mutex<Entry>>,
    reason: Option<CloseReason>,
}
impl Drop for CancelDelivery {
    fn drop(&mut self) {
        if let Some(reason) = self.reason.take() {
            self.entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending_cancel
                .get_or_insert(reason);
        }
    }
}

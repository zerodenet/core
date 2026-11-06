//! Per-original native registration suspension; compiled only for real fixtures.
//! The barrier is not a native receipt, task witness, or alternate acquisition.
use super::*;
use std::sync::{Condvar, Weak};

pub struct RegistrationPause(Arc<Gate>);
pub(super) struct Gate {
    state: Mutex<State>,
    resumed: Condvar,
    arrived: tokio::sync::Notify,
}
struct State {
    entry: Option<Weak<Entry>>,
    released: bool,
}
impl ControlledDatagramOwner {
    /// Arm only this cold native owner. Its original is acquired/bound normally;
    /// pause immediately before registering that same socket with Tokio.
    pub fn pause_native_registration(&mut self) -> RegistrationPause {
        assert!(self.next == 0 && self.registration_pause.is_none());
        let gate = Arc::new(Gate {
            state: Mutex::new(State {
                entry: None,
                released: false,
            }),
            resumed: Condvar::new(),
            arrived: tokio::sync::Notify::new(),
        });
        self.registration_pause = Some(gate.clone());
        RegistrationPause(gate)
    }
}
impl Gate {
    pub(super) fn arrive(&self, entry: &Arc<Entry>) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.entry.is_none(), "one actual registration suspension");
            assert!(matches!(&*entry.lock(), NativeHandleState::Raw(_)));
            state.entry = Some(Arc::downgrade(entry));
        }
        self.arrived.notify_waiters();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = self
            .resumed
            .wait_while(state, |state| !state.released)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(state);
    }
}
impl RegistrationPause {
    pub async fn wait_acquired(&self) -> ControlledDatagramIdentity {
        loop {
            let changed = self.0.arrived.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let entry = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry
                .as_ref()
                .and_then(Weak::upgrade);
            if let Some(entry) = entry {
                assert!(matches!(&*entry.lock(), NativeHandleState::Raw(_)));
                return ControlledDatagramIdentity::from_entry(&entry);
            }
            changed.await;
        }
    }
    /// Exercise the same retained original's real retirement path. No control
    /// is manufactured and no synthetic close outcome is returned.
    pub fn try_close(&self) -> Option<Result<DatagramCloseReceipt, Arc<std_io::Error>>> {
        let entry = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("acquired original");
        entry.try_close()
    }
    pub fn is_raw_and_unclosed(&self) -> bool {
        let entry = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("acquired original");
        let raw = matches!(&*entry.lock(), NativeHandleState::Raw(_));
        raw && entry.closed_receipt.get().is_none()
    }
    pub fn closed(&self) -> Option<DatagramCloseReceipt> {
        let entry = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("acquired original");
        entry.closed_receipt.get().copied()
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("acquired original")
            .id
    }
    pub fn resume(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .released = true;
        self.0.resumed.notify_all();
    }
}
impl Drop for RegistrationPause {
    fn drop(&mut self) {
        // Unblock the actual producer during fixture unwind. Native ownership
        // remains with the original Entry; no closed/no-native fact is minted.
        self.resume();
    }
}

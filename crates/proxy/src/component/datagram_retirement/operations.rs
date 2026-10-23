//! State mutation and protocol callbacks have separate, explicit boundaries.
use super::*;
use zero_platform_tokio::contract::lifecycle::DynChannelControl;
impl RetiredDatagrams {
    pub(super) fn entries(&self) -> Vec<(u64, Arc<Mutex<Entry>>)> {
        self.state
            .lock()
            .expect("Datagram owner poisoned")
            .entries
            .iter()
            .map(|(id, entry)| (*id, entry.clone()))
            .collect()
    }
    pub(super) fn notify(&self) {
        self.changed.notify_waiters();
        self.wake.signal();
    }
    pub(super) fn retire_all(&self) {
        let entries = {
            let mut state = self.state.lock().expect("Datagram owner poisoned");
            state.stopping = true;
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        for entry in &entries {
            entry
                .lock()
                .expect("Datagram entry poisoned")
                .retire(CloseReason::Shutdown);
        }
        let _callbacks = self.wake.enter();
        self.notify();
        for entry in entries {
            Entry::cancel_pending(&entry);
        }
    }

    pub(super) fn retire(&self, id: u64, entry: &Arc<Mutex<Entry>>, reason: CloseReason) {
        let present = self
            .state
            .lock()
            .expect("Datagram owner poisoned")
            .entries
            .get(&id)
            .is_some_and(|actual| Arc::ptr_eq(actual, entry));
        if present {
            entry
                .lock()
                .expect("Datagram entry poisoned")
                .retire(reason);
        }
        let _callbacks = self.wake.enter();
        self.notify();
        if present {
            Entry::cancel_pending(entry);
        }
    }
    pub(super) fn abandon_reservation(&self, id: u64) {
        let removed = {
            let mut state = self.state.lock().expect("Datagram owner poisoned");
            if state
                .entries
                .get(&id)
                .is_some_and(|entry| entry.lock().expect("Datagram entry poisoned").reserved)
            {
                state.entries.remove(&id)
            } else {
                None
            }
        };
        drop(removed);
        self.notify();
    }
    pub(super) fn adopt(
        self: &Arc<Self>,
        id: u64,
        entry: &Arc<Mutex<Entry>>,
        control: Box<dyn DynChannelControl>,
    ) {
        let control = Arc::new(AsyncMutex::new(Some(control)));
        {
            let state = self.state.lock().expect("Datagram owner poisoned");
            let mut installed = entry.lock().expect("Datagram entry poisoned");
            assert!(
                state
                    .entries
                    .get(&id)
                    .is_some_and(|actual| Arc::ptr_eq(actual, entry))
                    && installed.reserved,
                "actual controller requires its reserved owner"
            );
            installed.reserved = false;
            installed.active = true;
            installed.control = Some(control);
            if state.stopping {
                installed.retire(CloseReason::Shutdown)
            }
        }
        let _callbacks = self.wake.enter();
        self.notify();
        Entry::cancel_pending(entry);
    }
}

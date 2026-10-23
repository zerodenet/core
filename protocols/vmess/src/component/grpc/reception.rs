//! Bounded local delivery of the actual aggregate exit, separate from I/O close.
use super::state::Record;
use std::sync::atomic::Ordering;
use zero_platform_tokio::contract::scope::TaskJoined;

impl Record {
    pub(super) fn receive(&self, joined: TaskJoined) -> TaskJoined {
        // Persist the exact scope receipt (and its original failure Arc) before
        // marking local acceptance. Host publication credit still needs its
        // original receipt ACK. This is one slot per carrier,
        // retained with that carrier's existing physical owner, never a history.
        let received = self.received.lock().unwrap().get_or_insert(joined).clone();
        if received.result().is_err() && !self.accepted.load(Ordering::Acquire) {
            let witness = self.witness.lock().unwrap().clone();
            if let Some(witness) = witness {
                if witness.acknowledge_hosted_failure(&received).is_ok() {
                    self.accepted.store(true, Ordering::Release);
                }
                // A rejected acknowledgement leaves the original scope credit
                // and this carrier's received failure intact for cleanup retry.
            }
        }
        received
    }
    pub(super) fn receive_available(&self) {
        let witness = self.witness.lock().unwrap().clone();
        if let Some(joined) = witness.and_then(|witness| witness.try_joined()) {
            self.receive(joined);
        }
    }
    pub(super) fn failure_accepted(&self) -> bool {
        let joined = self
            .witness
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|witness| witness.try_joined());
        joined.is_none_or(|joined| joined.result().is_ok() || self.accepted.load(Ordering::Acquire))
    }
}

//! Bounded local delivery of the actual aggregate exit, separate from I/O close.
use super::state::Record;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::Instant;
use zero_platform_tokio::contract::scope::{TaskJoinWitness, TaskJoined};

impl Record {
    pub(super) fn install_witness(&self, witness: TaskJoinWitness) {
        *self.witness.lock().unwrap() = Some(witness);
        self.exit_changed.notify_waiters();
    }
    /// Local delivery may unblock physical cleanup. It must run beside every
    /// original's cleanup, without waiting for another original or Host ACK.
    pub(super) async fn receive_exit(&self, deadline: Instant) {
        let waiting = async {
            loop {
                let changed = self.exit_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let witness = self.witness.lock().unwrap().clone();
                if let Some(witness) = witness {
                    self.receive(witness.wait().await);
                    return;
                }
                if !self.opening.load(Ordering::Acquire) {
                    // Opening publishes its witness before clearing this flag.
                    // Recheck after acquiring that flag to cover publication.
                    let witness = self.witness.lock().unwrap().clone();
                    if let Some(witness) = witness {
                        self.receive(witness.wait().await);
                    }
                    return;
                }
                changed.await;
            }
        };
        // Expiry only drops this observer. Record retains the actual witness,
        // received result and acceptance state for the next cleanup observation.
        let _ = tokio::time::timeout_at(deadline, waiting).await;
    }

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

/// One receiver per actual bounded original; none waits behind another's exit.
pub(super) async fn receive_all(records: Vec<Arc<Record>>, deadline: Instant) {
    let mut exits: FuturesUnordered<_> = records
        .into_iter()
        .map(|record| async move { record.receive_exit(deadline).await })
        .collect();
    while exits.next().await.is_some() {}
}

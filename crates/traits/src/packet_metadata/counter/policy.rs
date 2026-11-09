//! Same-counter provisional backing. Host owners bind source permits to real
//! target instances/Scopes; no IO, task, receiver or native-close authority.
use super::*;
use core::sync::atomic::AtomicU8;
mod source;
pub use source::{
    PacketMetadataCandidateCredit, PacketMetadataCandidateIssuer, PacketMetadataCandidateSource,
};
const PREPARING: u8 = 0;
const SEALED: u8 = 1;
const COMMITTED: u8 = 2;
const PUBLISHED: u8 = 3;
const ROLLED_BACK: u8 = 4;
pub struct PacketMetadataLimitCandidate {
    state: PacketMetadataArc<State>,
}
pub(super) struct State {
    counter: Arc<PacketMetadataCounter>,
    token: usize,
    target: PacketMetadataLayout,
    phase: AtomicU8,
    sources: AtomicUsize,
}
impl PacketMetadataCounter {
    pub fn candidate_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<State>::layout()
    }
    /// The actual Host prepays the concrete policy header before reserving a
    /// single candidate on this counter. Old callers retain their old ceiling;
    /// only issued target-source permits may use provisional growth.
    pub fn prepare_limit(
        self: &Arc<Self>,
        target: PacketMetadataLayout,
        header: PacketMetadataCredit,
    ) -> Result<PacketMetadataLimitCandidate, (PacketMemoryError, PacketMetadataCredit)> {
        let expected = match Self::candidate_layout() {
            Ok(expected) => expected,
            Err(error) => return Err((error, header)),
        };
        if header.layout().allocations < expected.allocations
            || header.layout().bytes < expected.bytes
        {
            return Err((PacketMemoryError::Capacity, header));
        }
        let prepared = (|| {
            let _gate = self.lock();
            if self.policy_pending.load(Ordering::Relaxed) != 0 {
                return Err(PacketMemoryError::Busy);
            }
            self.validate(target)?;
            let token = self
                .policy_serial
                .load(Ordering::Relaxed)
                .checked_add(1)
                .ok_or(PacketMemoryError::Overflow)?;
            self.policy_serial.store(token, Ordering::Relaxed);
            self.policy_rolled_back.store(false, Ordering::Relaxed);
            self.policy_allocations
                .store(target.allocations, Ordering::Relaxed);
            self.policy_bytes.store(target.bytes, Ordering::Relaxed);
            let previous = self.current().limit;
            self.policy_previous_allocations
                .store(previous.allocations, Ordering::Relaxed);
            self.policy_previous_bytes
                .store(previous.bytes, Ordering::Relaxed);
            self.policy_pending.store(token, Ordering::Relaxed);
            Ok(token)
        })();
        let token = match prepared {
            Ok(token) => token,
            Err(error) => return Err((error, header)),
        };
        // Actual allocation and all failed-value destruction are outside gate.
        let value = State {
            counter: self.clone(),
            token,
            target,
            phase: AtomicU8::new(PREPARING),
            sources: AtomicUsize::new(0),
        };
        PacketMetadataArc::from_prepaid(value, header)
            .map(|state| PacketMetadataLimitCandidate { state })
            .map_err(|(error, value, header)| {
                drop(value);
                (error, header)
            })
    }
}
impl PacketMetadataLimitCandidate {
    pub fn target(&self) -> PacketMetadataLayout {
        self.state.target
    }
    pub fn same_counter(&self, counter: &Arc<PacketMetadataCounter>) -> bool {
        Arc::ptr_eq(&self.state.counter, counter)
    }
    /// End the exact preparing phase before final metadata publication.
    /// Old and provisional allocations are included in the same occupied sum.
    /// Every reservation remains bounded by target, so sealing never cancels
    /// an accepted old query merely because final publication is in progress.
    pub fn seal(&self) -> Result<(), PacketMemoryError> {
        let state = &self.state;
        let _gate = state.counter.lock();
        if state.counter.policy_pending.load(Ordering::Relaxed) != state.token
            || !matches!(state.phase.load(Ordering::Relaxed), PREPARING | SEALED)
        {
            return Err(PacketMemoryError::Stopped);
        }
        state.counter.validate(state.target)?;
        state.phase.store(SEALED, Ordering::Release);
        Ok(())
    }
    pub fn validate_publication(&self) -> Result<(), PacketMemoryError> {
        let state = &self.state;
        let _gate = state.counter.lock();
        if state.counter.policy_pending.load(Ordering::Relaxed) != state.token
            || state.phase.load(Ordering::Relaxed) != SEALED
        {
            return Err(PacketMemoryError::Paused);
        }
        state.counter.validate(state.target)
    }
    /// Caller first validates all actual books under its original publication
    /// guards. Every reserve is already target-bounded, so this scalar commit
    /// is infallible even while accepted original queries finish their bodies.
    pub fn commit_validated_deferred(&self) {
        let state = &self.state;
        let _gate = state.counter.lock();
        assert_eq!(
            state.counter.policy_pending.load(Ordering::Relaxed),
            state.token
        );
        assert_eq!(state.phase.load(Ordering::Relaxed), SEALED);
        state
            .counter
            .max_allocations
            .store(state.target.allocations, Ordering::Relaxed);
        state
            .counter
            .max_bytes
            .store(state.target.bytes, Ordering::Relaxed);
        state.phase.store(COMMITTED, Ordering::Release);
    }
    /// Restore only allocation admission. Outstanding backing stays charged;
    /// this acknowledges no native original, task join or Source registration.
    pub fn rollback(&self) {
        let state = &self.state;
        let _gate = state.counter.lock();
        if matches!(state.phase.load(Ordering::Relaxed), COMMITTED | PUBLISHED) {
            return;
        }
        state.phase.store(ROLLED_BACK, Ordering::Release);
        if state.counter.policy_pending.load(Ordering::Relaxed) == state.token {
            state
                .counter
                .policy_rolled_back
                .store(true, Ordering::Relaxed);
            if state.sources.load(Ordering::Acquire) == 0 {
                state.clear_pending();
            }
        }
    }
    pub fn metadata_committed(&self) -> bool {
        matches!(
            self.state.phase.load(Ordering::Acquire),
            COMMITTED | PUBLISHED
        )
    }
    /// Only after the real Host/catalog/Engine publication guards are gone.
    pub fn publish(&self) {
        let state = &self.state;
        let _gate = state.counter.lock();
        if state.phase.load(Ordering::Relaxed) == PUBLISHED {
            return;
        }
        assert_eq!(state.phase.load(Ordering::Relaxed), COMMITTED);
        state.phase.store(PUBLISHED, Ordering::Release);
        state.clear_pending();
    }
}
impl State {
    // Called inside this same counter's scalar gate. Publication and rollback
    // cannot split phase selection from the actual allocation reservation.
    pub(super) fn reservation(&self) -> Result<Option<usize>, PacketMemoryError> {
        match self.phase.load(Ordering::Acquire) {
            PREPARING | SEALED | COMMITTED => Ok(Some(self.token)),
            PUBLISHED => Ok(None),
            _ => Err(PacketMemoryError::Paused),
        }
    }
    /// Called only with this counter's scalar gate held. No allocation, wake,
    /// deallocation, source/Scope action or external callback occurs here.
    fn clear_pending(&self) {
        if self.counter.policy_pending.load(Ordering::Relaxed) == self.token {
            self.counter.policy_pending.store(0, Ordering::Relaxed);
        }
    }
    fn returned_source(&self) {
        let last = self.sources.fetch_sub(1, Ordering::AcqRel) == 1;
        if last && self.phase.load(Ordering::Acquire) == ROLLED_BACK {
            let _gate = self.counter.lock();
            self.clear_pending();
        }
    }
}
impl Drop for PacketMetadataLimitCandidate {
    fn drop(&mut self) {
        if self.metadata_committed() {
            self.publish();
        } else {
            self.rollback();
        }
    }
}
impl Drop for State {
    fn drop(&mut self) {
        let _gate = self.counter.lock();
        self.clear_pending();
    }
}

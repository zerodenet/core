use super::*;
#[derive(Clone)]
pub struct PacketMetadataCandidateSource {
    source: PacketMetadataArc<Source>,
}
#[derive(Clone)]
pub struct PacketMetadataCandidateIssuer {
    state: PacketMetadataArc<State>,
}
struct Source {
    state: PacketMetadataArc<State>,
    active: AtomicBool,
}
/// Pure backing credit only. No source/controller/Scope is retained by it.
pub struct PacketMetadataCandidateCredit {
    credit: PacketMetadataCredit,
    // Keeps the already-paid policy header while provisional backing lives.
    _state: Option<PacketMetadataArc<State>>,
}
impl PacketMetadataLimitCandidate {
    pub fn source_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<Source>::layout()
    }
    pub fn issuer(&self) -> PacketMetadataCandidateIssuer {
        PacketMetadataCandidateIssuer {
            state: self.state.clone(),
        }
    }
}
impl PacketMetadataCandidateIssuer {
    pub fn is_published(&self) -> bool {
        self.state.phase.load(Ordering::Acquire) == PUBLISHED
    }
    pub fn same_counter(&self, counter: &Arc<PacketMetadataCounter>) -> bool {
        Arc::ptr_eq(&self.state.counter, counter)
    }
    pub fn same_budget(&self, budget: &PacketMetadataBudget) -> bool {
        Arc::ptr_eq(&self.state.counter, &budget.counter)
    }
    pub fn same_candidate(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.state, &other.state)
    }
    pub fn source_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLimitCandidate::source_layout()
    }
    /// The Host uses this only at the actual target Source's Scope binding.
    /// The pure value itself supplies no instance, native or task authority.
    pub fn prepare_source(
        &self,
        header: PacketMetadataCredit,
    ) -> Result<PacketMetadataCandidateSource, (PacketMemoryError, PacketMetadataCredit)> {
        let expected = match Self::source_layout() {
            Ok(expected) => expected,
            Err(error) => return Err((error, header)),
        };
        if header.layout().allocations < expected.allocations
            || header.layout().bytes < expected.bytes
        {
            return Err((PacketMemoryError::Capacity, header));
        }
        let state = &self.state;
        let registered = (|| {
            let _gate = state.counter.lock();
            if state.counter.policy_pending.load(Ordering::Relaxed) != state.token
                || state.phase.load(Ordering::Relaxed) != PREPARING
            {
                return Err(PacketMemoryError::Stopped);
            }
            let mut previous = state.sources.load(Ordering::Acquire);
            loop {
                let count = previous.checked_add(1).ok_or(PacketMemoryError::Overflow)?;
                match state.sources.compare_exchange_weak(
                    previous,
                    count,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => previous = current,
                }
            }
            Ok(())
        })();
        if let Err(error) = registered {
            return Err((error, header));
        }
        let value = Source {
            state: state.clone(),
            active: AtomicBool::new(true),
        };
        PacketMetadataArc::from_prepaid(value, header)
            .map(|source| PacketMetadataCandidateSource { source })
            .map_err(|(error, value, header)| {
                drop(value);
                (error, header)
            })
    }
}
impl PacketMetadataCandidateSource {
    /// Pure identity of the actual Root's counter; no IO/Scope authority.
    pub fn same_counter(&self, counter: &Arc<PacketMetadataCounter>) -> bool {
        Arc::ptr_eq(&self.source.state.counter, counter)
    }
    pub fn same_budget(&self, budget: &PacketMetadataBudget) -> bool {
        Arc::ptr_eq(&self.source.state.counter, &budget.counter)
    }
    /// Return memory authority, not native close, task join or Scope ACK.
    /// Existing original backing keeps its credit, even after rollback.
    pub fn revoke(&self) {
        if self.source.active.swap(false, Ordering::AcqRel) {
            self.source.state.returned_source();
        }
    }
    pub fn reserve(
        &self,
        layout: PacketMetadataLayout,
        wake: Waker,
    ) -> Result<PacketMetadataCandidateCredit, PacketMemoryError> {
        if !self.source.active.load(Ordering::Acquire) {
            return Err(PacketMemoryError::Stopped);
        }
        let state = &self.source.state;
        let phase = state.phase.load(Ordering::Acquire);
        let (candidate, retained) = match phase {
            PREPARING | SEALED | COMMITTED => (Some(&**state), Some(state.clone())),
            PUBLISHED => (None, None),
            _ => return Err(PacketMemoryError::Paused),
        };
        let credit = state.counter.reserve_for(layout, wake, candidate)?;
        // Revoke may race this reservation. A real returned backing is still
        // held and charged, never cleared by its source's signal alone.
        if !self.source.active.load(Ordering::Acquire) {
            drop(credit);
            drop(retained);
            return Err(PacketMemoryError::Stopped);
        }
        Ok(PacketMetadataCandidateCredit {
            credit,
            _state: retained,
        })
    }
}
impl PacketMetadataCandidateCredit {
    pub fn layout(&self) -> PacketMetadataLayout {
        self.credit.layout()
    }
    pub fn split(&mut self, layout: PacketMetadataLayout) -> Result<Self, PacketMemoryError> {
        let credit = self.credit.split(layout)?;
        Ok(Self {
            credit,
            _state: self._state.clone(),
        })
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.state.returned_source();
        }
    }
}

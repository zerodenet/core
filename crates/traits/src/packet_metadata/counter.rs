//! One exact metadata lane, also owned by the actual runtime's packet root.
//! The tiny gate protects only scalar loads/stores; never a user wake/drop.
use super::*;
pub struct PacketMetadataCounter {
    gate: AtomicBool,
    allocations: AtomicUsize,
    bytes: AtomicUsize,
    max_allocations: AtomicUsize,
    max_bytes: AtomicUsize,
}
struct Gate<'a>(&'a AtomicBool);
impl Drop for Gate<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl PacketMetadataCounter {
    pub fn owned_layout() -> PacketMetadataLayout {
        PacketMetadataLayout::arc::<Self>().expect("fixed concrete metadata counter layout")
    }
    /// The lane's one concrete shared counter allocation is itself included.
    /// No pointer/handle size substitutes for its actual allocation layout.
    pub fn new_owned(limit: PacketMetadataLayout) -> Result<Arc<Self>, PacketMemoryError> {
        let owned = Self::owned_layout();
        if limit.allocations < owned.allocations || limit.bytes < owned.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        // These initial units cover Arc::new below, before it allocates. They
        // persist through every credit and the actual counter's last Arc.
        Ok(Arc::new(Self {
            gate: AtomicBool::new(false),
            allocations: AtomicUsize::new(owned.allocations),
            bytes: AtomicUsize::new(owned.bytes),
            max_allocations: AtomicUsize::new(limit.allocations),
            max_bytes: AtomicUsize::new(limit.bytes),
        }))
    }
    fn lock(&self) -> Gate<'_> {
        while self
            .gate
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        Gate(&self.gate)
    }
    fn current(&self) -> PacketMetadataSnapshot {
        PacketMetadataSnapshot {
            limit: PacketMetadataLayout::new(
                self.max_allocations.load(Ordering::Relaxed),
                self.max_bytes.load(Ordering::Relaxed),
            ),
            occupied: PacketMetadataLayout::new(
                self.allocations.load(Ordering::Relaxed),
                self.bytes.load(Ordering::Relaxed),
            ),
        }
    }
    pub fn snapshot(&self) -> PacketMetadataSnapshot {
        let _gate = self.lock();
        self.current()
    }
    fn validate(&self, limit: PacketMetadataLayout) -> Result<(), PacketMemoryError> {
        let own = Self::owned_layout();
        if limit.allocations < own.allocations || limit.bytes < own.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        let current = self.current().occupied;
        if current.allocations > limit.allocations || current.bytes > limit.bytes {
            return Err(PacketMemoryError::Busy);
        }
        Ok(())
    }
    /// Host-only metadata policy. The enclosing Root's existing mutex/fences
    /// serialize this with its other lanes; a consumer only receives a credit.
    pub fn check_limit(&self, limit: PacketMetadataLayout) -> Result<(), PacketMemoryError> {
        let _gate = self.lock();
        self.validate(limit)
    }
    pub fn install_limit(&self, limit: PacketMetadataLayout) -> Result<(), PacketMemoryError> {
        let _gate = self.lock();
        self.validate(limit)?;
        self.max_allocations
            .store(limit.allocations, Ordering::Relaxed);
        self.max_bytes.store(limit.bytes, Ordering::Relaxed);
        Ok(())
    }
    /// Receives one already constructed standard waker, not an allocator or
    /// deallocator callback. No clone/drop/wake of it occurs under this gate.
    pub fn reserve(
        self: &Arc<Self>,
        layout: PacketMetadataLayout,
        wake: Waker,
    ) -> Result<PacketMetadataCredit, PacketMemoryError> {
        let gate = self.lock();
        let current = self.current();
        if layout.allocations > current.limit.allocations || layout.bytes > current.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if layout.allocations
            > current
                .limit
                .allocations
                .saturating_sub(current.occupied.allocations)
            || layout.bytes > current.limit.bytes.saturating_sub(current.occupied.bytes)
        {
            return Err(PacketMemoryError::Busy);
        }
        self.allocations.store(
            current.occupied.allocations + layout.allocations,
            Ordering::Relaxed,
        );
        self.bytes
            .store(current.occupied.bytes + layout.bytes, Ordering::Relaxed);
        let counter = self.clone();
        drop(gate);
        Ok(PacketMetadataCredit {
            counter,
            layout,
            wake,
        })
    }
    fn release(&self, layout: PacketMetadataLayout) {
        let _gate = self.lock();
        self.allocations.store(
            self.allocations
                .load(Ordering::Relaxed)
                .checked_sub(layout.allocations)
                .expect("actual metadata allocation credit"),
            Ordering::Relaxed,
        );
        self.bytes.store(
            self.bytes
                .load(Ordering::Relaxed)
                .checked_sub(layout.bytes)
                .expect("actual metadata byte credit"),
            Ordering::Relaxed,
        );
    }
}
/// The owning object must drop this AFTER freeing its paid allocation. It
/// carries only same-counter custody and notification, never acquisition/IO.
pub struct PacketMetadataCredit {
    counter: Arc<PacketMetadataCounter>,
    layout: PacketMetadataLayout,
    wake: Waker,
}
impl PacketMetadataCredit {
    /// Only identity of the actual paid counter; not IO or policy authority.
    pub fn same_counter(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.counter, &other.counter)
    }
    pub fn layout(&self) -> PacketMetadataLayout {
        self.layout
    }
    /// Move custody of existing same-counter units. The consumed handle is
    /// returned, with zero units on success, so its original Waker and Arc are
    /// destroyed by the caller outside any owner/promise lock.
    pub fn absorb(&mut self, mut other: Self) -> Result<Self, (PacketMemoryError, Self)> {
        if !self.same_counter(&other) {
            return Err((PacketMemoryError::Stopped, other));
        }
        let combined = match self.layout.checked_add(other.layout) {
            Ok(combined) => combined,
            Err(error) => return Err((error, other)),
        };
        self.layout = combined;
        other.layout = PacketMetadataLayout::new(0, 0);
        Ok(other)
    }
    /// Split custody with a Waker already prepared outside the owner's lock.
    /// No Waker is cloned, dropped or woken here, including rejection.
    pub fn split_prepared(
        &mut self,
        layout: PacketMetadataLayout,
        wake: Waker,
    ) -> Result<Self, (PacketMemoryError, Waker)> {
        if layout.allocations > self.layout.allocations || layout.bytes > self.layout.bytes {
            return Err((PacketMemoryError::Capacity, wake));
        }
        self.layout.allocations -= layout.allocations;
        self.layout.bytes -= layout.bytes;
        Ok(Self {
            counter: self.counter.clone(),
            layout,
            wake,
        })
    }
    /// Splits an already paid promise. It neither allocates nor refunds units.
    pub fn split(&mut self, layout: PacketMetadataLayout) -> Result<Self, PacketMemoryError> {
        if layout.allocations > self.layout.allocations || layout.bytes > self.layout.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        // Waker clone can execute external code, so never put this under gate.
        let wake = self.wake.clone();
        self.layout.allocations -= layout.allocations;
        self.layout.bytes -= layout.bytes;
        Ok(Self {
            counter: self.counter.clone(),
            layout,
            wake,
        })
    }
}
impl Drop for PacketMetadataCredit {
    fn drop(&mut self) {
        if self.layout.allocations == 0 && self.layout.bytes == 0 {
            return;
        }
        self.counter.release(self.layout); // both scalar units updated; gate released
        self.wake.wake_by_ref(); // can reenter snapshot/reserve with no held lock
    }
}

//! Concrete storage-header promises over the SAME paid counter. Scalar lease
//! counts prevent header reuse before PacketBuffer's actual Storage Box free.
//! No allocator hook, Root/policy setter, IO, task or closing authority exists.
use super::*;
use core::cell::UnsafeCell;
struct State {
    active: usize,
    capacity: usize,
    minimum: usize,
    retiring: usize,
    growing: usize,
}
struct Header {
    gate: AtomicBool,
    state: UnsafeCell<State>,
    layout: PacketMetadataLayout,
    changed: Waker,
    refunded: Waker,
}
struct Credits {
    gate: AtomicBool,
    value: UnsafeCell<PacketMetadataCredit>,
}
// Each cell is accessed only while its own RAII scalar/custody gate is held.
unsafe impl Sync for Header {}
unsafe impl Sync for Credits {}
struct Guard<'a>(&'a AtomicBool);
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
fn lock(gate: &AtomicBool) -> Guard<'_> {
    while gate
        .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        core::hint::spin_loop();
    }
    Guard(gate)
}
pub struct PacketMetadataHeaderPromise {
    original: Option<Arc<Header>>,
    credit: Option<Arc<Credits>>,
}
pub struct PacketMetadataHeaderLease {
    original: Option<Arc<Header>>,
    credit: Option<Arc<Credits>>,
    count: usize,
}
#[derive(Clone)]
pub struct PacketMetadataHeaderObservation {
    original: Option<Arc<Header>>,
    credit: Option<Arc<Credits>>,
}
fn multiplied(
    layout: PacketMetadataLayout,
    count: usize,
) -> Result<PacketMetadataLayout, PacketMemoryError> {
    Ok(PacketMetadataLayout::new(
        layout
            .allocations()
            .checked_mul(count)
            .ok_or(PacketMemoryError::Overflow)?,
        layout
            .bytes()
            .checked_mul(count)
            .ok_or(PacketMemoryError::Overflow)?,
    ))
}
impl PacketMetadataHeaderPromise {
    pub fn layout(
        header: PacketMetadataLayout,
        count: usize,
    ) -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLayout::arc::<Header>()?
            .checked_add(PacketMetadataLayout::arc::<Credits>()?)?
            .checked_add(multiplied(header, count)?)
    }
    pub fn from_prepaid(
        header: PacketMetadataLayout,
        count: usize,
        minimum: usize,
        credit: PacketMetadataCredit,
        changed: Waker,
        refunded: Waker,
    ) -> Result<Self, (PacketMemoryError, PacketMetadataCredit)> {
        let expected = match Self::layout(header, count) {
            Ok(layout) => layout,
            Err(source) => return Err((source, credit)),
        };
        if minimum > count
            || credit.layout().allocations() < expected.allocations()
            || credit.layout().bytes() < expected.bytes()
        {
            return Err((PacketMemoryError::Capacity, credit));
        }
        Ok(Self {
            original: Some(Arc::new(Header {
                gate: AtomicBool::new(false),
                state: UnsafeCell::new(State {
                    active: 0,
                    capacity: count,
                    minimum,
                    retiring: 0,
                    growing: 0,
                }),
                layout: header,
                changed,
                refunded,
            })),
            credit: Some(Arc::new(Credits {
                gate: AtomicBool::new(false),
                value: UnsafeCell::new(credit),
            })),
        })
    }
    pub fn capacity(&self) -> usize {
        let header = self.original.as_ref().unwrap();
        let _guard = lock(&header.gate);
        unsafe { &*header.state.get() }.capacity
    }
    pub fn is_available(&self) -> bool {
        let header = self.original.as_ref().unwrap();
        let _guard = lock(&header.gate);
        let state = unsafe { &*header.state.get() };
        state.active < state.capacity
    }
    pub fn try_lease(&self, count: usize) -> Result<PacketMetadataHeaderLease, PacketMemoryError> {
        let header = self.original.as_ref().unwrap();
        let _guard = lock(&header.gate);
        let state = unsafe { &mut *header.state.get() };
        if count > state.capacity.saturating_sub(state.active) {
            return Err(PacketMemoryError::Busy);
        }
        state.active += count;
        Ok(PacketMetadataHeaderLease {
            original: self.original.clone(),
            credit: self.credit.clone(),
            count,
        })
    }
    /// Success returns the same emptied original credit. Its Waker/Arc must
    /// be dropped by the caller AFTER releasing slot/envelope/owner locks.
    pub fn grow(
        &self,
        count: usize,
        credit: PacketMetadataCredit,
    ) -> Result<PacketMetadataCredit, (PacketMemoryError, PacketMetadataCredit)> {
        let header = self.original.as_ref().unwrap();
        let expected = match multiplied(header.layout, count) {
            Ok(layout) => layout,
            Err(cause) => return Err((cause, credit)),
        };
        if credit.layout() != expected {
            return Err((PacketMemoryError::Capacity, credit));
        }
        {
            let _guard = lock(&header.gate);
            let state = unsafe { &mut *header.state.get() };
            if state
                .capacity
                .checked_add(state.growing)
                .and_then(|v| v.checked_add(count))
                .is_none()
            {
                return Err((PacketMemoryError::Overflow, credit));
            }
            state.growing += count;
        }
        let result = {
            let credits = self.credit.as_ref().unwrap();
            let _guard = lock(&credits.gate);
            unsafe { &mut *credits.value.get() }.absorb(credit)
        };
        {
            let _guard = lock(&header.gate);
            let state = unsafe { &mut *header.state.get() };
            state.growing -= count;
            if result.is_ok() {
                state.capacity += count;
            }
        }
        result
    }
}
impl PacketMetadataHeaderLease {
    pub fn layout(&self) -> PacketMetadataLayout {
        multiplied(self.original.as_ref().unwrap().layout, self.count)
            .expect("same prepaid header lease count")
    }
    pub fn observe(&self) -> PacketMetadataHeaderObservation {
        PacketMetadataHeaderObservation {
            original: self.original.clone(),
            credit: self.credit.clone(),
        }
    }
    pub fn split_one(&mut self) -> Result<Self, PacketMemoryError> {
        if self.count == 0 {
            return Err(PacketMemoryError::Capacity);
        }
        self.count -= 1;
        Ok(Self {
            original: self.original.clone(),
            credit: self.credit.clone(),
            count: 1,
        })
    }
}
impl PacketMetadataHeaderObservation {
    /// Only marks already-grown header units as retiring; it never returns a
    /// byte or claims a Box/native/Scope closed. Actual final lease Drop does.
    pub fn retire_grown(&self, count: usize) {
        let header = self.original.as_ref().unwrap();
        let _guard = lock(&header.gate);
        let state = unsafe { &mut *header.state.get() };
        state.retiring = state
            .retiring
            .checked_add(count)
            .expect("finite prepaid header responsibility");
    }
}
impl Drop for PacketMetadataHeaderLease {
    fn drop(&mut self) {
        if self.count != 0 {
            let header = self.original.as_ref().unwrap();
            // Preparing notification is outside either promise lock.
            let refund_wake = header.refunded.clone();
            let refund = {
                let _guard = lock(&header.gate);
                let state = unsafe { &mut *header.state.get() };
                state.active = state
                    .active
                    .checked_sub(self.count)
                    .expect("actual header lease");
                let refund = state.retiring.min(
                    state
                        .capacity
                        .saturating_sub(state.minimum)
                        .min(state.capacity.saturating_sub(state.active)),
                );
                state.capacity -= refund;
                state.retiring -= refund;
                refund
            };
            let released = if refund != 0 {
                let credits = self.credit.as_ref().unwrap();
                let _guard = lock(&credits.gate);
                Some(unsafe { &mut *credits.value.get() }.split_prepared(
                    multiplied(header.layout, refund).expect("same prepaid header units"),
                    refund_wake,
                ))
            } else {
                drop(refund_wake);
                None
            };
            match released {
                Some(Ok(credit)) => drop(credit),
                Some(Err((_cause, wake))) => {
                    drop(wake);
                    panic!("same actual prepaid header refund");
                }
                None => {}
            }
            header.changed.wake_by_ref();
        }
        release(self.original.take(), self.credit.take());
    }
}
fn release(allocation: Option<Arc<Header>>, credit: Option<Arc<Credits>>) {
    struct CreditRelease(Option<Arc<Credits>>);
    impl Drop for CreditRelease {
        fn drop(&mut self) {
            if let Some(credit) = self.0.take() {
                if let Some(credit) = Arc::into_inner(credit) {
                    drop(credit.value.into_inner());
                }
            }
        }
    }
    let credit = CreditRelease(credit);
    drop(allocation); // data/header first, including changed Waker's real owner
    drop(credit); // credit-cell header itself frees before global refund
}
impl Drop for PacketMetadataHeaderPromise {
    fn drop(&mut self) {
        release(self.original.take(), self.credit.take());
    }
}
impl Drop for PacketMetadataHeaderObservation {
    fn drop(&mut self) {
        release(self.original.take(), self.credit.take());
    }
}

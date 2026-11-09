//! Exact read-slot/header layouts and its one concrete weak memory wake.
//! The RawWaker owns no Source/Scope/Host; it only notifies the same read slot.
use super::*;
use std::task::{RawWaker, RawWakerVTable, Waker};
use zero_traits::packet_metadata::{PacketMetadataHeaderPromise, PacketMetadataWeak};
struct Wake {
    slot: PacketMetadataWeak<ReadSlot>,
    _credit: PacketMetadataCredit,
}
impl NativePacketCredit {
    pub(in crate::packet_memory) fn header_layout() -> PacketMetadataLayout {
        let read = PacketMetadataLayout::boxed::<ReadPacket>();
        let decoded = Self::decoded_header_layout();
        PacketMetadataLayout::new(
            read.allocations().max(decoded.allocations()),
            read.bytes().max(decoded.bytes()),
        )
    }
}
pub(super) fn promise_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
    PacketMetadataHeaderPromise::layout(NativePacketCredit::header_layout(), 1)?
        .checked_add(PacketMetadataLayout::arc::<Wake>()?)
}
pub(in crate::packet_memory) fn slot_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
    PacketMetadataArc::<ReadSlot>::layout()?.checked_add(promise_layout()?)
}
pub(super) fn promise(
    slot: &PacketMetadataArc<ReadSlot>,
    mut credit: PacketMetadataCredit,
) -> Result<PacketMetadataHeaderPromise, PacketMemoryError> {
    let wake_credit = credit.split(PacketMetadataLayout::arc::<Wake>()?)?;
    let pointer = Arc::into_raw(Arc::new(Wake {
        slot: PacketMetadataArc::downgrade(slot),
        _credit: wake_credit,
    }));
    // Only these four private operations reconstruct this exact Arc. No Weak
    // to Wake escapes, so into_inner really frees its header before credit.
    let wake = unsafe { Waker::from_raw(RawWaker::new(pointer.cast(), &VTABLE)) };
    PacketMetadataHeaderPromise::from_prepaid(
        NativePacketCredit::header_layout(),
        1,
        1,
        credit,
        wake,
        Waker::from(slot.root.clone()),
    )
    .map_err(|(cause, _)| cause)
}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_ref, release);
unsafe fn clone(pointer: *const ()) -> RawWaker {
    unsafe { Arc::increment_strong_count(pointer.cast::<Wake>()) };
    RawWaker::new(pointer, &VTABLE)
}
fn notify(wake: &Wake) {
    if let Some(slot) = wake.slot.upgrade() {
        let state = slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = !state.revoked;
        drop(state); // pairs with the original blocking Condvar predicate
        if current {
            slot.notify();
        }
    }
}
unsafe fn wake(pointer: *const ()) {
    let arc = unsafe { Arc::from_raw(pointer.cast::<Wake>()) };
    notify(&arc);
    drop(Arc::into_inner(arc));
}
unsafe fn wake_ref(pointer: *const ()) {
    let arc = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(pointer.cast::<Wake>()) });
    notify(&arc);
}
unsafe fn release(pointer: *const ()) {
    drop(Arc::into_inner(unsafe {
        Arc::from_raw(pointer.cast::<Wake>())
    }));
}

#[cfg(test)]
mod tests;

//! Half of the original 64KiB RPC response storage, owned until its result ends.
use crate::packet_memory::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
use std::{
    cell::UnsafeCell,
    io,
    ops::Deref,
    sync::atomic::{AtomicBool, Ordering},
};
// Protocol working storage, not an exempt quota: both actual Arc layouts are
// charged before IO on the same configured Root. Large output grows separately.
const BYTES: usize = 32768;
pub(in crate::macos_privilege) struct Storage {
    bytes: UnsafeCell<[u8; BYTES]>,
    borrowed: AtomicBool,
}
// Only a successful single claim can mutate. A claim is never Clone, and its
// returned immutable byte owner keeps the claim until Drop. No raw Arc/Weak or
// mutable slice escapes this module, so no writer can coexist with readers.
unsafe impl Sync for Storage {}
pub(in crate::macos_privilege) struct Bytes {
    original: PacketMetadataArc<Storage>,
    length: usize,
}
impl Storage {
    pub(in crate::macos_privilege) fn layout(
    ) -> Result<PacketMetadataLayout, crate::packet_memory::PacketMemoryError> {
        PacketMetadataArc::<Self>::layout()
    }
    pub(in crate::macos_privilege) fn from_prepaid(
        credit: PacketMetadataCredit,
    ) -> io::Result<PacketMetadataArc<Self>> {
        PacketMetadataArc::from_prepaid(
            Self {
                bytes: UnsafeCell::new([0; BYTES]),
                borrowed: AtomicBool::new(false),
            },
            credit,
        )
        .map_err(|(source, _, _)| io::Error::other(source))
    }
    pub(in crate::macos_privilege) fn claim(
        original: &PacketMetadataArc<Self>,
    ) -> io::Result<Bytes> {
        original
            .borrowed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        Ok(Bytes {
            original: original.clone(),
            length: 0,
        })
    }
}
impl Bytes {
    pub(in crate::macos_privilege) fn capacity(&self) -> usize {
        BYTES
    }
    pub(in crate::macos_privilege) fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let end = self
            .length
            .checked_add(bytes.len())
            .filter(|end| *end <= BYTES)
            .ok_or_else(|| {
                io::Error::other("command bytes exceed their original fixed response storage")
            })?;
        // SAFETY: this non-clone claim is the sole possible writer, and its
        // borrowed flag prevents every other claim until this owner drops.
        unsafe {
            (&mut *self.original.bytes.get())[self.length..end].copy_from_slice(bytes);
        }
        self.length = end;
        Ok(())
    }
}
impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // No mutable access is available to a result consumer. The sole owner
        // can call append only with &mut self, excluding this borrowed slice.
        unsafe { &(&*self.original.bytes.get())[..self.length] }
    }
}
impl Drop for Bytes {
    fn drop(&mut self) {
        self.original.borrowed.store(false, Ordering::Release);
    }
}

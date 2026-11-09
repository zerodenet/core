//! Growth pays the exact replacement and retains the old allocation to free.
use crate::packet_memory::{PacketMemoryView, PacketMetadataVec};
use std::{fmt, io, ops::Deref};
pub(crate) struct CommandBytes(Storage);
enum Storage {
    Library(Vec<u8>),
    Owned(PacketMetadataVec<u8>),
    Inline(super::inline::Bytes),
}
impl CommandBytes {
    pub(in crate::macos_privilege) fn inline(bytes: super::inline::Bytes) -> Self {
        Self(Storage::Inline(bytes))
    }
    pub(in crate::macos_privilege) fn library(bytes: Vec<u8>) -> Self {
        Self(Storage::Library(bytes))
    }
    pub(in crate::macos_privilege) fn reserve_owned(
        &mut self,
        memory: &PacketMemoryView,
        max: usize,
    ) -> Result<(), crate::packet_memory::PacketMemoryError> {
        if self.len() >= max {
            return Err(crate::packet_memory::PacketMemoryError::Capacity);
        }
        match &mut self.0 {
            Storage::Inline(bytes) if bytes.len() < bytes.capacity() => Ok(()),
            Storage::Inline(bytes) => {
                let mut next = PacketMetadataVec::default();
                // Keep the original fully paid backing through the ENTIRE
                // copy and every actual old/new Vec growth peak. Failure
                // leaves it untouched, so the original continuation remains.
                for &byte in bytes.iter() {
                    next.try_push(byte, memory, max)
                        .map_err(|(source, _)| source)?;
                }
                next.try_reserve_one(memory, max)?;
                self.0 = Storage::Owned(next);
                Ok(())
            }
            Storage::Owned(target) => target.try_reserve_one(memory, max),
            Storage::Library(_) => Err(crate::packet_memory::PacketMemoryError::Stopped),
        }
    }
    pub(in crate::macos_privilege) fn append_reserved(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Storage::Inline(target) = &mut self.0 {
            return target.append(bytes);
        }
        let Storage::Owned(target) = &mut self.0 else {
            return Err(io::Error::other("Kernel command has no owned receiver"));
        };
        if bytes.len() > target.capacity().saturating_sub(target.len()) {
            return Err(io::Error::other(
                "command bytes exceed their actual prepaid capacity",
            ));
        }
        for &byte in bytes {
            target
                .push_reserved(byte)
                .map_err(|_| io::Error::other("missing original command capacity"))?;
        }
        Ok(())
    }
    pub(in crate::macos_privilege) fn capacity(&self) -> usize {
        match &self.0 {
            Storage::Library(bytes) => bytes.capacity(),
            Storage::Owned(bytes) => bytes.capacity(),
            Storage::Inline(bytes) => bytes.capacity(),
        }
    }
}
impl Deref for CommandBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match &self.0 {
            Storage::Library(bytes) => bytes,
            Storage::Owned(bytes) => bytes,
            Storage::Inline(bytes) => bytes,
        }
    }
}
impl fmt::Debug for CommandBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandBytes")
            .field("length", &self.len())
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

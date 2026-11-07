//! The exact utun read/write allocations are prepaid before FD acquisition.
use crate::packet_memory::{NativeReadBuffer, NativeReadGrant, PacketMemoryNotifications};
use std::{
    io,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
pub(crate) const WORKSPACE_BYTES: usize = 65_540;
pub(super) enum Buffer {
    Library(Vec<u8>),
    Owned(NativeReadBuffer),
}
impl Buffer {
    pub(super) fn library() -> Self {
        Self::Library(vec![0; WORKSPACE_BYTES])
    }
}
impl Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Library(buffer) => buffer,
            Self::Owned(buffer) => buffer.as_bytes(),
        }
    }
}
impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Library(buffer) => buffer,
            Self::Owned(buffer) => buffer.as_mut_bytes(),
        }
    }
}
pub(crate) struct Buffers {
    pub(super) read: Buffer,
    pub(super) write: Buffer,
}
impl Buffers {
    pub(super) fn library() -> Self {
        Self {
            read: Buffer::library(),
            write: Buffer::library(),
        }
    }
}
pub(crate) struct Original {
    read: NativeReadGrant,
    write: NativeReadGrant,
    buffers: Mutex<Option<Buffers>>,
}
impl Original {
    pub(crate) fn prepare(read: NativeReadGrant, write: NativeReadGrant) -> io::Result<Arc<Self>> {
        if read.bytes() != WORKSPACE_BYTES || write.bytes() != WORKSPACE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "utun workspace dimensions differ from the actual backend",
            ));
        }
        let read_buffer = read.try_buffer().map_err(io::Error::other)?;
        let write_buffer = write.try_buffer().map_err(io::Error::other)?;
        Ok(Arc::new(Self {
            read,
            write,
            buffers: Mutex::new(Some(Buffers {
                read: Buffer::Owned(read_buffer),
                write: Buffer::Owned(write_buffer),
            })),
        }))
    }
    pub(crate) fn take(&self) -> io::Result<Buffers> {
        self.buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "same utun workspaces already transferred",
                )
            })
    }
    pub(crate) fn notifications(&self) -> (PacketMemoryNotifications, PacketMemoryNotifications) {
        (
            self.read.defer_notifications(),
            self.write.defer_notifications(),
        )
    }
    pub(crate) fn take_unused(&self) -> Option<Buffers> {
        self.buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

//! Owned Wintun ring session, packets, and explicit `EndSessionReceipt` evidence.

use super::{raw::WintunSessionHandle, Adapter, Error, Wintun};
use std::{
    fmt, io, ptr, slice,
    sync::{Arc, OnceLock},
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, ERROR_NO_MORE_ITEMS, HANDLE, WAIT_FAILED, WAIT_OBJECT_0},
    System::Threading::{SetEvent, WaitForMultipleObjects, INFINITE},
};

#[derive(Clone, Debug)]
pub struct SessionIdentity(Arc<SessionIdentityInner>);

#[derive(Debug)]
struct SessionIdentityInner {
    native: usize,
}

impl SessionIdentity {
    pub(super) fn new(native: usize) -> Self {
        Self(Arc::new(SessionIdentityInner { native }))
    }

    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Issued only after explicit `WintunEndSession` returns for this exact session.
#[derive(Debug)]
pub struct EndSessionReceipt {
    identity: SessionIdentity,
}

impl EndSessionReceipt {
    pub fn identity(&self) -> &SessionIdentity {
        &self.identity
    }
}

/// Issued only when `WintunStartSession` returns null before acquiring a session handle.
#[derive(Debug)]
pub struct SessionNotAcquired {
    source: io::Error,
}

impl SessionNotAcquired {
    pub(super) fn from_null() -> Self {
        Self {
            source: io::Error::last_os_error(),
        }
    }
}

impl fmt::Display for SessionNotAcquired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WintunStartSession returned no Session: {}", self.source)
    }
}

impl std::error::Error for SessionNotAcquired {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Shutdown-event creation failed after acquiring and explicitly ending the session.
#[derive(Debug)]
pub struct SessionStartError {
    ended: EndSessionReceipt,
    source: io::Error,
}

impl SessionStartError {
    pub(super) fn new(ended: EndSessionReceipt, source: io::Error) -> Self {
        Self { ended, source }
    }

    pub fn ended(&self) -> &EndSessionReceipt {
        &self.ended
    }
}

impl fmt::Display for SessionStartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shutdown event setup failed after ending the acquired session: {}",
            self.source
        )
    }
}

impl std::error::Error for SessionStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The session ended, but closure of its original shutdown event failed.
#[derive(Debug)]
pub struct SessionCloseError {
    ended: EndSessionReceipt,
    shutdown_event: usize,
    source: io::Error,
}

impl SessionCloseError {
    pub fn ended(&self) -> &EndSessionReceipt {
        &self.ended
    }
}

impl fmt::Display for SessionCloseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "original shutdown event {:#x} close unconfirmed: {}",
            self.shutdown_event, self.source
        )
    }
}

impl std::error::Error for SessionCloseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub struct Session {
    pub(super) session: WintunSessionHandle,
    pub(super) wintun: Wintun,
    pub(super) read_event: OnceLock<usize>,
    pub(super) shutdown_event: HANDLE,
    pub(super) _adapter: Arc<Adapter>,
    pub(super) identity: SessionIdentity,
}

// SAFETY: Wintun session operations and Win32 event handles are thread-safe across reader/writer.
unsafe impl Send for Session {}
unsafe impl Sync for Session {}

impl Session {
    pub fn owned_identity(&self) -> SessionIdentity {
        self.identity.clone()
    }

    pub(super) fn end_original(&mut self) -> EndSessionReceipt {
        assert_eq!(self.identity.0.native, self.session as usize);
        let native = std::mem::replace(&mut self.session, ptr::null_mut());
        unsafe { (self.wintun.end_session)(native) };
        EndSessionReceipt {
            identity: self.identity.clone(),
        }
    }

    pub fn close_owned(mut self) -> Result<EndSessionReceipt, SessionCloseError> {
        let ended = self.end_original();
        let event = std::mem::replace(&mut self.shutdown_event, ptr::null_mut());
        if unsafe { CloseHandle(event) } == 0 {
            return Err(SessionCloseError {
                ended,
                shutdown_event: event as usize,
                source: io::Error::last_os_error(),
            });
        }
        Ok(ended)
    }

    pub fn allocate_send_packet(self: &Arc<Self>, size: u16) -> Result<Packet, Error> {
        let ptr = unsafe { (self.wintun.allocate_send_packet)(self.session, u32::from(size)) };
        if ptr.is_null() {
            Err(Error::Io(io::Error::last_os_error()))
        } else {
            Ok(Packet {
                kind: PacketKind::SendPending,
                bytes: unsafe { slice::from_raw_parts_mut(ptr, usize::from(size)) },
                session: self.clone(),
            })
        }
    }

    pub fn send_packet(&self, mut packet: Packet) {
        assert!(matches!(packet.kind, PacketKind::SendPending));
        unsafe { (self.wintun.send_packet)(self.session, packet.bytes.as_ptr()) };
        packet.kind = PacketKind::SendSent;
    }

    pub fn try_receive(self: &Arc<Self>) -> Result<Option<Packet>, Error> {
        let mut size = 0u32;
        let ptr = unsafe { (self.wintun.receive_packet)(self.session, &mut size) };
        debug_assert!(size <= u32::from(u16::MAX));
        if ptr.is_null() {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_NO_MORE_ITEMS as i32) {
                Ok(None)
            } else {
                Err(Error::Io(err))
            }
        } else {
            Ok(Some(Packet {
                kind: PacketKind::Receive,
                bytes: unsafe { slice::from_raw_parts_mut(ptr, size as usize) },
                session: self.clone(),
            }))
        }
    }

    pub fn get_read_wait_event(&self) -> Result<HANDLE, Error> {
        let raw = *self
            .read_event
            .get_or_init(|| unsafe { (self.wintun.get_read_wait_event)(self.session) as usize });
        Ok(raw as HANDLE)
    }

    pub fn receive_blocking(self: &Arc<Self>) -> Result<Packet, Error> {
        loop {
            for _ in 0..5 {
                if let Some(packet) = self.try_receive()? {
                    return Ok(packet);
                }
            }
            let handles = [self.get_read_wait_event()?, self.shutdown_event];
            let result = unsafe {
                WaitForMultipleObjects(handles.len() as u32, handles.as_ptr(), 0, INFINITE)
            };
            if result == WAIT_FAILED {
                return Err(Error::Io(io::Error::last_os_error()));
            }
            if result == WAIT_OBJECT_0 {
                continue;
            }
            if result == WAIT_OBJECT_0 + 1 {
                return Err(Error::ShuttingDown);
            }
            return Err(Error::Message(format!(
                "WaitForMultipleObjects returned unexpected status {result:#x}"
            )));
        }
    }

    pub fn shutdown(&self) -> Result<(), Error> {
        if unsafe { SetEvent(self.shutdown_event) } == 0 {
            Err(Error::Io(io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if !self.shutdown_event.is_null() {
            let _ = unsafe { CloseHandle(self.shutdown_event) };
            self.shutdown_event = ptr::null_mut();
        }
        if !self.session.is_null() {
            unsafe { (self.wintun.end_session)(self.session) };
            self.session = ptr::null_mut();
        }
    }
}

enum PacketKind {
    SendPending,
    SendSent,
    Receive,
}

pub struct Packet {
    kind: PacketKind,
    bytes: &'static mut [u8],
    session: Arc<Session>,
}

impl Packet {
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.bytes
    }

    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        if matches!(self.kind, PacketKind::Receive) {
            unsafe {
                (self.session.wintun.release_receive_packet)(
                    self.session.session,
                    self.bytes.as_ptr(),
                );
            }
        }
    }
}

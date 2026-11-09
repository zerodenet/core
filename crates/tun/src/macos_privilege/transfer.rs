//! One helper original and the distinct parent descriptor delivered by SCM_RIGHTS.
//! The actual parent owner stores this record before the first Create request.
use std::os::unix::net::UnixStream;
use std::{
    io,
    os::fd::RawFd,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
};
mod confirm;
mod descriptor;
mod receive;
pub(super) use descriptor::Original;
#[cfg(test)]
mod tests;

pub(crate) struct Transfer {
    id: u64,
    submitted: AtomicBool,
    receiving: AtomicBool,
    received: OnceLock<RawFd>,
    local: Mutex<Local>,
    progress: Mutex<confirm::Progress>,
    remote: Mutex<Remote>,
}
#[derive(Default)]
struct Local {
    descriptors: [Option<Original>; 2],
    received: bool,
    claimed: Option<RawFd>,
}
#[derive(Clone, Copy)]
enum Remote {
    Unattempted,
    Unknown,
    NoNative,
    Closed,
    CloseUnknown(i32),
}
impl Transfer {
    pub(super) fn new(id: u64) -> Self {
        Self {
            id,
            submitted: AtomicBool::new(false),
            receiving: AtomicBool::new(false),
            received: OnceLock::new(),
            local: Mutex::new(Local::default()),
            progress: Mutex::new(confirm::Progress::default()),
            remote: Mutex::new(Remote::Unattempted),
        }
    }
    pub(super) fn id(&self) -> u64 {
        self.id
    }
    pub(super) fn submit_create(&self) {
        self.submitted.store(true, Ordering::Release);
        *self.remote.lock().unwrap_or_else(|e| e.into_inner()) = Remote::Unknown;
    }
    pub(super) fn enter_receive(&self) {
        self.receiving.store(true, Ordering::Release);
    }
    pub(super) fn capture_received(&self, descriptors: &[RawFd]) -> io::Result<()> {
        let mut local = self.local.lock().unwrap_or_else(|e| e.into_inner());
        if local.received || descriptors.len() > local.descriptors.len() {
            return Err(io::Error::other("actual ancillary receiver was reused"));
        }
        // All delivered originals enter their finite slots before validation.
        for (slot, fd) in local.descriptors.iter_mut().zip(descriptors) {
            *slot = Some(Original::new(*fd, self.id));
        }
        local.received = true;
        Ok(())
    }
    pub(super) fn claim_received(&self, fd: RawFd) -> io::Result<()> {
        let mut local = self.local.lock().unwrap_or_else(|e| e.into_inner());
        if local.claimed.is_some()
            || !local
                .descriptors
                .iter()
                .flatten()
                .any(|original| original.fd() == fd)
        {
            return Err(io::Error::other(
                "descriptor is not this retained ancillary original",
            ));
        }
        local.claimed = Some(fd);
        self.received
            .set(fd)
            .map_err(|_| io::Error::other("same transferred descriptor already claimed"))
    }
    pub(super) fn observe_no_native(&self) {
        *self.remote.lock().unwrap_or_else(|e| e.into_inner()) = Remote::NoNative;
    }
    pub(super) fn observe_remote_close(&self, errno: i32) {
        *self.remote.lock().unwrap_or_else(|e| e.into_inner()) = if errno == 0 {
            Remote::Closed
        } else {
            Remote::CloseUnknown(errno)
        };
    }
    /// Called only by the same retained helper's authenticated exit observer.
    /// This never closes or confirms the distinct parent's local descriptor.
    pub(super) fn observe_remote_settled(&self) {
        *self.remote.lock().unwrap_or_else(|e| e.into_inner()) = Remote::Closed;
    }
    pub(super) fn remote_closed(&self) -> bool {
        matches!(
            *self.remote.lock().unwrap_or_else(|e| e.into_inner()),
            Remote::NoNative | Remote::Closed
        ) || !self.submitted.load(Ordering::Acquire)
    }
    pub(super) fn close_unclaimed(&self) -> io::Result<()> {
        let mut local = self.local.lock().unwrap_or_else(|e| e.into_inner());
        let claimed = local.claimed;
        let mut first = None;
        for original in local.descriptors.iter_mut().flatten() {
            if Some(original.fd()) != claimed {
                if let Err(source) = original.close_once() {
                    first.get_or_insert(source);
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
    pub(super) fn require_closed(&self) -> io::Result<()> {
        // Observation never waits for a peer that is inside blocking IPC.
        let local = self.local.lock().unwrap_or_else(|e| e.into_inner());
        if self.receiving.load(Ordering::Acquire) && !local.received {
            return Err(io::Error::other(
                "actual ancillary receive is still pending",
            ));
        }
        if local
            .descriptors
            .iter()
            .flatten()
            .any(|original| Some(original.fd()) != local.claimed && !original.closed())
        {
            return Err(io::Error::other(
                "unclaimed transferred original lacks its close receipt",
            ));
        }
        match *self.remote.lock().unwrap_or_else(|e| e.into_inner()) {
            Remote::Unattempted | Remote::NoNative | Remote::Closed => Ok(()),
            Remote::CloseUnknown(errno) => Err(io::Error::from_raw_os_error(errno)),
            Remote::Unknown => Err(io::Error::other(
                "helper original has no matching close acknowledgement",
            )),
        }
    }
}
#[derive(Clone)]
pub(crate) enum TransferOwner {
    Library(Arc<Transfer>),
    Kernel(crate::packet_memory::PacketMetadataArc<Transfer>),
}
impl std::ops::Deref for TransferOwner {
    type Target = Transfer;
    fn deref(&self) -> &Transfer {
        match self {
            Self::Library(owner) => owner,
            Self::Kernel(owner) => owner,
        }
    }
}
impl TransferOwner {
    pub(super) fn from_prepaid(
        id: u64,
        credit: crate::packet_memory::PacketMetadataCredit,
    ) -> io::Result<Self> {
        crate::packet_memory::PacketMetadataArc::from_prepaid(Transfer::new(id), credit)
            .map(Self::Kernel)
            .map_err(|(source, _, _)| io::Error::other(source))
    }
    pub(super) fn prepare(
        id: u64,
        memory: Option<&crate::packet_memory::PacketMemoryView>,
    ) -> io::Result<Self> {
        match memory {
            Some(memory) => {
                type Paid = crate::packet_memory::PacketMetadataArc<Transfer>;
                let layout = Paid::layout().map_err(io::Error::other)?;
                let credit = memory
                    .reserve_driver_metadata(layout)
                    .map_err(io::Error::other)?;
                Paid::from_prepaid(Transfer::new(id), credit)
                    .map(Self::Kernel)
                    .map_err(|(source, _, _)| io::Error::other(source))
            }
            None => Ok(Self::Library(Arc::new(Transfer::new(id)))),
        }
    }
    pub(super) fn same_owner(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Library(a), Self::Library(b)) => Arc::ptr_eq(a, b),
            (Self::Kernel(a), Self::Kernel(b)) => {
                crate::packet_memory::PacketMetadataArc::ptr_eq(a, b)
            }
            _ => false,
        }
    }
}

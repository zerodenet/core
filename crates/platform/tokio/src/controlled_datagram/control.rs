//! Host-only separation of a unique I/O view from its same-original control.
use super::*;
use std::sync::{atomic::Ordering, Weak};

/// Neither a socket clone nor another FD pool. The original owner issues at
/// most one control for a successfully acquired view before that view escapes.
pub struct ControlledDatagramControl {
    entry: Arc<Entry>,
}
/// Weak original-entry identity; supplies no I/O, close or task authority.
#[derive(Clone)]
pub struct ControlledDatagramIdentity(Weak<Entry>);
impl ControlledDatagramIdentity {
    pub fn matches_control(&self, control: &ControlledDatagramControl) -> bool {
        self.0.ptr_eq(&Arc::downgrade(&control.entry))
    }
    pub fn matches_socket(&self, socket: &ControlledDatagramSocket) -> bool {
        self.0.ptr_eq(&Arc::downgrade(&socket.entry))
    }
}
#[derive(Clone)]
pub struct ControlledDatagramClosed {
    entry: Arc<Entry>,
}
impl ControlledDatagramClosed {
    pub fn matches(&self, control: &ControlledDatagramControl) -> bool {
        Arc::ptr_eq(&self.entry, &control.entry)
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.entry.id
    }
}
#[derive(Clone, Debug)]
pub struct DatagramControlObservation {
    pub native_closed: bool,
    pub operation_pending: bool,
    pub unknown: Option<Arc<std_io::Error>>,
}
impl ControlledDatagramOwner {
    /// Validate the actual same entry, not equal reservation numbers. Taking a
    /// foreign/already-issued control does not retire or mutate that socket.
    pub fn take_control(
        &self,
        socket: &ControlledDatagramSocket,
    ) -> Option<ControlledDatagramControl> {
        let entry = self.entries.get(&socket.entry.id)?;
        if !Arc::ptr_eq(entry, &socket.entry)
            || entry
                .control_issued
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return None;
        }
        Some(ControlledDatagramControl {
            entry: entry.clone(),
        })
    }
}
impl ControlledDatagramControl {
    pub fn identity(&self) -> ControlledDatagramIdentity {
        ControlledDatagramIdentity(Arc::downgrade(&self.entry))
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.entry.id
    }
    /// Revoke original I/O first. This can synchronously wake/drop external
    /// callers and must be invoked outside every host/resource owner lock.
    pub fn revoke(&self) {
        self.entry.retire_io();
    }
    pub fn close(&self) -> Result<ControlledDatagramClosed, Arc<std_io::Error>> {
        self.entry.close().map(|_| ControlledDatagramClosed {
            entry: self.entry.clone(),
        })
    }
    /// Busy native/close leases stay pending; they are not Unknown or receipts.
    pub fn try_close(&self) -> Option<Result<ControlledDatagramClosed, Arc<std_io::Error>>> {
        self.entry.try_close().map(|result| {
            result.map(|_| ControlledDatagramClosed {
                entry: self.entry.clone(),
            })
        })
    }
    /// Pure nonblocking state observation. This issues neither close nor join
    /// evidence and preserves the actual original Unknown source Arc.
    pub fn observe(&self) -> DatagramControlObservation {
        let native = match self.entry.native.try_lock() {
            Ok(native) => native,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return DatagramControlObservation {
                    native_closed: false,
                    operation_pending: true,
                    unknown: None,
                }
            }
        };
        DatagramControlObservation {
            native_closed: matches!(&*native, NativeHandleState::Closed),
            operation_pending: self.entry.close_running.load(Ordering::Acquire),
            unknown: match &*native {
                NativeHandleState::Unknown(source) => Some(source.clone()),
                _ => None,
            },
        }
    }
}
impl Drop for ControlledDatagramControl {
    fn drop(&mut self) {
        // A dropped control can only retire views. No syscall/closed receipt is
        // implied; the persistent original owner retains its charged entry.
        self.entry.retire_io();
    }
}

#[cfg(test)]
#[path = "../../tests/controlled_datagram/control.rs"]
mod tests;

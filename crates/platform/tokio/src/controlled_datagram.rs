//! Bounded original UDP ownership, including acquisition failure and rotation.
//! Dropping an I/O handle never releases its owner's native reservation.
mod acquire;
mod flags;
mod io;
#[cfg(test)]
mod tests;
use crate::{EgressInterface, TokioDatagramSocket};
use std::{
    collections::BTreeMap,
    fmt, io as std_io,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatagramReservationId(u64);
impl DatagramReservationId {
    pub fn get(self) -> u64 {
        self.0
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatagramCloseReceipt {
    id: DatagramReservationId,
}
impl DatagramCloseReceipt {
    pub fn reservation(self) -> DatagramReservationId {
        self.id
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquisitionOutcome {
    /// The operation never acquired a native handle.
    KnownNoNative,
    /// The original handle was explicitly closed, with a successful OS result.
    Closed(DatagramCloseReceipt),
    /// No close confirmation exists. This reservation stays charged.
    Unknown(DatagramReservationId),
}
#[derive(Debug)]
pub struct DatagramAcquisitionError {
    pub reservation: Option<DatagramReservationId>,
    pub outcome: AcquisitionOutcome,
    pub source: Arc<std_io::Error>,
}
impl fmt::Display for DatagramAcquisitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for DatagramAcquisitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
#[derive(Debug)]
pub struct UnconfirmedDatagram {
    pub reservation: DatagramReservationId,
    pub source: Arc<std_io::Error>,
    pub acquisition_source: Option<Arc<std_io::Error>>,
}
#[derive(Debug, Default)]
pub struct DatagramOwnerReport {
    pub confirmed_closed: u64,
    pub retained_live: usize,
    pub known_no_native: u64,
    /// Exact bounded original sources, including the native close source.
    pub unconfirmed: Vec<UnconfirmedDatagram>,
}
impl DatagramOwnerReport {
    pub fn is_complete(&self) -> bool {
        self.retained_live == 0 && self.unconfirmed.is_empty()
    }
}
/// The caller retains this owner for the actual resource's whole lifetime.
/// `limit` counts reserved, live, retiring and unknown originals together.
pub struct ControlledDatagramOwner {
    limit: NonZeroUsize,
    next: u64,
    entries: BTreeMap<DatagramReservationId, Arc<Entry>>,
    confirmed_closed: u64,
    known_no_native: u64,
}
struct Entry {
    id: DatagramReservationId,
    native: Mutex<NativeHandleState>,
    // Prepared before the original fd exists; installed before consuming OS
    // registration/close calls, including panic or cancellation of the caller.
    uncertain: Arc<std_io::Error>,
    acquisition_failure: Mutex<Option<Arc<std_io::Error>>>,
}
enum NativeHandleState {
    Reserved,
    Raw(std::net::UdpSocket),
    Live(TokioDatagramSocket),
    Unknown(Arc<std_io::Error>),
    Closed,
    NoNative,
}
impl Entry {
    fn lock(&self) -> MutexGuard<'_, NativeHandleState> {
        self.native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
/// Unique non-cloneable I/O view. The persistent owner retains the original.
/// The caller must serialize native receive polling, just as for Tokio UDP.
pub struct ControlledDatagramSocket {
    entry: Arc<Entry>,
    interface: Option<EgressInterface>,
}
impl fmt::Debug for ControlledDatagramOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlledDatagramOwner")
            .field("limit", &self.limit)
            .field("retained", &self.entries.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for ControlledDatagramSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ControlledDatagramSocket")
            .field(&self.entry.id)
            .finish()
    }
}
impl ControlledDatagramOwner {
    pub fn new(limit: NonZeroUsize) -> Self {
        Self {
            limit,
            next: 0,
            entries: BTreeMap::new(),
            confirmed_closed: 0,
            known_no_native: 0,
        }
    }
    fn reap_confirmed(&mut self) {
        self.entries.retain(|_, entry| match &*entry.lock() {
            NativeHandleState::Closed => {
                self.confirmed_closed += 1;
                false
            }
            NativeHandleState::NoNative => {
                self.known_no_native += 1;
                false
            }
            _ => true,
        });
    }
    pub fn occupied(&mut self) -> usize {
        self.reap_confirmed();
        self.entries.len()
    }
    /// Whether this owner still retains the exact reservation. Confirmed
    /// entries are removed only after their receipts entered owner accounting.
    pub fn retains(&self, id: DatagramReservationId) -> bool {
        self.entries.contains_key(&id)
    }
    fn reserve(&mut self) -> Result<Arc<Entry>, DatagramAcquisitionError> {
        self.reap_confirmed();
        let denied = |message| DatagramAcquisitionError {
            reservation: None,
            outcome: AcquisitionOutcome::KnownNoNative,
            source: Arc::new(std_io::Error::other(message)),
        };
        // An unresolved original cannot be repaired by opening another one.
        if let Some((id, source)) = self
            .entries
            .values()
            .find_map(|entry| match &*entry.lock() {
                NativeHandleState::Unknown(source) => Some((entry.id, source.clone())),
                _ => None,
            })
        {
            return Err(DatagramAcquisitionError {
                reservation: Some(id),
                outcome: AcquisitionOutcome::Unknown(id),
                source,
            });
        }
        if self.entries.len() >= self.limit.get() {
            return Err(denied("original UDP reservation capacity exhausted"));
        }
        let next = self
            .next
            .checked_add(1)
            .ok_or_else(|| denied("original UDP reservation identity exhausted"))?;
        let id = DatagramReservationId(self.next);
        let entry = Arc::new(Entry {
            id,
            native: Mutex::new(NativeHandleState::Reserved),
            uncertain: Arc::new(std_io::Error::other(
                "original UDP native operation remains unconfirmed",
            )),
            acquisition_failure: Mutex::new(None),
        });
        self.entries.insert(id, entry.clone());
        self.next = next;
        Ok(entry)
    }
    /// Report extraction does not clear unknown originals or their sources.
    /// The caller must persist returned successful receipts before suspension.
    pub fn take_report(&mut self) -> DatagramOwnerReport {
        self.reap_confirmed();
        let mut report = DatagramOwnerReport {
            confirmed_closed: std::mem::take(&mut self.confirmed_closed),
            retained_live: 0,
            known_no_native: std::mem::take(&mut self.known_no_native),
            unconfirmed: Vec::new(),
        };
        for entry in self.entries.values() {
            match &*entry.lock() {
                NativeHandleState::Unknown(source) => {
                    report.unconfirmed.push(UnconfirmedDatagram {
                        reservation: entry.id,
                        source: source.clone(),
                        acquisition_source: entry
                            .acquisition_failure
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone(),
                    })
                }
                _ => report.retained_live += 1,
            }
        }
        report
    }
    /// Call only after the actual consumer stopped using these originals.
    /// This reports real OS close outcomes, never a task-exit assertion.
    pub fn close_all(&mut self) -> DatagramOwnerReport {
        for entry in self.entries.values() {
            let _ = entry.close();
        }
        self.take_report()
    }
}
impl Entry {
    fn close(&self) -> Result<DatagramCloseReceipt, Arc<std_io::Error>> {
        let mut native = self.lock();
        if matches!(&*native, NativeHandleState::Closed) {
            return Ok(DatagramCloseReceipt { id: self.id });
        }
        if matches!(
            &*native,
            NativeHandleState::Reserved | NativeHandleState::NoNative
        ) {
            *native = NativeHandleState::NoNative;
            return Err(Arc::new(std_io::Error::other(
                "no native UDP socket was acquired",
            )));
        }
        if let NativeHandleState::Unknown(source) = &*native {
            if !TokioDatagramSocket::close_is_retryable(source) {
                return Err(source.clone());
            }
            let source = source.clone();
            *native = NativeHandleState::Unknown(self.uncertain.clone());
            return match TokioDatagramSocket::retry_close(&source) {
                Ok(true) => {
                    *native = NativeHandleState::Closed;
                    Ok(DatagramCloseReceipt { id: self.id })
                }
                Ok(false) => {
                    *native = NativeHandleState::Unknown(source.clone());
                    Err(source)
                }
                Err(source) => {
                    let source = Arc::new(source);
                    *native = NativeHandleState::Unknown(source.clone());
                    Err(source)
                }
            };
        }
        let original = std::mem::replace(
            &mut *native,
            NativeHandleState::Unknown(self.uncertain.clone()),
        );
        let result = match original {
            NativeHandleState::Raw(socket) => crate::datagram::native_close(socket),
            NativeHandleState::Live(socket) => socket.close(),
            _ => unreachable!("native state checked under same lock"),
        };
        match result {
            Ok(()) => {
                *native = NativeHandleState::Closed;
                Ok(DatagramCloseReceipt { id: self.id })
            }
            Err(source) => {
                let source = Arc::new(source);
                *native = NativeHandleState::Unknown(source.clone());
                Err(source)
            }
        }
    }
}

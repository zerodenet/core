//! Explicit ownership of actual ICMP originals, not ordinary UDP authority.
mod acquire;
mod close;
mod io;
mod report;
#[cfg(test)]
mod tests;
use crate::EgressInterface;
use std::{
    collections::BTreeMap,
    fmt, io as std_io,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IcmpReservationId(u64);
impl IcmpReservationId {
    pub fn get(self) -> u64 {
        self.0
    }
}
/// Receipt for the same original entry. A numeric id alone is not authority.
pub struct IcmpCloseReceipt {
    entry: Arc<Entry>,
}
impl IcmpCloseReceipt {
    pub fn reservation(&self) -> IcmpReservationId {
        self.entry.id
    }
    pub fn matches(&self, view: &ControlledIcmpSocket) -> bool {
        Arc::ptr_eq(&self.entry, &view.entry)
    }
}
impl fmt::Debug for IcmpCloseReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("IcmpCloseReceipt")
            .field(&self.entry.id)
            .finish()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcmpMode {
    Raw,
    PingDatagram,
}
#[derive(Debug)]
pub enum IcmpAcquisitionOutcome {
    KnownNoNative,
    Closed(IcmpCloseReceipt),
    Unknown(IcmpReservationId),
}
#[derive(Debug)]
pub struct IcmpAcquisitionError {
    pub raw_permission_source: Option<Arc<std_io::Error>>,
    pub reservation: Option<IcmpReservationId>,
    pub outcome: IcmpAcquisitionOutcome,
    pub source: Arc<std_io::Error>,
}
impl fmt::Display for IcmpAcquisitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for IcmpAcquisitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
#[derive(Debug)]
pub struct UnconfirmedIcmp {
    pub reservation: IcmpReservationId,
    pub source: Arc<std_io::Error>,
    pub acquisition_source: Option<Arc<std_io::Error>>,
    pub raw_permission_source: Option<Arc<std_io::Error>>,
    pub close_source: Option<Arc<std_io::Error>>,
}
#[derive(Debug, Default)]
pub struct IcmpOwnerReport {
    pub first_raw_permission_source: Option<Arc<std_io::Error>>,
    pub first_acquisition_failure: Option<Arc<std_io::Error>>,
    pub first_close_failure: Option<Arc<std_io::Error>>,
    pub confirmed_closed: u64,
    pub known_no_native: u64,
    pub retained_live: usize,
    pub unconfirmed: Vec<UnconfirmedIcmp>,
}
impl IcmpOwnerReport {
    pub fn is_complete(&self) -> bool {
        self.retained_live == 0 && self.unconfirmed.is_empty()
    }
}
/// Install before bind. The explicit limit counts live, retiring and Unknown
/// entries, not lifetime probes. No task, extra probe socket or FD clone exists.
pub struct ControlledIcmpOwner {
    first_raw_permission_source: Option<Arc<std_io::Error>>,
    identity: Arc<()>,
    first_acquisition_failure: Option<Arc<std_io::Error>>,
    first_close_failure: Option<Arc<std_io::Error>>,
    limit: NonZeroUsize,
    next: u64,
    cancel: CancellationToken,
    entries: BTreeMap<u64, Arc<Entry>>,
    confirmed_closed: u64,
    known_no_native: u64,
}
struct Entry {
    owner: Arc<()>,
    id: IcmpReservationId,
    cancel: CancellationToken,
    native: Mutex<Native>,
    uncertain: Arc<std_io::Error>,
    ipv6: bool,
    metadata: Mutex<Metadata>,
}
#[derive(Default)]
struct Metadata {
    mode: Option<IcmpMode>,
    acquisition_source: Option<Arc<std_io::Error>>,
    raw_permission_source: Option<Arc<std_io::Error>>,
    close_source: Option<Arc<std_io::Error>>,
}
enum Native {
    Reserved,
    Raw(std::net::UdpSocket),
    Live(tokio::net::UdpSocket),
    Unknown(Arc<std_io::Error>),
    Closed,
    NoNative,
}
impl Entry {
    fn native(&self) -> MutexGuard<'_, Native> {
        self.native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn metadata(&self) -> MutexGuard<'_, Metadata> {
        self.metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
/// Unique caller I/O; Drop cancels polling but never signs a native receipt.
pub struct ControlledIcmpSocket {
    entry: Arc<Entry>,
}
impl Drop for ControlledIcmpSocket {
    fn drop(&mut self) {
        self.entry.cancel.cancel();
    }
}
impl ControlledIcmpOwner {
    pub fn new(limit: NonZeroUsize, parent: CancellationToken) -> Self {
        Self {
            first_raw_permission_source: None,
            identity: Arc::new(()),
            first_acquisition_failure: None,
            first_close_failure: None,
            limit,
            next: 0,
            cancel: parent.child_token(),
            entries: BTreeMap::new(),
            confirmed_closed: 0,
            known_no_native: 0,
        }
    }
    fn reap(&mut self) {
        self.entries.retain(|_, entry| match &*entry.native() {
            Native::Closed => {
                self.confirmed_closed += 1;
                false
            }
            Native::NoNative => {
                self.known_no_native += 1;
                false
            }
            _ => true,
        });
    }
    pub fn occupied(&mut self) -> usize {
        self.reap();
        self.entries.len()
    }
    pub fn revoke(&self) {
        self.cancel.cancel();
    }
    fn reserve(&mut self, ipv6: bool) -> Result<Arc<Entry>, IcmpAcquisitionError> {
        self.reap();
        let denied = |message| IcmpAcquisitionError {
            raw_permission_source: None,
            reservation: None,
            outcome: IcmpAcquisitionOutcome::KnownNoNative,
            source: Arc::new(std_io::Error::other(message)),
        };
        if self.cancel.is_cancelled() {
            return Err(denied("ICMP owner is revoked"));
        }
        if let Some(entry) = self
            .entries
            .values()
            .find(|entry| matches!(&*entry.native(), Native::Unknown(_)))
        {
            let native = entry.native();
            let Native::Unknown(source) = &*native else {
                unreachable!()
            };
            return Err(IcmpAcquisitionError {
                raw_permission_source: entry.metadata().raw_permission_source.clone(),
                reservation: Some(entry.id),
                outcome: IcmpAcquisitionOutcome::Unknown(entry.id),
                source: source.clone(),
            });
        }
        if self.entries.len() >= self.limit.get() {
            return Err(denied("ICMP original capacity exhausted"));
        }
        let next = self
            .next
            .checked_add(1)
            .ok_or_else(|| denied("ICMP identity exhausted"))?;
        let entry = Arc::new(Entry {
            owner: self.identity.clone(),
            id: IcmpReservationId(self.next),
            cancel: self.cancel.child_token(),
            native: Mutex::new(Native::Reserved),
            uncertain: Arc::new(std_io::Error::other(
                "ICMP original native outcome is unconfirmed",
            )),
            ipv6,
            metadata: Mutex::new(Metadata::default()),
        });
        self.entries.insert(self.next, entry.clone());
        self.next = next;
        Ok(entry)
    }
    /// Retire exactly this view's entry, never another pool's equal numeric id.
    pub fn close(
        &mut self,
        view: &ControlledIcmpSocket,
    ) -> Result<IcmpCloseReceipt, Arc<std_io::Error>> {
        if !Arc::ptr_eq(&self.identity, &view.entry.owner) {
            return Err(Arc::new(std_io::Error::other(
                "ICMP view belongs to another owner",
            )));
        }
        if !self
            .entries
            .get(&view.entry.id.0)
            .is_some_and(|entry| Arc::ptr_eq(entry, &view.entry))
        {
            return if matches!(&*view.entry.native(), Native::Closed) {
                Ok(IcmpCloseReceipt {
                    entry: view.entry.clone(),
                })
            } else {
                Err(Arc::new(std_io::Error::other(
                    "ICMP entry is no longer retained",
                )))
            };
        }
        let result = view.entry.close();
        if let Some(source) = view.entry.metadata().close_source.clone() {
            self.first_close_failure.get_or_insert(source);
        }
        result
    }
}
impl Drop for ControlledIcmpOwner {
    fn drop(&mut self) {
        self.revoke();
    }
}

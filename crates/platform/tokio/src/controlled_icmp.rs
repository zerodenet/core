//! Explicit ownership of actual ICMP originals, not ordinary UDP authority.
mod acquire;
mod notifications;
mod observation;
mod reserve;
pub use notifications::IcmpNotifications;
use notifications::Identity;
pub use observation::{IcmpAttemptDisposition, IcmpAttemptObservation};
mod totals;
pub use totals::IcmpTotalsBinding;
mod close;
mod io;
mod relay;
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
    identity: Identity,
    first_acquisition_failure: Option<Arc<std_io::Error>>,
    first_close_failure: Option<Arc<std_io::Error>>,
    limit: NonZeroUsize,
    next: u64,
    cancel: CancellationToken,
    entries: BTreeMap<u64, Arc<Entry>>,
    confirmed_closed: u64,
    known_no_native: u64,
    totals: Option<IcmpTotalsBinding>,
}
struct Entry {
    wakes: relay::IoRelays,
    owner: Identity,
    id: IcmpReservationId,
    cancel: CancellationToken,
    native: Mutex<Native>,
    uncertain: Arc<std_io::Error>,
    creation_running: std::sync::atomic::AtomicBool,
    view_live: std::sync::atomic::AtomicBool,
    changed: Identity,
    metadata_receiver:
        Option<crate::controlled_datagram::metadata_pool::MetadataPoolRegistrationObservation>,
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
    NeverAcquired,
}
impl Entry {
    fn native(&self) -> relay::NativeGuard<'_> {
        relay::NativeGuard::lock(self)
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
        self.entry
            .view_live
            .store(false, std::sync::atomic::Ordering::Release);
        self.entry.cancel.cancel();
        self.entry.changed.notify_waiters();
    }
}
impl ControlledIcmpOwner {
    pub fn new(limit: NonZeroUsize, parent: CancellationToken) -> Self {
        Self::with_identity(limit, parent, Identity::library())
    }
    pub fn new_with_metadata(
        limit: NonZeroUsize,
        parent: CancellationToken,
        memory: &crate::contract::component::PacketDriverMetadataView,
    ) -> Result<Self, zero_traits::packet_metadata::PacketMemoryError> {
        Ok(Self::with_identity(
            limit,
            parent,
            Identity::hosted(memory)?,
        ))
    }
    pub fn defer_notifications(&self) -> IcmpNotifications {
        self.identity.defer()
    }
    fn with_identity(limit: NonZeroUsize, parent: CancellationToken, identity: Identity) -> Self {
        Self {
            first_raw_permission_source: None,
            identity,
            first_acquisition_failure: None,
            first_close_failure: None,
            limit,
            next: 0,
            cancel: parent.child_token(),
            entries: BTreeMap::new(),
            confirmed_closed: 0,
            known_no_native: 0,
            totals: None,
        }
    }
    fn reap(&mut self) {
        self.entries.retain(|_, entry| match &*entry.native() {
            Native::Closed => {
                self.confirmed_closed += 1;
                false
            }
            Native::NeverAcquired => {
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
    /// Retire exactly this view's entry, never another pool's equal numeric id.
    pub fn close(
        &mut self,
        view: &ControlledIcmpSocket,
    ) -> Result<IcmpCloseReceipt, Arc<std_io::Error>> {
        if !self.identity.same(&view.entry.owner) {
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

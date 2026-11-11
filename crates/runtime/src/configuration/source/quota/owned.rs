//! One prepaid original quota controller; Weak custody retains its real header.
use super::*;
use std::ops::Deref;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout, PacketMetadataWeak};

#[derive(Clone)]
pub struct OwnedQuotaStorage(PacketMetadataArc<QuotaStorage>);
#[derive(Clone)]
pub struct WeakQuotaStorage(PacketMetadataWeak<QuotaStorage>);
#[derive(Clone)]
pub(super) enum StorageEntry {
    Library(Arc<QuotaStorage>),
    Owned(OwnedQuotaStorage),
}
impl Deref for StorageEntry {
    type Target = QuotaStorage;
    fn deref(&self) -> &QuotaStorage {
        match self {
            Self::Library(value) => value,
            Self::Owned(value) => value,
        }
    }
}
impl Deref for OwnedQuotaStorage {
    type Target = QuotaStorage;
    fn deref(&self) -> &QuotaStorage {
        &self.0
    }
}
impl std::fmt::Debug for OwnedQuotaStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}
impl OwnedQuotaStorage {
    pub fn downgrade(&self) -> WeakQuotaStorage {
        WeakQuotaStorage(PacketMetadataArc::downgrade(&self.0))
    }
    pub fn same_original(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
}
impl WeakQuotaStorage {
    pub fn upgrade(&self) -> Option<OwnedQuotaStorage> {
        self.0.upgrade().map(OwnedQuotaStorage)
    }
}
impl ConfigurationSource {
    /// This same counter is established before the first lease open or read.
    /// Later Host assembly adopts it unchanged; this grants no Scope or IO.
    pub fn open_owned_quota_storage(
        self: &Arc<Self>,
        path: impl Into<PathBuf>,
        memory: &QuotaMemoryBudget,
    ) -> io::Result<OwnedQuotaStorage> {
        let same_source = self
            .quota
            .memory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|actual| actual.same_budget(memory));
        if !same_source {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "foreign quota memory owner",
            ));
        }
        let path = path.into();
        let layout = PacketMetadataArc::<QuotaStorage>::layout()
            .and_then(|header| {
                header.checked_add(PacketMetadataLayout::array::<u8>(path.capacity())?)
            })
            .map_err(io::Error::other)?;
        let credit = memory.reserve(layout)?;
        super::super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available()?;
            let mut slot = self
                .quota
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(previous) = slot.as_ref() {
                previous.confirm_closed()?;
                if !previous.execution.retired() {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "previous quota writer remains owned",
                    ));
                }
                if let Some(failure) = previous.report_worker_failure() {
                    return Err(failure);
                }
            }
            let storage = QuotaStorage {
                source: Arc::downgrade(self),
                path,
                lease: Mutex::new(Lease::default()),
                worker: Default::default(),
                execution: execution::Execution::cold(memory)?,
            };
            let storage = match PacketMetadataArc::from_prepaid(storage, credit.into_metadata()) {
                Ok(value) => OwnedQuotaStorage(value),
                Err((error, value, credit)) => {
                    drop(value);
                    drop(slot);
                    drop(credit);
                    return Err(io::Error::other(error));
                }
            };
            let previous = slot.replace(StorageEntry::Owned(storage.clone()));
            drop(slot);
            drop(previous);
            if let Some(tasks) = self.quota.tasks.get() {
                storage.execution.bind_kernel(tasks.clone())?;
            }
            storage.acquire()?;
            Ok(storage)
        })?
    }
}

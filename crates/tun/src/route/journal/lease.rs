//! Existing file responsibility and the actual Source's owned lock.
use super::*;
mod retained;
pub(in crate::route) struct RouteLease {
    pub(super) journal_path: PathBuf,
    owner_path: PathBuf,
    pub(super) _lock: Lock,
    metadata: Option<crate::packet_memory::PacketDriverMetadataView>,
    pub(in crate::route) file_io:
        Option<crate::packet_memory::PacketMetadataArc<crate::route::file_originals::RouteFileIo>>,
    failures: Option<(
        crate::route::RouteFileFaults,
        crate::packet_memory::PacketMetadataArc<crate::route::file_originals::Attempt>,
    )>,
    owner_attempted: bool,
    owner_retired: bool,
    close_reported: bool,
}
impl std::fmt::Debug for RouteLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteLease")
            .field("journal_path", &self.journal_path)
            .field("lock", &self._lock)
            .field("transient", &self.file_io.is_some())
            .field("fault_delivery", &self.failures.is_some())
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub(super) enum Lock {
    Existing(std::fs::File),
    Retained(crate::route::file_originals::TrackedFile),
    #[cfg(target_os = "macos")]
    PendingOwned(Option<crate::macos_privilege::MacosRouteLock>),
    #[cfg(target_os = "macos")]
    Owned(crate::macos_privilege::MacosRouteLock),
}
impl RouteLease {
    pub(in crate::route) fn acquire(
        recovery_key: &str,
        ipv6: bool,
        metadata: Option<&crate::packet_memory::PacketDriverMetadataView>,
    ) -> io::Result<Self> {
        Self::acquire_family(
            route_journal_path(recovery_key, ipv6)?,
            route_family_lock_path(ipv6)?,
            recovery_key,
            ipv6,
            metadata,
        )
    }
    #[cfg(test)]
    pub(in crate::route) fn acquire_at(journal_path: PathBuf, tun_name: &str) -> io::Result<Self> {
        let lock_path = journal_path.with_extension("lock");
        Self::acquire_paths(journal_path, lock_path, tun_name)
    }
    #[cfg(test)]
    pub(in crate::route) fn acquire_paths(
        journal_path: PathBuf,
        lock_path: PathBuf,
        owner: &str,
    ) -> io::Result<Self> {
        Self::acquire_family(journal_path, lock_path, owner, false, None)
    }
    fn acquire_family(
        journal_path: PathBuf,
        lock_path: PathBuf,
        owner: &str,
        ipv6: bool,
        metadata: Option<&crate::packet_memory::PacketDriverMetadataView>,
    ) -> io::Result<Self> {
        let metadata = metadata.cloned();
        let owner_path = lock_owner_path(&lock_path);
        let _ = ipv6;
        #[cfg(target_os = "macos")]
        let owned = crate::macos_privilege::journal_lock(&lock_path, ipv6);
        #[cfg(target_os = "macos")]
        if let Some(lock) = owned {
            let lock = lock.map_err(|source| {
                conflict(source, &owner_path, &lock_path, owner, metadata.as_ref())
            })?;
            let _context = lock.context()?;
            persist_lock_owner(&owner_path, owner)?;
            return Ok(Self {
                journal_path,
                owner_path,
                _lock: Lock::Owned(lock),
                metadata,
                file_io: None,
                failures: None,
                owner_attempted: false,
                owner_retired: false,
                close_reported: false,
            });
        }
        // The existing non-Mac and independent file path. A metadata view
        // accounts storage only; it does not qualify this FD's native cleanup.
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|source| {
            conflict(source, &owner_path, &lock_path, owner, metadata.as_ref())
        })?;
        persist_lock_owner(&owner_path, owner)?;
        Ok(Self {
            journal_path,
            owner_path,
            _lock: Lock::Existing(lock),
            metadata,
            file_io: None,
            failures: None,
            owner_attempted: false,
            owner_retired: false,
            close_reported: false,
        })
    }
    /// Only a view of this same actual Source Root, without policy/IO rights.
    pub(in crate::route) fn metadata(
        &self,
    ) -> Option<crate::packet_memory::PacketDriverMetadataView> {
        self.metadata.clone()
    }
    #[cfg(target_os = "macos")]
    pub(super) fn source(&self) -> Option<crate::macos_privilege::MacosRouteFileAccess> {
        match &self._lock {
            Lock::Owned(lock) => Some(lock.access()),
            Lock::Existing(_) | Lock::Retained(_) => None,
            Lock::PendingOwned(lock) => lock.as_ref().map(|lock| lock.access()),
        }
    }
    #[cfg(target_os = "macos")]
    pub(super) fn context(&self) -> io::Result<Option<crate::MacosHelperContext>> {
        self.source().map(|source| source.context()).transpose()
    }
}
impl Drop for RouteLease {
    fn drop(&mut self) {
        match &self._lock {
            Lock::Existing(lock) => {
                if let Err(error) = remove_owner(&self.owner_path) {
                    tracing::warn!(%error,"failed to remove TUN route lease owner metadata");
                }
                if let Err(error) = fs2::FileExt::unlock(lock) {
                    tracing::warn!(%error,"failed to unlock TUN route lease");
                }
            }
            #[cfg(target_os = "macos")]
            Lock::Owned(_) | Lock::PendingOwned(_) => {}
            Lock::Retained(_) => {}
        }
        // Owned Drop only returns the lease. Its same Session keeps the actual
        // FD until its original duty consumes explicit close, including errors.
    }
}
#[cfg(test)]
impl Lock {
    pub(super) fn try_clone(&self) -> io::Result<std::fs::File> {
        match self {
            Self::Existing(lock) => lock.try_clone(),
            Self::Retained(_) => Err(io::Error::other(
                "Kernel route lock cannot duplicate its original",
            )),
            #[cfg(target_os = "macos")]
            Self::PendingOwned(_) => Err(io::Error::other(
                "Kernel pending lock cannot duplicate its original",
            )),
            #[cfg(target_os = "macos")]
            Self::Owned(_) => Err(io::Error::other(
                "Kernel route lock cannot duplicate its original",
            )),
        }
    }
}
fn conflict(
    source: io::Error,
    owner_path: &Path,
    lock_path: &Path,
    owner: &str,
    metadata: Option<&crate::packet_memory::PacketDriverMetadataView>,
) -> io::Error {
    // Preserve non-contention IO/native causes. Reading metadata must never
    // overwrite a close-unknown source or turn it into a successful fallback.
    if source.kind() != io::ErrorKind::WouldBlock {
        return source;
    }
    let active_owner = match material::read(owner_path, metadata) {
        Ok(value) => std::str::from_utf8(&value)
            .ok()
            .map(|owner| owner.trim().to_owned())
            .filter(|owner| !owner.is_empty())
            .unwrap_or_else(|| "unknown".to_owned()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => "unknown".to_owned(),
        Err(source) => return source,
    };
    io::Error::new(io::ErrorKind::AlreadyExists,format!("TUN route transaction for `{owner}` conflicts with active owner `{active_owner}` (`{}`): {source}",lock_path.display()))
}
fn remove_owner(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source),
    }
}

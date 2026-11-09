//! Existing library file responsibility and the actual Source's owned lock.
use super::*;
#[derive(Debug)]
pub(in crate::route) struct RouteLease {
    pub(super) journal_path: PathBuf,
    owner_path: PathBuf,
    pub(super) _lock: Lock,
}
#[derive(Debug)]
pub(super) enum Lock {
    Library(std::fs::File),
    #[cfg(target_os = "macos")]
    Owned(crate::macos_privilege::MacosRouteLock),
}
impl RouteLease {
    pub(in crate::route) fn acquire(recovery_key: &str, ipv6: bool) -> io::Result<Self> {
        Self::acquire_family(
            route_journal_path(recovery_key, ipv6)?,
            route_family_lock_path(ipv6)?,
            recovery_key,
            ipv6,
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
        Self::acquire_family(journal_path, lock_path, owner, false)
    }
    fn acquire_family(
        journal_path: PathBuf,
        lock_path: PathBuf,
        owner: &str,
        ipv6: bool,
    ) -> io::Result<Self> {
        let owner_path = lock_owner_path(&lock_path);
        let _ = ipv6;
        #[cfg(target_os = "macos")]
        let owned = crate::macos_privilege::journal_lock(&lock_path, ipv6);
        #[cfg(target_os = "macos")]
        if let Some(lock) = owned {
            let lock = lock.map_err(|source| conflict(source, &owner_path, &lock_path, owner))?;
            let _context = lock.context()?;
            persist_lock_owner(&owner_path, owner)?;
            return Ok(Self {
                journal_path,
                owner_path,
                _lock: Lock::Owned(lock),
            });
        }
        // No actual Kernel context: this remains the explicit library caller.
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|source| conflict(source, &owner_path, &lock_path, owner))?;
        persist_lock_owner(&owner_path, owner)?;
        Ok(Self {
            journal_path,
            owner_path,
            _lock: Lock::Library(lock),
        })
    }
    #[cfg(target_os = "macos")]
    pub(super) fn source(&self) -> Option<crate::macos_privilege::MacosRouteFileAccess> {
        match &self._lock {
            Lock::Owned(lock) => Some(lock.access()),
            Lock::Library(_) => None,
        }
    }
    #[cfg(target_os = "macos")]
    pub(super) fn context(&self) -> io::Result<Option<crate::MacosHelperContext>> {
        self.source().map(|source| source.context()).transpose()
    }
    #[cfg(target_os = "macos")]
    pub(super) fn owned_closed(&self) -> bool {
        matches!(&self._lock, Lock::Owned(lock) if lock.is_closed())
    }
    #[cfg(target_os = "macos")]
    pub(super) fn close_owned(&mut self) -> io::Result<()> {
        let Lock::Owned(lock) = &mut self._lock else {
            return Ok(());
        };
        if lock.is_closed() {
            return Ok(());
        }
        let _context = lock.context()?;
        remove_owner(&self.owner_path)?;
        lock.close_retained()
    }
}
impl Drop for RouteLease {
    fn drop(&mut self) {
        if let Lock::Library(lock) = &self._lock {
            if let Err(error) = remove_owner(&self.owner_path) {
                tracing::warn!(%error,"failed to remove TUN route lease owner metadata");
            }
            if let Err(error) = fs2::FileExt::unlock(lock) {
                tracing::warn!(%error,"failed to unlock TUN route lease");
            }
        }
        // Owned Drop only returns the lease. Its same Session keeps the actual
        // FD until its original duty consumes explicit close, including errors.
    }
}
#[cfg(test)]
impl Lock {
    pub(super) fn try_clone(&self) -> io::Result<std::fs::File> {
        match self {
            Self::Library(lock) => lock.try_clone(),
            #[cfg(target_os = "macos")]
            Self::Owned(_) => Err(io::Error::other(
                "Kernel route lock cannot duplicate its original",
            )),
        }
    }
}
fn conflict(source: io::Error, owner_path: &Path, lock_path: &Path, owner: &str) -> io::Error {
    // Preserve non-contention IO/native causes. Reading metadata must never
    // overwrite a close-unknown source or turn it into a successful fallback.
    if source.kind() != io::ErrorKind::WouldBlock {
        return source;
    }
    let active_owner = match material::read(owner_path) {
        Ok(value) => String::from_utf8(value)
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

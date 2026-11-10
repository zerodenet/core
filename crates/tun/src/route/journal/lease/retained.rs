//! Persistent original installation precedes native open/flock and all queries.
use super::*;
use crate::route::RouteFileOriginals;
impl RouteLease {
    pub(in crate::route) fn acquire_retained(
        files: &mut RouteFileOriginals,
        recovery_key: &str,
        ipv6: bool,
        memory: &crate::packet_memory::PacketDriverMetadataView,
    ) -> io::Result<()> {
        let journal_path = route_journal_path(recovery_key, ipv6)?;
        let lock_path = route_family_lock_path(ipv6)?;
        Self::acquire_retained_paths(files, journal_path, lock_path, recovery_key, ipv6, memory)
    }
    pub(in crate::route) fn acquire_retained_paths(
        files: &mut RouteFileOriginals,
        journal_path: PathBuf,
        lock_path: PathBuf,
        recovery_key: &str,
        ipv6: bool,
        memory: &crate::packet_memory::PacketDriverMetadataView,
    ) -> io::Result<()> {
        if files.pending.is_some() {
            return Err(io::Error::other(
                "previous route File still requires its original close",
            ));
        }
        let attempt = files.faults.begin(memory).map_err(io::Error::other)?;
        #[cfg(target_os = "macos")]
        let has_session = crate::macos_privilege::has_owned_context();
        #[cfg(not(target_os = "macos"))]
        let has_session = false;
        let file_io = if has_session {
            None
        } else {
            Some(
                crate::route::file_originals::RouteFileIo::prepare(files.faults.clone(), memory)
                    .map_err(io::Error::other)?,
            )
        };
        let owner_path = lock_owner_path(&lock_path);
        let tracked = crate::route::file_originals::TrackedFile::pending(
            files.faults.clone(),
            attempt.clone(),
        );
        files.pending = Some(Self {
            journal_path,
            owner_path,
            _lock: Lock::Retained(tracked),
            metadata: Some(memory.clone()),
            file_io,
            failures: Some((files.faults.clone(), attempt)),
            owner_attempted: false,
            owner_retired: false,
            close_reported: false,
        });
        let lease = files.pending.as_mut().unwrap();
        let result = lease.acquire_original(&lock_path, recovery_key, ipv6);
        // Error never extracts the persistent pending original. The caller's
        // real cleanup drives this same lease, even if no guard was installed.
        match result {
            Ok(()) => Ok(()),
            Err(source) => {
                let reply = source
                    .raw_os_error()
                    .map(io::Error::from_raw_os_error)
                    .unwrap_or_else(|| io::Error::from(source.kind()));
                let (faults, attempt) = lease.failures.as_ref().unwrap();
                faults.retain(attempt, 0, source);
                Err(reply)
            }
        }
    }
    fn acquire_original(&mut self, path: &Path, owner: &str, ipv6: bool) -> io::Result<()> {
        let _ = ipv6;
        #[cfg(target_os = "macos")]
        if crate::macos_privilege::has_owned_context() {
            // The Session's exact family original is installed before open.
            self._lock = Lock::PendingOwned(None);
            let Lock::PendingOwned(original) = &mut self._lock else {
                unreachable!()
            };
            crate::macos_privilege::journal_lock_retained(path, ipv6, original)
                .ok_or_else(|| io::Error::other("original route helper context disappeared"))??;
            let lock = original.take().expect("actual acquired route lock");
            self._lock = Lock::Owned(lock);
            let _context = self.context()?;
            self.owner_attempted = true;
            return material::write_from(
                &self.owner_path,
                safe_recovery_key(owner).as_bytes(),
                self.file_io.as_ref().map(|io| &**io),
            );
        }
        let Lock::Retained(original) = &mut self._lock else {
            unreachable!()
        };
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        original.install(file); // before flock, owner write, query or journal
        fs2::FileExt::try_lock_exclusive(original.file()?)?;
        original.locked();
        original.publishing_owner();
        material::write_from(
            &self.owner_path,
            safe_recovery_key(owner).as_bytes(),
            self.file_io.as_ref().map(|io| &**io),
        )
    }
    pub(in crate::route) fn physically_closed(&self) -> bool {
        if self.file_io.as_ref().is_some_and(|io| !io.closed()) {
            return false;
        }
        self.lock_closed()
    }
    fn lock_closed(&self) -> bool {
        match &self._lock {
            Lock::Retained(file) => file.closed(),
            Lock::Existing(_) => false,
            #[cfg(target_os = "macos")]
            Lock::Owned(lock) => lock.is_closed(),
            #[cfg(target_os = "macos")]
            Lock::PendingOwned(lock) => lock.as_ref().is_none_or(|lock| lock.is_closed()),
        }
    }
    pub(in crate::route) fn close_retained(&mut self) -> io::Result<()> {
        let transient = self.file_io.as_ref().map(|io| io.close()).unwrap_or(Ok(()));
        let lock = if self.lock_closed() {
            Ok(())
        } else {
            self.close_lock()
        };
        transient.and(lock)
    }
    fn close_lock(&mut self) -> io::Result<()> {
        match &mut self._lock {
            Lock::Retained(file) => {
                file.retire_owner(&self.owner_path);
                file.close()
            }
            // Genuine independent library path retains its existing behavior.
            Lock::Existing(_) => Ok(()),
            #[cfg(target_os = "macos")]
            Lock::Owned(lock) => {
                if self.failures.is_none() {
                    let _context = lock.context()?;
                    remove_owner(&self.owner_path)?;
                    return lock.close_retained();
                }
                // Metadata unlink is independent from actual descriptor close.
                if self.owner_attempted && !self.owner_retired {
                    self.owner_retired = true;
                    let context = lock.context();
                    if let Ok(_context) = context {
                        if let Err(source) = remove_owner(&self.owner_path) {
                            if let Some((faults, attempt)) = &self.failures {
                                faults.retain(attempt, 1, source)
                            }
                        }
                    }
                }
                match lock.close_retained() {
                    Ok(()) => Ok(()),
                    Err(source) => {
                        let closed = lock.is_closed();
                        let reply = source
                            .raw_os_error()
                            .map(io::Error::from_raw_os_error)
                            .unwrap_or_else(|| io::Error::from(source.kind()));
                        if !self.close_reported {
                            self.close_reported = true;
                            if let Some((faults, attempt)) = &self.failures {
                                faults.retain(attempt, 3, source)
                            }
                        }
                        if closed {
                            Ok(())
                        } else {
                            Err(reply)
                        }
                    }
                }
            }
            #[cfg(target_os = "macos")]
            Lock::PendingOwned(Some(lock)) => {
                let result = lock.close_retained();
                if lock.is_closed() {
                    Ok(())
                } else {
                    result
                }
            }
            #[cfg(target_os = "macos")]
            Lock::PendingOwned(None) => Ok(()),
        }
    }
}

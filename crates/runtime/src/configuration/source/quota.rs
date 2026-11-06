//! Durable quota bytes belong to the same deployment source; the Engine owns
//! accounting and its actual persistence worker, never an original file handle.
mod lock_error;
mod worker;
use super::*;
use std::{
    fs::{OpenOptions, TryLockError},
    sync::{Arc, Weak},
};
use zero_platform_tokio::file::{OriginalFile, OriginalFileClosed};

#[derive(Default)]
pub(super) struct QuotaSlot(Mutex<Option<Arc<QuotaStorage>>>);
pub struct QuotaStorage {
    source: Weak<ConfigurationSource>,
    path: PathBuf,
    lease: Mutex<Lease>,
    worker: worker::WorkerReceiver,
}
#[derive(Default)]
struct Lease {
    original: Option<OriginalFile>,
    acquired: bool,
    opening: bool,
    closed: Option<OriginalFileClosed>,
    fault: Option<Arc<io::Error>>,
}
impl std::fmt::Debug for QuotaStorage {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("QuotaStorage")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
struct RetainUnknownOnUnwind(Arc<ConfigurationSource>);
impl Drop for RetainUnknownOnUnwind {
    fn drop(&mut self) {
        if std::thread::panicking() {
            super::super::startup::retain_unknown(self.0.clone());
        }
    }
}
#[derive(Debug)]
struct RetainedQuotaSource(Arc<io::Error>);
impl std::fmt::Display for RetainedQuotaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}
impl std::error::Error for RetainedQuotaSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(self.0.as_ref()) }
}
fn retained(source: &Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), RetainedQuotaSource(source.clone()))
}
#[derive(Debug)]
struct AcquisitionCleanupFailure {
    acquisition: io::Error,
    cleanup: io::Error,
}
impl std::fmt::Display for AcquisitionCleanupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}; original quota lease close also failed: {}",
            self.acquisition, self.cleanup
        )
    }
}
impl std::error::Error for AcquisitionCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.acquisition)
    }
}
fn lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".zero.lock");
    PathBuf::from(value)
}
impl ConfigurationSource {
    /// Retain this exact source when its actual quota owner cannot finish
    /// cleanup. This is a debt, never a worker-join or native-close receipt.
    pub fn retain_quota_cleanup(self: &Arc<Self>) {
        super::super::startup::retain_unknown(self.clone());
    }

    /// Decode a quota snapshot through this source's actual short-file slot.
    /// The bounded BufReader preserves streaming preflight for large files;
    /// neither the native handle nor a writer lease escapes to the Engine.
    /// Domain decode errors return only after the original close is confirmed.
    pub fn read_quota_snapshot<T>(
        self: &Arc<Self>,
        path: &Path,
        decode: impl FnOnce(&mut dyn std::io::Read) -> io::Result<T>,
    ) -> io::Result<(u64, io::Result<T>)> {
        self.inspect_file_snapshot(path,decode)
    }

    /// Streaming inspection by the actual quota inspector and CLI binary hash.
    /// The decoder only borrows Read; no original handle escapes, and results
    /// return only after a matching native close on this same source's slot.
    pub fn inspect_file_snapshot<T>(
        self: &Arc<Self>,
        path: &Path,
        decode: impl FnOnce(&mut dyn std::io::Read) -> io::Result<T>,
    ) -> io::Result<(u64, io::Result<T>)> {
        super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available()?;
            self.io.with_file(
                || std::fs::File::open(path),
                |original| {
                    let bytes = original.metadata()?.len();
                    let mut reader = std::io::BufReader::new(original);
                    Ok((bytes, decode(&mut reader)))
                },
            )
        })?
    }

    /// Installs the sole cold quota child before opening or trying its native
    /// lease. A second accounting registry cannot borrow an active lease.
    pub fn open_quota_storage(
        self: &Arc<Self>,
        path: impl Into<PathBuf>,
    ) -> io::Result<Arc<QuotaStorage>> {
        let path = path.into();
        super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available()?;
            let mut slot = self
                .quota
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(previous) = slot.as_ref() {
                previous.confirm_closed()?;
                if let Some(failure) = previous.report_worker_failure() {
                    return Err(failure);
                }
            }
            let storage = Arc::new(QuotaStorage {
                source: Arc::downgrade(self),
                path,
                lease: Mutex::new(Lease::default()),
                worker: Default::default(),
            });
            *slot = Some(storage.clone());
            drop(slot);
            storage.acquire()?;
            Ok(storage)
        })?
    }
    pub(in crate::configuration) fn quota_io_available(&self) -> io::Result<()> {
        let storage = self
            .quota
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(storage) = storage {
            storage.ensure_known()?;
        }
        Ok(())
    }
    /// Called after the actual quota worker has joined. It never infers task
    /// completion or closes a native lease on behalf of a running worker.
    pub fn confirm_quota_storage_closed(&self) -> io::Result<()> {
        let storage = self
            .quota
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(storage) = storage {
            storage.confirm_closed()?;
        }
        Ok(())
    }
}
impl QuotaStorage {
    pub fn path(&self) -> &Path {
        &self.path
    }
    fn ensure_known(&self) -> io::Result<()> {
        let lease = self.lease.lock().map_err(|_| {
            io::Error::other("quota original lease operation panicked; disposition unknown")
        })?;
        if let Some(fault) = &lease.fault {
            return Err(retained(fault));
        }
        if lease.opening {
            return Err(io::Error::other(
                "quota original lease acquisition interrupted",
            ));
        }
        Ok(())
    }
    fn confirm_closed(&self) -> io::Result<()> {
        self.ensure_known()?;
        let lease = self.lease.lock().unwrap();
        match &lease.original {
            Some(original)
                if lease
                    .closed
                    .as_ref()
                    .is_some_and(|receipt| original.matches_closed(receipt)) =>
            {
                Ok(())
            }
            None if !lease.acquired => Ok(()),
            _ => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "quota original lease remains active",
            )),
        }
    }
    fn acquire(&self) -> io::Result<()> {
        let mut lease = self.lease.lock().unwrap();
        lease.opening = true;
        let result = (|| {
            let path = lock_path(&self.path);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)?;
            lease.original = Some(OriginalFile::adopt(file));
            match lease.original.as_ref().unwrap().try_lock() {
                Ok(()) => { lease.acquired = true; Ok(()) }
                Err(TryLockError::WouldBlock) => Err(io::Error::new(io::ErrorKind::WouldBlock, format!("principal quota state `{}` is already owned by another Zero process (lock `{}`)", self.path.display(), path.display()))),
                Err(TryLockError::Error(error)) => Err(lock_error::retain(&self.path, &path, error)),
            }
        })();
        lease.opening = false;
        if let Err(opening) = result {
            if let Err(cleanup) = Self::close_original(&mut lease) {
                return Err(io::Error::new(
                    opening.kind(),
                    AcquisitionCleanupFailure {
                        acquisition: opening,
                        cleanup,
                    },
                ));
            }
            return Err(opening);
        }
        Ok(())
    }
    fn close_original(lease: &mut Lease) -> io::Result<()> {
        if let Some(fault) = &lease.fault {
            return Err(retained(fault));
        }
        let Some(original) = lease.original.as_mut() else {
            return Ok(());
        };
        match original.close() {
            Ok(receipt) if original.matches_closed(&receipt) => {
                lease.closed = Some(receipt);
                lease.acquired = false;
                Ok(())
            }
            outcome => {
                let fault = Arc::new(outcome.err().unwrap_or_else(|| {
                    io::Error::other("quota original lease close receipt mismatched")
                }));
                lease.fault = Some(fault.clone());
                Err(retained(&fault))
            }
        }
    }
    pub fn close_lease(&self) -> io::Result<()> {
        let result = self
            .lease
            .lock()
            .map_err(|_| {
                io::Error::other("quota original lease operation panicked; disposition unknown")
            })
            .and_then(|mut lease| Self::close_original(&mut lease));
        if result.is_err() {
            if let Some(source) = self.source.upgrade() {
                super::super::startup::retain_unknown(source);
            }
        }
        result
    }
    fn with_source<T>(
        &self,
        operation: impl FnOnce(&ConfigurationSource) -> io::Result<T>,
    ) -> io::Result<T> {
        let source = self.source.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "quota deployment source retired",
            )
        })?;
        let _retain_on_unwind = RetainUnknownOnUnwind(source.clone());
        let lease = self.lease.lock().map_err(|_| {
            io::Error::other("quota original lease operation panicked; disposition unknown")
        })?;
        if let Some(fault) = &lease.fault {
            return Err(retained(fault));
        }
        if !lease.acquired || lease.opening {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "quota original lease is not active",
            ));
        }
        // The lease guard spans this synchronous operation, so explicit close
        // cannot expose the durable path to a competing process mid-write.
        let result = operation(&source);
        drop(lease);
        if source.io.ensure_available().is_err() {
            super::super::startup::retain_unknown(source);
        }
        result
    }
    pub fn read(&self) -> io::Result<Option<Vec<u8>>> {
        self.with_source(|source| files::read_optional(&self.path, &source.io))
    }
    /// Stream the durable book under the same actual lease and source IO
    /// close barrier. Failed/missing reads do not construct another file owner.
    pub fn read_with<T>(
        &self,
        decode: impl FnOnce(&mut dyn std::io::Read) -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        self.with_source(|source| {
            let result = source.io.with_file(
                || std::fs::File::open(&self.path),
                |original| {
                    let mut reader = std::io::BufReader::new(original);
                    decode(&mut reader)
                },
            );
            match result {
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound
                        && source.io.ensure_available().is_ok() =>
                {
                    Ok(None)
                }
                result => result.map(Some),
            }
        })
    }

    pub fn replace(&self, bytes: &[u8]) -> io::Result<()> {
        self.with_source(|source| {
            let parent = files::parent(&self.path);
            std::fs::create_dir_all(parent)?;
            files::atomic_write(&self.path, bytes, None, &source.io)
        })
    }
}

#[cfg(test)]
#[path = "../../tests/configuration_quota.rs"]
mod tests;

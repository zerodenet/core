//! Explicit library ownership or one actual hosted Default journal binding.
use super::*;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use zero_platform_tokio::journal::{JournalOwner, JournalSpec};
pub(crate) struct FakeIpStateLease {
    pub(super) path: PathBuf,
    pub(super) journal: Mutex<JournalState>,
    binding: Mutex<Option<JournalGrant>>,
    standalone: Option<JournalOwner>,
    pauses: AtomicUsize,
    io_running: AtomicBool,
}
#[derive(Default)]
pub(super) struct JournalState {
    pub generation: u64,
    pub ready: bool,
    pub records: usize,
    pub bytes: usize,
}
impl FakeIpStateLease {
    /// Genuine explicit library builder. Production prepares metadata only.
    pub(crate) fn acquire(path: PathBuf) -> io::Result<Arc<Self>> {
        Self::acquire_bounded(path, 8 * 1024 * 1024)
    }
    pub(crate) fn acquire_bounded(path: PathBuf, max_bytes: usize) -> io::Result<Arc<Self>> {
        let max_bytes = NonZeroUsize::new(max_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP persistence needs a finite nonzero envelope",
            )
        })?;
        let owner = JournalOwner::standalone(JournalSpec {
            path: path.clone(),
            max_bytes,
        })?;
        let lease = Arc::new(Self {
            path,
            binding: Mutex::new(Some(owner.grant())),
            journal: Mutex::default(),
            standalone: Some(owner),
            pauses: AtomicUsize::new(0),
            io_running: AtomicBool::new(false),
        });
        if let Err(work) = lease.standalone.as_ref().unwrap().start() {
            return Err(io::Error::new(
                work.kind(),
                FakeIpStateOpenFailure {
                    work: Arc::new(work),
                    lease,
                },
            ));
        }
        Ok(lease)
    }
    pub(crate) fn retain_open_failure(self: &Arc<Self>, work: io::Error) -> io::Error {
        if self.standalone.is_none()
            || work.get_ref().is_some_and(|source| {
                source
                    .downcast_ref::<FakeIpStateOpenFailure>()
                    .is_some_and(|failure| Arc::ptr_eq(&failure.lease, self))
            })
        {
            return work;
        }
        io::Error::new(
            work.kind(),
            FakeIpStateOpenFailure {
                work: Arc::new(work),
                lease: self.clone(),
            },
        )
    }
    pub(crate) fn hosted(path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            path,
            binding: Mutex::default(),
            journal: Mutex::default(),
            standalone: None,
            pauses: AtomicUsize::new(0),
            io_running: AtomicBool::new(false),
        })
    }
    pub(crate) fn bind(&self, grant: JournalGrant) -> io::Result<()> {
        let _call = self.begin()?;
        if self.standalone.is_some() || grant.path() != self.path {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Fake-IP journal does not match the actual hosted path",
            ));
        }
        let mut journal = self
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut binding = self
            .binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(old) = binding.as_ref() {
            if old.same_original(&grant) {
                return Ok(());
            }
            if !old.native_closed() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "old Fake-IP journal originals remain live",
                ));
            }
        }
        journal
            .generation
            .checked_add(1)
            .ok_or_else(generation_exhausted)?;
        let retired = binding.replace(grant);
        journal.ready = false;
        drop(binding);
        drop(journal);
        drop(retired);
        Ok(())
    }
    /// One concrete synchronous operation. Metadata locks never cross its IO,
    /// external encoder/reader, native notification or original Drop.
    pub(super) fn begin(&self) -> io::Result<JournalCall<'_>> {
        self.io_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "same Fake-IP journal operation remains live",
                )
            })?;
        Ok(JournalCall(&self.io_running))
    }
    pub(crate) fn ensure_next_generation(&self) -> io::Result<()> {
        let _call = self.begin()?;
        self.journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
            .checked_add(1)
            .ok_or_else(generation_exhausted)?;
        Ok(())
    }
    pub(crate) fn grant(&self) -> io::Result<JournalGrant> {
        self.binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "hosted Fake-IP journal is not started",
                )
            })
    }
    pub(crate) fn bound(&self) -> bool {
        self.binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
    pub(super) fn matches(&self, grant: &JournalGrant) -> bool {
        self.binding
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|actual| actual.same_original(grant))
    }
    pub(crate) fn close_standalone(&self) -> io::Result<bool> {
        match &self.standalone {
            Some(owner) => Ok(owner
                .try_close()?
                .is_some_and(|receipt| owner.matches_closed(&receipt))),
            None => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "hosted journal close belongs to its original Host",
            )),
        }
    }
}
impl Drop for FakeIpStateLease {
    fn drop(&mut self) {
        // Compatibility emergency disposal for the genuine library owner only.
        // No receipt escapes Drop and production custody is retained by Host.
        if let Some(owner) = &self.standalone {
            let _ = owner.try_close();
        }
    }
}
#[derive(Debug)]
pub struct JournalGenerationExhausted;
impl std::fmt::Display for JournalGenerationExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Fake-IP journal generation exhausted")
    }
}
impl std::error::Error for JournalGenerationExhausted {}
pub(super) fn generation_exhausted() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, JournalGenerationExhausted)
}

pub(crate) struct JournalPause(Arc<FakeIpStateLease>);
impl FakeIpStateLease {
    pub(in crate::fake_ip) fn pause(self: &Arc<Self>) -> io::Result<JournalPause> {
        self.pauses
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "Fake-IP journal pause count exhausted",
                )
            })?;
        Ok(JournalPause(self.clone()))
    }
    pub(in crate::fake_ip) fn paused(&self) -> bool {
        self.pauses.load(Ordering::Acquire) != 0
    }
}
impl Drop for JournalPause {
    fn drop(&mut self) {
        self.0.pauses.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct JournalCall<'a>(&'a AtomicBool);
impl Drop for JournalCall<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Genuine library open failure retaining its exact originals until explicit
/// cleanup; production never substitutes this owner for a Host declaration.
#[derive(Clone)]
pub struct FakeIpStateOpenFailure {
    work: Arc<io::Error>,
    lease: Arc<FakeIpStateLease>,
}
impl FakeIpStateOpenFailure {
    pub fn work(&self) -> &Arc<io::Error> {
        &self.work
    }
    pub fn try_close(&self) -> io::Result<bool> {
        self.lease.close_standalone()
    }
}
impl std::fmt::Debug for FakeIpStateOpenFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeIpStateOpenFailure")
            .field("path", &self.lease.path)
            .field("work", &self.work)
            .finish()
    }
}
impl std::fmt::Display for FakeIpStateOpenFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.work.kind() == io::ErrorKind::AlreadyExists {
            write!(
                f,
                "Fake-IP state `{}` is already owned by another Zero process (`{}`): {}",
                self.lease.path.display(),
                self.lease.path.with_extension("lock").display(),
                self.work
            )
        } else {
            self.work.fmt(f)
        }
    }
}
impl std::error::Error for FakeIpStateOpenFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.work.as_ref())
    }
}

//! The same Source's two family lock originals, never a file-owner registry.
use super::*;
use std::{fs::File, mem::ManuallyDrop, os::fd::FromRawFd, path::Path};
mod acquire;
mod encoding;
mod transient;
#[derive(Default)]
pub(super) struct Files {
    locks: [Lock; 2],
}
#[derive(Default)]
struct Lock {
    original: Option<Original>,
    serial: u64,
    held: bool,
    unlock: Option<Result<(), i32>>,
}
pub(crate) struct MacosRouteLock {
    owner: PacketMetadataWeak<Session>,
    family: usize,
    serial: u64,
}
impl std::fmt::Debug for MacosRouteLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MacosRouteLock")
            .field("family", &self.family)
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(crate) struct MacosRouteFileAccess {
    owner: PacketMetadataWeak<Session>,
    family: usize,
    serial: u64,
}
impl std::fmt::Debug for MacosRouteFileAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MacosRouteFileAccess")
            .field("family", &self.family)
            .finish_non_exhaustive()
    }
}
impl MacosRouteFileAccess {
    pub(crate) fn context(&self) -> io::Result<MacosHelperContext> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("actual file owner expired"))?;
        {
            let files = owner
                .files
                .lock()
                .unwrap_or_else(|source| source.into_inner());
            let slot = &files.locks[self.family];
            if slot.serial != self.serial
                || slot
                    .original
                    .as_ref()
                    .is_none_or(|original| !original.usable())
            {
                return Err(io::Error::other(
                    "route file no longer grants its original cleanup context",
                ));
            }
        }
        MacosHelperOwner(owner).enter_mode(true)
    }
}
impl MacosRouteLock {
    pub(crate) fn access(&self) -> MacosRouteFileAccess {
        MacosRouteFileAccess {
            owner: self.owner.clone(),
            family: self.family,
            serial: self.serial,
        }
    }
    pub(crate) fn context(&self) -> io::Result<MacosHelperContext> {
        self.access().context()
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.owner.upgrade().is_some_and(|owner| {
            owner
                .files
                .lock()
                .unwrap_or_else(|source| source.into_inner())
                .locks
                .get(self.family)
                .is_some_and(|slot| {
                    slot.serial == self.serial
                        && slot.original.as_ref().is_none_or(Original::closed)
                })
        })
    }
    pub(crate) fn close_retained(&mut self) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("actual route lock owner expired before receipt"))?;
        let _notifications = owner.memory.defer_notifications();
        let mut files = owner
            .files
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        let slot = &mut files.locks[self.family];
        if slot.serial != self.serial {
            return Err(io::Error::other(
                "route lock belongs to another actual acquisition",
            ));
        }
        slot.held = false;
        let [unlock, close] = close_lock(slot);
        let failed = unlock.is_some() || close.is_some();
        drop(files);
        owner.retain_file_outcomes(self.family, unlock, close);
        if failed {
            Err(failures::retained(owner.failures.clone().into_error()))
        } else {
            Ok(())
        }
    }
}
impl Drop for MacosRouteLock {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            {
                let mut files = owner
                    .files
                    .lock()
                    .unwrap_or_else(|source| source.into_inner());
                let slot = &mut files.locks[self.family];
                if slot.serial == self.serial {
                    slot.held = false;
                }
            }
            // Returning a lease requests actual close; it is never that close.
            owner.notify();
        }
    }
}
fn close_lock(slot: &mut Lock) -> [Option<io::Error>; 2] {
    let Some(original) = slot.original.as_mut() else {
        return [None, None];
    };
    if original.closed() {
        return [None, None];
    }
    // Never unlock or close a handle after an uncertain original close.
    let unlock = if original.usable() {
        let result = *slot.unlock.get_or_insert_with(|| {
            let borrowed = unsafe { ManuallyDrop::new(File::from_raw_fd(original.fd())) };
            fs2::FileExt::unlock(&*borrowed)
                .map_err(|source| source.raw_os_error().unwrap_or(libc::EIO))
        });
        result.err().map(io::Error::from_raw_os_error)
    } else {
        None
    };
    [unlock, original.close_once().err()]
}
impl Session {
    pub(super) fn close_returned_route_files(&self) {
        // One original at a time; no temporary snapshot/history allocation.
        for family in 0..2 {
            let outcomes = {
                let mut files = self
                    .files
                    .lock()
                    .unwrap_or_else(|source| source.into_inner());
                if files.locks[family].held {
                    continue;
                }
                close_lock(&mut files.locks[family])
            };
            let [unlock, close] = outcomes;
            self.retain_file_outcomes(family, unlock, close);
        }
    }
    fn retain_file_outcomes(
        &self,
        family: usize,
        unlock: Option<io::Error>,
        close: Option<io::Error>,
    ) {
        if let Some(source) = unlock {
            self.failures.retain(
                0,
                if family == 0 {
                    Phase::RouteUnlockV4
                } else {
                    Phase::RouteUnlockV6
                },
                source,
            );
        }
        if let Some(source) = close {
            self.retain_file_failure(family, source);
        }
    }
    pub(super) fn route_files_closed(&self) -> bool {
        self.files.try_lock().is_ok_and(|files| {
            files
                .locks
                .iter()
                .all(|slot| slot.original.as_ref().is_none_or(Original::closed))
        })
    }
    fn retain_file_failure(&self, family: usize, source: io::Error) {
        self.failures.retain(
            0,
            if family == 0 {
                Phase::RouteLockV4
            } else {
                Phase::RouteLockV6
            },
            source,
        );
    }
}
pub(crate) fn journal_lock(path: &Path, ipv6: bool) -> Option<io::Result<MacosRouteLock>> {
    current().map(|owner| owner.acquire_route_lock(path, ipv6))
}
/// Same current Source only; metadata cannot create or select a helper.
pub(crate) fn has_owned_context() -> bool {
    current().is_some()
}
pub(crate) fn journal_lock_retained(
    path: &Path,
    ipv6: bool,
    original: &mut Option<MacosRouteLock>,
) -> Option<io::Result<()>> {
    current().map(|owner| owner.acquire_route_lock_retained(path, ipv6, original))
}
pub(crate) fn journal_read(
    path: &Path,
) -> Option<io::Result<crate::packet_memory::PacketMetadataVec<u8>>> {
    current().map(|owner| owner.read_file(path))
}
pub(crate) fn journal_encode(
    value: &impl serde::Serialize,
) -> Option<io::Result<crate::packet_memory::PacketMetadataVec<u8>>> {
    current().map(|owner| owner.encode_journal(value))
}
pub(crate) fn journal_write(path: &Path, bytes: &[u8]) -> Option<io::Result<()>> {
    current().map(|owner| owner.write_file(path, bytes))
}

#[cfg(test)]
#[path = "files/tests.rs"]
mod tests;

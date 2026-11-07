//! Exactly root DIR, current pid/fd DIR, and one table/comm File, not a pool.
use super::*;
use crate::file::OriginalFile;
use std::{fs::OpenOptions, io::Read, os::unix::fs::OpenOptionsExt, path::Path};
pub(super) const ROOT: usize = 0;
pub(super) const DESCRIPTORS: usize = 1;
pub(super) const FILE: usize = 2;
#[derive(Default)]
pub(in crate::process) struct Originals {
    slots: [Option<OriginalFile>; 3],
    opening: [bool; 3],
    close_failure: Option<Arc<ChannelError>>,
    operation_failure: Option<Arc<ChannelError>>,
    pub(super) closed: usize,
}
impl Originals {
    pub(super) fn open(
        &mut self,
        slot: usize,
        path: &Path,
        directory: bool,
    ) -> std::io::Result<()> {
        if self.slots[slot].is_some() || self.close_failure.is_some() || self.opening[slot] {
            return Err(std::io::Error::other(
                "same metadata original has not confirmed close",
            ));
        }
        self.opening[slot] = true;
        let flags =
            libc::O_CLOEXEC | libc::O_NONBLOCK | if directory { libc::O_DIRECTORY } else { 0 };
        let opened = OpenOptions::new().read(true).custom_flags(flags).open(path);
        match opened {
            Ok(file) => {
                // The sole original is installed before the first metadata,
                // read or getdents operation. Nothing exports/duplicates it.
                self.slots[slot] = Some(OriginalFile::adopt(file));
                self.opening[slot] = false;
                Ok(())
            }
            Err(source) => {
                self.opening[slot] = false;
                Err(source)
            }
        }
    }
    pub(super) fn close(&mut self, slot: usize) -> Result<(), Arc<ChannelError>> {
        let Some(original) = self.slots[slot].as_mut() else {
            return if self.opening[slot] {
                Err(cause(Stage::Close, Reason::Io))
            } else {
                Ok(())
            };
        };
        match original.close() {
            Ok(receipt) if original.matches_closed(&receipt) => {
                self.slots[slot] = None;
                self.closed += 1;
                Ok(())
            }
            Ok(_) => Err(cause(Stage::Close, Reason::StalePlan)),
            Err(source) => {
                let source = self
                    .close_failure
                    .get_or_insert_with(|| {
                        Arc::new(
                            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                                .with_source(CloseFailure {
                                    close: Arc::new(source),
                                    operation: self.operation_failure.clone(),
                                }),
                        )
                    })
                    .clone();
                Err(source)
            }
        }
    }
    pub(in crate::process) fn close_all(&mut self) -> Result<(), Arc<ChannelError>> {
        let mut first = None;
        for slot in [FILE, DESCRIPTORS, ROOT] {
            if let Err(source) = self.close(slot) {
                first.get_or_insert(source);
            }
        }
        first.map_or(Ok(()), Err)
    }
    pub(in crate::process) fn confirmed(&self) -> bool {
        self.close_failure.is_none()
            && !self.opening.iter().any(|opening| *opening)
            && self.slots.iter().all(Option::is_none)
    }
    pub(super) fn retain_operation_failure(&mut self, source: Arc<ChannelError>) {
        self.operation_failure.get_or_insert(source);
    }
    pub(in crate::process) fn failure(&self) -> Option<Arc<ChannelError>> {
        self.close_failure.clone()
    }
    pub(super) fn read(&mut self, slot: usize, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.slots[slot]
            .as_mut()
            .ok_or_else(|| std::io::Error::other("metadata original absent"))?
            .read(bytes)
    }
    pub(super) fn directory(&mut self, slot: usize, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.slots[slot]
            .as_mut()
            .ok_or_else(|| std::io::Error::other("metadata directory original absent"))?
            .read_linux_directory_entries(bytes)
    }
}

#[derive(Debug)]
struct CloseFailure {
    close: Arc<std::io::Error>,
    operation: Option<Arc<ChannelError>>,
}
impl std::fmt::Display for CloseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.operation {
            Some(operation) => write!(
                f,
                "metadata operation {operation}; original close {}",
                self.close
            ),
            None => self.close.fmt(f),
        }
    }
}
impl std::error::Error for CloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.close.as_ref())
    }
}

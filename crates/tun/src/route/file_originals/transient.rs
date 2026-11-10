//! One existing journal read/write FD, retained by the same family lease.
use super::{RouteFileFaults, TrackedFile};
use crate::packet_memory::{
    PacketDriverMetadataView, PacketMemoryError, PacketMetadataArc, PacketMetadataVec,
};
use std::{
    fs::OpenOptions,
    io::{self, Read, Write},
    path::Path,
    sync::Mutex,
};
struct State {
    file: Option<TrackedFile>,
    retiring: bool,
}
pub(in crate::route) struct RouteFileIo {
    state: Mutex<State>,
    faults: RouteFileFaults,
    memory: PacketDriverMetadataView,
}
impl std::fmt::Debug for RouteFileIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RouteFileIo")
    }
}
impl RouteFileIo {
    pub(in crate::route) fn prepare(
        faults: RouteFileFaults,
        memory: &PacketDriverMetadataView,
    ) -> Result<PacketMetadataArc<Self>, PacketMemoryError> {
        let credit = memory.reserve(PacketMetadataArc::<Self>::layout()?)?;
        PacketMetadataArc::from_prepaid(
            Self {
                state: Mutex::new(State {
                    file: None,
                    retiring: false,
                }),
                faults,
                memory: memory.clone(),
            },
            credit,
        )
        .map_err(|(e, _, _)| e)
    }
    pub(in crate::route) fn closed(&self) -> bool {
        self.state.try_lock().is_ok_and(|state| {
            state.retiring && state.file.as_ref().is_none_or(TrackedFile::closed)
        })
    }
    pub(in crate::route) fn close(&self) -> io::Result<()> {
        let notifications = self.memory.notifications();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.retiring = true;
        let result = state
            .file
            .as_mut()
            .map(TrackedFile::close)
            .unwrap_or(Ok(()));
        let retired = if result.is_ok() {
            state.file.take()
        } else {
            None
        };
        drop(state);
        drop(retired);
        drop(notifications);
        result
    }
    pub(in crate::route) fn read(&self, path: &Path) -> io::Result<PacketMetadataVec<u8>> {
        self.run(path, None)
    }
    pub(in crate::route) fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.run(path, Some(bytes)).map(drop)
    }
    fn run(&self, path: &Path, input: Option<&[u8]>) -> io::Result<PacketMetadataVec<u8>> {
        // All quota, Arc/Waker construction precedes owner locking/native IO.
        let attempt = self.faults.begin(&self.memory).map_err(io::Error::other)?;
        let mut candidate = Some(TrackedFile::pending(self.faults.clone(), attempt.clone()));
        let notifications = self.memory.notifications();
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.retiring || state.file.is_some() {
            drop(state);
            drop(candidate);
            drop(notifications);
            return Err(io::Error::other(
                "journal File still owes its original close",
            ));
        }
        state.file = candidate.take(); // before open, actual failed original persists
        let opened = if input.is_some() {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)
        } else {
            std::fs::File::open(path)
        };
        let result = match opened {
            Err(source) => Err(source),
            Ok(file) => {
                let original = state.file.as_mut().unwrap();
                original.install(file);
                match input {
                    Some(bytes) => {
                        let mut file = original.file().expect("same installed transient File");
                        file.write_all(bytes).map(|()| PacketMetadataVec::default())
                    }
                    None => read_all(
                        original.file().expect("same installed transient File"),
                        &self.memory,
                    ),
                }
            }
        };
        let closed = state.file.as_mut().unwrap().close();
        // These cells were paid before opening. Both errors survive, with no
        // admission needed during close or while a policy fence is paused.
        let result = match result {
            Err(source) => {
                let reply = source
                    .raw_os_error()
                    .map(io::Error::from_raw_os_error)
                    .unwrap_or_else(|| io::Error::from(source.kind()));
                // An open NotFound is the journal's ordinary absent-state fact.
                // A real read/write failure is retained even if close succeeds.
                if source.kind() != io::ErrorKind::NotFound {
                    self.faults.retain(&attempt, 0, source)
                }
                Err(reply)
            }
            Ok(value) => match &closed {
                Ok(()) => Ok(value),
                Err(source) => Err(source
                    .raw_os_error()
                    .map(io::Error::from_raw_os_error)
                    .unwrap_or_else(|| io::Error::from(source.kind()))),
            },
        };
        let retired = if closed.is_ok() {
            state.file.take()
        } else {
            None
        };
        drop(state);
        drop(retired);
        drop(notifications);
        result
    }
}
fn read_all(
    mut file: &std::fs::File,
    memory: &PacketDriverMetadataView,
) -> io::Result<PacketMetadataVec<u8>> {
    let mut result = PacketMetadataVec::default();
    loop {
        let mut scratch = [0u8; 4096];
        let count = match file.read(&mut scratch) {
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Ok(result);
        }
        for &byte in &scratch[..count] {
            result
                .try_push_with_metadata(byte, memory, usize::MAX)
                .map_err(|(e, _)| io::Error::other(e))?;
        }
    }
}

#[cfg(test)]
mod tests;

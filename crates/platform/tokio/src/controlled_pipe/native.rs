//! Only this actual worker submits/cancels these original OVERLAPPED objects.
use super::*;
mod create;
mod operations;
mod panic;
pub(super) use panic::split as split_panic;
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF, ERROR_IO_INCOMPLETE, ERROR_IO_PENDING,
        ERROR_NOT_FOUND, ERROR_OPERATION_ABORTED, HANDLE, INVALID_HANDLE_VALUE, WAIT_FAILED,
        WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    Storage::FileSystem::{ReadFile, WriteFile},
    System::{
        Threading::{ResetEvent, SetEvent, WaitForMultipleObjects},
        IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED},
    },
};
#[derive(Default)]
pub(super) struct Native {
    pipe: usize,
    ever_acquired: bool,
    events: [usize; 4],
    connect: Operation,
    read: Operation,
    write: Operation,
    faults: Vec<Arc<io::Error>>,
    close_faults: [Option<Arc<io::Error>>; 5],
}
struct Operation {
    overlapped: Box<OVERLAPPED>,
    pending: bool,
    bytes: Vec<u8>,
    offset: usize,
}
impl Default for Operation {
    fn default() -> Self {
        Self {
            overlapped: Box::new(OVERLAPPED::default()),
            pending: false,
            bytes: Vec::new(),
            offset: 0,
        }
    }
}
// SAFETY: Original OVERLAPPED/buffers never move while submitted. The sole
// original worker holds Native's mutex throughout run/drain; close is legal
// only after that exact worker/std/Scope join and unique view return.
unsafe impl Send for Native {}
fn handle(raw: usize) -> HANDLE {
    raw as HANDLE
}
pub(super) fn signal(event: usize) -> io::Result<()> {
    if unsafe { SetEvent(handle(event)) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
impl Native {
    pub(super) fn release_workspaces(&mut self) -> (Vec<u8>, Vec<u8>) {
        debug_assert!(!self.read.pending && !self.write.pending);
        (
            std::mem::take(&mut self.read.bytes),
            std::mem::take(&mut self.write.bytes),
        )
    }
    pub(super) fn acquired(&self) -> bool {
        self.ever_acquired
    }
    pub(super) fn run(&mut self, shared: &Shared) -> io::Result<()> {
        let work = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.drive(shared)))
            .map(|result| result.map_err(Arc::new));
        if let Ok(Err(error)) = &work {
            lock(&shared.state)
                .fault
                .get_or_insert_with(|| error.clone());
        }
        // Stop all new operations before cancelling. All buffers/OVERLAPPED
        // remain owned here until actual completion, including unwind paths.
        shared.stop.store(true, Ordering::Release);
        let cleanup = self.drain();
        shared.pending.store(
            u8::from(self.connect.pending)
                | u8::from(self.read.pending) << 1
                | u8::from(self.write.pending) << 2,
            Ordering::Release,
        );
        {
            let mut state = lock(&shared.state);
            if state.writing || !state.write.is_empty() {
                state.fault.get_or_insert_with(|| {
                    Arc::new(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "original pending pipe output was cancelled before flush",
                    ))
                });
            }
            state.writing = false;
            state.eof = true;
            state.write.clear();
            if state.startup.is_none() {
                state.startup = Some(Err(Arc::new(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "pipe stopped during creation",
                ))));
            }
        }
        shared.notify_io();
        match work {
            Err(payload) => panic::resume(payload, cleanup),
            Ok(Err(error)) => {
                self.faults.push(error);
                self.fault_result()
            }
            Ok(Ok(())) => cleanup,
        }
    }
    fn drive(&mut self, shared: &Shared) -> io::Result<()> {
        if shared.stop.load(Ordering::Acquire) || shared.scope_stop.is_cancelled() {
            return Ok(());
        }
        if let Err(error) = self.create(shared) {
            let error = Arc::new(error);
            lock(&shared.state).startup = Some(Err(error.clone()));
            shared.notify_io();
            return Err(retained(&error));
        }
        lock(&shared.state).startup = Some(Ok(()));
        shared.notify_io();
        self.start_connect(shared)?;
        shared.notify_io();
        loop {
            #[cfg(test)]
            if shared.pending.load(Ordering::Acquire) != 0
                && shared.panic_pending.swap(false, Ordering::AcqRel)
            {
                panic!("original pipe worker fault after real native Pending");
            }
            if shared.stop.load(Ordering::Acquire) || shared.scope_stop.is_cancelled() {
                break;
            }
            let connected = lock(&shared.state).connected;
            if connected {
                self.start_read(shared)?;
                self.start_write(shared)?;
            }
            let more = {
                let state = lock(&shared.state);
                (!self.read.pending
                    && !state.eof
                    && state.read.len() < READ_SLOTS
                    && state.connected)
                    || (!self.write.pending
                        && (self.write.offset < self.write.bytes.len() || !state.write.is_empty())
                        && state.connected)
            };
            if more {
                continue;
            }
            let mut waits = [handle(self.events[3]); 4];
            let mut operations = [3; 4];
            let mut count = 1;
            for (index, pending) in [self.connect.pending, self.read.pending, self.write.pending]
                .into_iter()
                .enumerate()
            {
                if pending {
                    waits[count] = handle(self.events[index]);
                    operations[count] = index;
                    count += 1;
                }
            }
            let waited = unsafe { WaitForMultipleObjects(count as u32, waits.as_ptr(), 0, 25) };
            if waited == WAIT_TIMEOUT {
                continue;
            }
            if waited == WAIT_FAILED {
                return Err(io::Error::last_os_error());
            }
            let index = waited.wrapping_sub(WAIT_OBJECT_0) as usize;
            let Some(operation) = operations[..count].get(index).copied() else {
                return Err(io::Error::other("foreign pipe completion event"));
            };
            unsafe {
                ResetEvent(waits[index]);
            }
            match operation {
                0 => {
                    Self::complete(self.pipe, &mut self.connect, false)?;
                    shared.pending.fetch_and(!1, Ordering::AcqRel);
                    lock(&shared.state).connected = true;
                    shared.notify_io();
                }
                1 => self.finish_read(shared)?,
                2 => self.finish_write(shared)?,
                3 => {}
                _ => unreachable!(),
            }
        }
        Ok(())
    }
    fn fault_result(&self) -> io::Result<()> {
        if self.faults.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(PipeNativeFailures(self.faults.clone())))
        }
    }
    pub(super) fn close(&mut self) -> io::Result<()> {
        if self.connect.pending || self.read.pending || self.write.pending {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "original pipe overlapped IO is not complete",
            ));
        }
        for (index, original) in std::iter::once(&mut self.pipe)
            .chain(self.events.iter_mut())
            .enumerate()
        {
            if *original == 0 {
                continue;
            }
            if let Some(unknown) = &self.close_faults[index] {
                // A failed CloseHandle is not a license to act on a possibly
                // reused numeric handle. Preserve this exact unknown original.
                return Err(retained(unknown));
            }
            if unsafe { CloseHandle(handle(*original)) } == 0 {
                let error = Arc::new(io::Error::last_os_error());
                // Five fixed original handle positions bound cleanup debt.
                // Keep the first close fault for this still-held handle.
                self.close_faults[index].get_or_insert_with(|| error.clone());
                return Err(retained(&error));
            }
            *original = 0;
        }
        Ok(())
    }
}

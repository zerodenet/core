//! The original SDK reader's own reversible wake event, not a watcher/thread.
mod native;
use crate::packet_memory::NativeReadGrant;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
};
use tokio::sync::Notify;
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, WAIT_FAILED, WAIT_OBJECT_0},
    System::Threading::{CreateEventW, SetEvent, WaitForMultipleObjects, INFINITE},
};
#[derive(Clone)]
pub struct OwnedWintunReadControl(Arc<Control>);
struct Control {
    state: Mutex<State>,
    changed: Notify,
    blocking: Condvar,
    paused: AtomicBool,
    grant: NativeReadGrant,
    owner: std::sync::Weak<super::super::OwnedWintunOwner>,
}
struct State {
    event: Option<usize>,
    stopped: bool,
    serial: u64,
    request: Option<u64>,
    acknowledged: Option<u64>,
    closed: bool,
    close_unknown: Option<Arc<io::Error>>,
    signal_fault: Option<Arc<io::Error>>,
}
pub struct OwnedWintunReadPause {
    control: OwnedWintunReadControl,
    request: u64,
}
impl OwnedWintunReadControl {
    pub(super) fn new(
        grant: NativeReadGrant,
        owner: std::sync::Weak<super::super::OwnedWintunOwner>,
    ) -> Self {
        Self(Arc::new(Control {
            state: Mutex::new(State {
                event: None,
                stopped: false,
                serial: 0,
                request: None,
                acknowledged: None,
                closed: false,
                close_unknown: None,
                signal_fault: None,
            }),
            changed: Notify::new(),
            blocking: Condvar::new(),
            paused: AtomicBool::new(false),
            grant,
            owner,
        }))
    }
    /// Called only inside the actual admitted reader body, after its host has
    /// registered that original blocking task. Preparation remains non-native.
    pub(super) fn activate(&self) -> io::Result<bool> {
        let mut state = self.state();
        if let Some(source) = &state.close_unknown {
            return Err(super::super::shared(source.clone()));
        }
        if state.stopped || state.closed {
            return Ok(false);
        }
        if state.event.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "same reader wake event already acquired",
            ));
        }
        // SAFETY: the result is this task's unique unnamed event, immediately
        // retained in its original state before any other fallible operation.
        let event = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        state.event = Some(event as usize);
        Ok(true)
    }
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn signal(&self) -> io::Result<()> {
        let mut state = self.state();
        if let Some(source) = &state.close_unknown {
            return Err(super::super::shared(source.clone()));
        }
        let Some(raw) = state.event else {
            // Before this same task activates, there is no native event wait.
            // The session/grant Condvars below still wake its real bootstrap.
            return if state.closed {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "original reader wake event is closed",
                ))
            } else {
                Ok(())
            };
        };
        // SAFETY: this mutex serializes the same original event with close;
        // SetEvent only wakes its real wait, never asserts a payload or receipt.
        let result = unsafe { SetEvent(raw as HANDLE) };
        if result == 0 {
            let source = Arc::new(io::Error::last_os_error());
            state.signal_fault.get_or_insert_with(|| source.clone());
            Err(super::super::shared(source))
        } else {
            Ok(())
        }
    }
    pub async fn pause(&self) -> io::Result<OwnedWintunReadPause> {
        let request = {
            let mut state = self.state();
            if state.stopped || state.closed || state.request.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "same actual reader is stopped or already paused",
                ));
            }
            state.serial = state
                .serial
                .checked_add(1)
                .ok_or_else(|| io::Error::other("reader pause identity exhausted"))?;
            let request = state.serial;
            state.request = Some(request);
            state.acknowledged = None;
            self.0.paused.store(true, Ordering::Release);
            request
        };
        let pause = OwnedWintunReadPause {
            control: self.clone(),
            request,
        };
        self.signal()?;
        self.wake_original();
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state();
                if state.stopped {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "same actual reader stopped while pausing",
                    ));
                }
                if state.request == Some(request) && state.acknowledged == Some(request) {
                    return Ok(pause);
                }
            }
            changed.await;
        }
    }
    fn wake_original(&self) {
        self.0.grant.wake();
        self.0.blocking.notify_all();
        if let Some(owner) = self.0.owner.upgrade() {
            drop(owner.state());
            owner.reader_ready.notify_all();
        }
    }
    pub(super) fn checkpoint(&self) -> bool {
        let mut state = self.state();
        loop {
            if state.stopped {
                return false;
            }
            let Some(request) = state.request else {
                return true;
            };
            state.acknowledged = Some(request);
            drop(state);
            self.0.changed.notify_waiters();
            state = self.state();
            if state.request == Some(request) && !state.stopped {
                state = self
                    .0
                    .blocking
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }
    pub(super) fn paused(&self) -> &AtomicBool {
        &self.0.paused
    }
    pub(in crate::windows::owned) fn stop(&self) -> io::Result<()> {
        {
            let mut state = self.state();
            state.stopped = true;
        }
        let result = self.signal();
        self.wake_original();
        self.0.changed.notify_waiters();
        result
    }
    pub(super) fn finished(&self) {
        self.state().stopped = true;
        self.wake_original();
        self.0.changed.notify_waiters();
    }
    /// Wait on the borrowed SDK data event and this original control event.
    /// The SDK event is never closed/reset by this owner.
    pub(super) fn wait(&self, session: &wintun::Session) -> io::Result<()> {
        let event = self.wait_handle()?;
        let read = session.get_read_wait_event().map_err(io::Error::from)?;
        let handles = [read.0 as HANDLE, event as HANDLE];
        // SAFETY: both originals outlive this actual reader; the host consumes
        // its real Scope join before it can close this control or the SDK session.
        let result =
            unsafe { WaitForMultipleObjects(handles.len() as u32, handles.as_ptr(), 0, INFINITE) };
        if result == WAIT_FAILED {
            return Err(io::Error::last_os_error());
        }
        if result != WAIT_OBJECT_0 && result != WAIT_OBJECT_0 + 1 {
            return Err(io::Error::other(
                "unexpected original Wintun read wait result",
            ));
        }
        Ok(())
    }
    fn wait_handle(&self) -> io::Result<usize> {
        let state = self.state();
        if let Some(source) = &state.close_unknown {
            return Err(super::super::shared(source.clone()));
        }
        state.event.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original reader wake event closed",
            )
        })
    }
    pub fn signal_failure(&self) -> Option<Arc<io::Error>> {
        self.state().signal_fault.clone()
    }
}
impl Drop for OwnedWintunReadPause {
    fn drop(&mut self) {
        {
            let mut state = self.control.state();
            if state.request == Some(self.request) {
                state.request = None;
                state.acknowledged = None;
                self.control.0.paused.store(false, Ordering::Release);
            }
        }
        self.control.0.blocking.notify_all();
        self.control.0.grant.wake();
        self.control.0.changed.notify_waiters();
    }
}

impl OwnedWintunReadPause {
    /// After the real thread acknowledged its pause, outstanding queued payloads
    /// still keep the same IO credit. They must not be evicted for replacement.
    pub fn payload_returned(&self) -> bool {
        self.control.0.grant.workspace_returned()
    }
}

#[cfg(test)]
mod tests;

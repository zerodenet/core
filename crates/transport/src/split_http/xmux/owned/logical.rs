//! One admitted logical stream's real work return, independent of physical N.
use super::*;
use std::sync::Mutex;
use tokio::sync::Notify;
#[derive(Clone)]
pub struct OwnedLogicalReturn {
    original: PacketMetadataArc<State>,
    life: crate::split_http::io::LifetimeRef,
}
struct State {
    gate: Mutex<Count>,
    changed: Notify,
    // Stream and original work both pin this credit until their real endpoints drop.
    _duplex: zero_platform_tokio::controlled_carrier::CarrierBufferCredit,
}
struct Count {
    sealed: bool,
    active: usize,
    upload_started: bool,
    upload_returned: bool,
}
pub(in crate::split_http) struct WorkReturn {
    original: PacketMetadataArc<State>,
    life: crate::split_http::io::LifetimeRef,
    upload: bool,
}
impl OwnedLogicalReturn {
    pub(in crate::split_http) fn prepare(
        memory: &PacketDriverMetadataView,
        life: crate::split_http::io::LifetimeRef,
        duplex: zero_platform_tokio::controlled_carrier::CarrierBufferCredit,
    ) -> io::Result<Self> {
        let credit = memory
            .reserve(PacketMetadataArc::<State>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let original = PacketMetadataArc::from_prepaid(
            State {
                gate: Mutex::new(Count {
                    sealed: false,
                    active: 0,
                    upload_started: false,
                    upload_returned: false,
                }),
                changed: Notify::new(),
                _duplex: duplex,
            },
            credit,
        )
        .map_err(|(source, _, _)| io::Error::other(source))?;
        Ok(Self { original, life })
    }
    pub(in crate::split_http) fn enter(&self) -> io::Result<WorkReturn> {
        self.enter_work(false)
    }
    pub(super) fn enter_upload(&self) -> io::Result<WorkReturn> {
        self.enter_work(true)
    }
    fn enter_work(&self, upload: bool) -> io::Result<WorkReturn> {
        let mut state = self
            .original
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed || self.life.is_closed() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if upload && state.upload_started {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| io::Error::other("logical work count exhausted"))?;
        state.upload_started |= upload;
        Ok(WorkReturn {
            original: self.original.clone(),
            life: self.life.clone(),
            upload,
        })
    }
    /// Stops only this logical body. Real Session/Request work must still return.
    /// This neither retires its shared physical group nor acknowledges its Scope.
    pub fn request_return(&self) {
        let mut state = self
            .original
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed {
            return;
        }
        state.sealed = true;
        drop(state);
        self.life.close();
        self.original.changed.notify_waiters();
    }
    pub(super) async fn cancelled(&self) {
        self.life.cancelled().await;
    }
    pub(super) async fn wait_upload_returned(&self) {
        loop {
            let changed = self.original.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .original
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .upload_returned
            {
                return;
            }
            changed.await;
        }
    }
    pub fn is_returned(&self) -> bool {
        let state = self
            .original
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sealed && state.active == 0
    }
    pub async fn wait_returned(&self) {
        loop {
            let changed = self.original.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_returned() {
                return;
            }
            changed.await;
        }
    }
}
impl WorkReturn {
    pub(super) fn request_return(&self) {
        OwnedLogicalReturn {
            original: self.original.clone(),
            life: self.life.clone(),
        }
        .request_return();
    }
}
impl Drop for WorkReturn {
    fn drop(&mut self) {
        let mut state = self
            .original
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.active > 0);
        state.active -= 1;
        if self.upload {
            state.upload_returned = true;
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "../../../../tests/split_http/owned_lifetime.rs"]
mod tests;

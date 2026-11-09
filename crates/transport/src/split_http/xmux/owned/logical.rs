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
}
pub(super) struct WorkReturn(PacketMetadataArc<State>);
impl OwnedLogicalReturn {
    pub(super) fn prepare(
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
                }),
                changed: Notify::new(),
                _duplex: duplex,
            },
            credit,
        )
        .map_err(|(source, _, _)| io::Error::other(source))?;
        Ok(Self { original, life })
    }
    pub(super) fn enter(&self) -> io::Result<WorkReturn> {
        let mut state = self
            .original
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed || self.life.is_closed() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| io::Error::other("logical work count exhausted"))?;
        Ok(WorkReturn(self.original.clone()))
    }
    /// Stops only this logical body. Real Session/Request work must still return.
    /// This neither retires its shared physical group nor acknowledges its Scope.
    pub fn request_return(&self) {
        self.original
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sealed = true;
        self.life.close();
        self.original.changed.notify_waiters();
    }
    pub(super) async fn cancelled(&self) {
        self.life.cancelled().await;
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
impl Drop for WorkReturn {
    fn drop(&mut self) {
        let mut state = self
            .0
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.active > 0);
        state.active -= 1;
        drop(state);
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "../../../../tests/split_http/owned_lifetime.rs"]
mod tests;

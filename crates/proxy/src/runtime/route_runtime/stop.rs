//! Cooperative consumer stop precedes aborting generic accepted TCP tasks.
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use zero_platform_tokio::contract::CancellationToken;

#[derive(Default)]
struct State {
    closing: bool,
    consumers: usize,
}
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Notify,
    stop: CancellationToken,
}
#[derive(Clone, Default)]
pub(crate) struct ListenerStop(Arc<Shared>);

pub(crate) struct UdpStopGuard {
    stop: ListenerStop,
}
impl ListenerStop {
    pub(crate) fn claim(&self) -> Result<UdpStopGuard, zero_engine::EngineError> {
        let mut state = self.0.state.lock().expect("listener stop poisoned");
        if state.closing {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "listener stopped before UDP consumer admission",
            )
            .into());
        }
        state.consumers = state
            .consumers
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("listener UDP consumer count exhausted"))?;
        Ok(UdpStopGuard { stop: self.clone() })
    }
    pub(crate) fn begin_shutdown(&self) {
        self.0.state.lock().expect("listener stop poisoned").closing = true;
        self.0.stop.cancel();
    }
    /// This confirms only that admitted consumer futures released their guards.
    /// The persistent Datagram owner must still confirm actual channel closure.
    pub(crate) async fn wait_quiescent(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .0
                .state
                .lock()
                .expect("listener stop poisoned")
                .consumers
                == 0
            {
                return;
            }
            changed.await;
        }
    }
}
impl UdpStopGuard {
    pub(crate) async fn cancelled(&self) {
        self.stop.0.stop.cancelled().await;
    }
}
impl Drop for UdpStopGuard {
    fn drop(&mut self) {
        let mut state = self.stop.0.state.lock().expect("listener stop poisoned");
        state.consumers -= 1;
        drop(state);
        self.stop.0.changed.notify_waiters();
    }
}

#[cfg(test)]
#[path = "stop_tests.rs"]
mod tests;

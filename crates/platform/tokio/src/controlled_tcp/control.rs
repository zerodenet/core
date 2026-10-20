use super::{socket::Socket, Shared, TokioSocket};
use crate::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use std::{io, sync::Arc};
use tokio::time::{sleep_until, Duration, Instant};

/// Retains the same original socket after I/O cancellation or close interruption.
/// A receipt covers this descriptor, not TLS notification or remote delivery.
pub struct ControlledTcpControl {
    pub(super) shared: Arc<Shared>,
    pub(super) reported: bool,
}
enum CloseAction {
    Closed,
    Unfinished,
    Fault(Arc<io::Error>),
    Original(Socket),
    Retry(Arc<io::Error>),
}
impl Shared {
    fn close_action(&self, deadline: Instant) -> CloseAction {
        // Prepare the unconfirmed disposition before taking the original. If
        // deregistration, socket conversion or close unwinds, it stays retained.
        let unknown = Arc::new(io::Error::other(
            "original TCP close did not return an observed disposition",
        ));
        let mut state = self.state();
        if state.closed {
            return CloseAction::Closed;
        }
        if state.polling {
            return CloseAction::Unfinished;
        }
        if state.closing {
            return CloseAction::Fault(
                state
                    .unknown
                    .as_ref()
                    .expect("inflight disposition installed")
                    .clone(),
            );
        }
        if let Some(unknown) = &state.unknown {
            return CloseAction::Fault(unknown.clone());
        }
        if state.retry.is_none() {
            if let Some(source) = &state.error {
                return CloseAction::Fault(source.clone());
            }
        }
        if Instant::now() >= deadline {
            return CloseAction::Unfinished;
        }
        // The inflight marker cannot be mistaken for successful empty ownership
        // by another/reentrant close observer. There is no await in this window.
        state.closing = true;
        state.unknown = Some(unknown);
        if let Some(socket) = state.socket.take() {
            return CloseAction::Original(socket);
        }
        if let Some(source) = state.retry.take() {
            return CloseAction::Retry(source);
        }
        CloseAction::Fault(state.unknown.as_ref().expect("installed unknown").clone())
    }
    fn close_returned(&self, result: io::Result<bool>) {
        // No original descriptor or external source object is dropped under the
        // state lock. The first real fault Arc stays stable across known-open
        // Windows retries; only that retry's exact retained SOCKET may be used.
        let result = result.map_err(Arc::new);
        let (unknown, old_retry, unused_error, confirmed) = {
            let mut state = self.state();
            state.closing = false;
            let unknown = state.unknown.take();
            let old_retry = state.retry.take();
            let unused_error = match result {
                Ok(true) => {
                    state.closed = true;
                    None
                }
                Ok(false) => {
                    // A retry with no known-open original is still unconfirmed.
                    state.unknown = unknown.clone();
                    None
                }
                Err(source) => {
                    state.error.get_or_insert_with(|| source.clone());
                    if TokioSocket::close_is_retryable(&source) {
                        state.retry = Some(source);
                        None
                    } else {
                        Some(source)
                    }
                }
            };
            (unknown, old_retry, unused_error, state.closed)
        };
        drop((unknown, old_retry, unused_error));
        if confirmed {
            let observer = self
                .observer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            // Native close disposition is already durable, even if this external
            // observer destructor re-enters control or panics.
            drop(observer);
        }
    }
}
impl ChannelControl for ControlledTcpControl {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        loop {
            match self.shared.close_action(deadline) {
                CloseAction::Closed => {
                    let mut report = CloseReport::default();
                    if !self.reported {
                        report.closed_io.push(self.shared.label.clone());
                        self.reported = true;
                    }
                    return Ok(report);
                }
                CloseAction::Unfinished => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io(self.shared.label.clone())],
                        ..Default::default()
                    })
                }
                CloseAction::Fault(source) => {
                    let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Session)
                        .with_source(source);
                    error.delivery = DeliveryState::Unknown;
                    return Err(error);
                }
                CloseAction::Original(socket) => {
                    self.shared.close_returned(socket.close().map(|()| true))
                }
                CloseAction::Retry(source) => self
                    .shared
                    .close_returned(TokioSocket::retry_close(&source)),
            }
            // Cancellation/deadline never disposes of a known-open original.
            // Native/observer/waker operations above and this timer run lock-free.
            let retry = { self.shared.state().retry.is_some() };
            if retry {
                sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
            }
        }
    }
}
impl Drop for ControlledTcpControl {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

#[cfg(test)]
#[path = "../../tests/controlled_tcp/close_disposition.rs"]
mod tests;

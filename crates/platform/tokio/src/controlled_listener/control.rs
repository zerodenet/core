use super::*;
use crate::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use tokio::time::{sleep_until, Duration, Instant};

/// Owns confirmation for the original listener only, never its accepted sockets.
/// Tokio deregistration failures consume the handle with an unconfirmed native
/// outcome; they remain permanent errors even if the port later appears free.
pub struct ControlledTcpListenerControl {
    pub(super) shared: Arc<Shared>,
    pub(super) reported: bool,
}
impl ChannelControl for ControlledTcpListenerControl {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel_admission();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        loop {
            {
                let mut state = self
                    .shared
                    .state
                    .lock()
                    .expect("TCP listener owner poisoned");
                if !state.closed {
                    if let Some(source) = &state.error {
                        if !native::retryable(source) {
                            return Err(failure(source.clone()));
                        }
                    }
                    if Instant::now() >= deadline {
                        return Ok(CloseReport {
                            unfinished: vec![Unfinished::Resource(self.shared.label.clone())],
                            ..Default::default()
                        });
                    }
                    if let Some(listener) = state.listener.take() {
                        // Tokio 1.54 PollEvented::into_inner takes the native IO
                        // before deregistering. On Err it drops that IO. Keep the
                        // original error permanently: native close is unproven,
                        // and the consumed descriptor must never be reconstructed.
                        let result = listener.into_std().and_then(native::close);
                        match result {
                            Ok(()) => state.closed = true,
                            Err(source) => state.error = Some(Arc::new(source)),
                        }
                    } else if let Some(source) = &state.error {
                        match native::retry(source) {
                            Ok(true) => {
                                state.closed = true;
                                state.error = None;
                            }
                            Ok(false) => {}
                            Err(source) => state.error = Some(Arc::new(source)),
                        }
                    }
                }
                if state.closed {
                    let mut report = CloseReport::default();
                    if !self.reported {
                        report.closed_io.push(self.shared.label.clone());
                        self.reported = true;
                    }
                    return Ok(report);
                }
                if let Some(source) = &state.error {
                    if !native::retryable(source) {
                        return Err(failure(source.clone()));
                    }
                }
            }
            // Only a known-open Winsock outcome reaches this await. Shared
            // retains its exact SOCKET and source if the waiting future is lost.
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
        }
    }
}
fn failure(source: Arc<io::Error>) -> ChannelError {
    let mut error =
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(source);
    error.delivery = DeliveryState::Unknown;
    error
}
impl Drop for ControlledTcpListenerControl {
    fn drop(&mut self) {
        self.shared.cancel_admission();
    }
}

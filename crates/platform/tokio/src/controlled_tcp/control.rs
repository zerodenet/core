use super::{Shared, TokioSocket};
use crate::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use std::sync::Arc;
use tokio::time::{sleep_until, Duration, Instant};

/// Retains the same original socket after I/O cancellation or close interruption.
/// A native close receipt covers this descriptor, not TLS close_notify, remote
/// business delivery, duplicated external handles, or a whole component graph.
pub struct ControlledTcpControl {
    pub(super) shared: Arc<Shared>,
    pub(super) reported: bool,
}
impl ChannelControl for ControlledTcpControl {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        loop {
            {
                let mut state = self.shared.state.lock().expect("TCP owner poisoned");
                if !state.closed {
                    if let Some(source) = &state.error {
                        if !TokioSocket::close_is_retryable(source) {
                            let mut failure =
                                ChannelError::new(Stage::Close, Reason::Io, Impact::Session)
                                    .with_source(source.clone());
                            failure.delivery = DeliveryState::Unknown;
                            return Err(failure);
                        }
                    }
                    if Instant::now() >= deadline {
                        return Ok(CloseReport {
                            unfinished: vec![Unfinished::Io(self.shared.label.clone())],
                            ..Default::default()
                        });
                    }
                    if let Some(socket) = state.socket.take() {
                        match socket.close() {
                            Ok(()) => state.closed = true,
                            Err(source) => state.error = Some(Arc::new(source)),
                        }
                    } else if let Some(source) = state.error.clone() {
                        // Only Winsock's documented known-open outcome is
                        // retryable. Unix errors never reconstruct/retry an fd.
                        match TokioSocket::retry_close(&source) {
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
                    if !TokioSocket::close_is_retryable(source) {
                        let mut failure =
                            ChannelError::new(Stage::Close, Reason::Io, Impact::Session)
                                .with_source(source.clone());
                        failure.delivery = DeliveryState::Unknown;
                        return Err(failure);
                    }
                }
            }
            // The owned known-open SOCKET and original error remain in Shared
            // if this wait is dropped. A later caller can continue confirmation.
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
        }
    }
}
impl Drop for ControlledTcpControl {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

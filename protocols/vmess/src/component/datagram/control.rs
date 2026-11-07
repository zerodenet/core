use super::*;
use std::{future::poll_fn, pin::Pin, task::Poll};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    CloseReason,
};
pub struct Control {
    shared: Arc<Shared>,
    shutdown: bool,
    closed: bool,
    read_done: bool,
    failures: failure::Failures,
    pending_report: CloseReport,
}
impl Control {
    pub(in crate::component) async fn return_carrier_before(&mut self, deadline: Instant) -> bool {
        self.cancel(CloseReason::Shutdown);
        let shared = self.shared.clone();
        timeout_at(deadline, async {
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.return_carrier() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }
    /// Return logical custody only. The gRPC controller must separately consume
    /// its original driver and native close receipts before reporting closure.
    pub(in crate::component) fn return_carrier(&mut self) -> bool {
        let mut state = self.shared.state.lock().unwrap();
        if state.views != 0 {
            return false;
        }
        let stream = state.stream.take();
        drop(state);
        drop(stream);
        self.closed = true;
        true
    }
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            shutdown: false,
            closed: false,
            read_done: false,
            failures: failure::Failures::default(),
            pending_report: CloseReport::default(),
        }
    }
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel.cancel();
        self.shared.changed.notify_waiters();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if self.closed {
            return self.failures.finish(&mut self.pending_report);
        }
        let shared = self.shared.clone();
        let close = async {
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if shared.state.lock().unwrap().views == 0 {
                    break;
                }
                changed.await;
            }
            poll_fn(|cx| {
                let mut state = shared.state.lock().unwrap();
                let stream = state.stream.as_mut().expect("retained VMess UDP original");
                let mut progress = false;
                if !self.shutdown {
                    match Pin::new(&mut *stream).poll_shutdown(cx) {
                        Poll::Pending => {}
                        Poll::Ready(result) => {
                            // This original direction is terminal after its
                            // actual error. Never retry an uncertain END/write.
                            self.shutdown = true;
                            progress = true;
                            if let Err(source) = result {
                                self.failures.shutdown(
                                    shared
                                        .error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                                        .with_source(source),
                                );
                            }
                        }
                    }
                }
                if !self.read_done {
                    let mut bytes = [0; 4096];
                    let mut buf = ReadBuf::new(&mut bytes);
                    match Pin::new(stream).poll_read(cx, &mut buf) {
                        Poll::Pending => {}
                        Poll::Ready(Ok(())) => {
                            self.read_done = buf.filled().is_empty();
                            progress = true;
                        }
                        Poll::Ready(Err(source)) => {
                            self.read_done = true;
                            progress = true;
                            self.failures.read(
                                shared
                                    .error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                                    .with_source(source),
                            );
                        }
                    }
                }
                // No packet views remain. A fatal IO outcome ends this one
                // logical stream; return its view to the actual donor, whose
                // native/driver closure is independently owned by the SDK.
                if self.failures.has_failure() || (self.shutdown && self.read_done) {
                    let returned = state.stream.take();
                    drop(state);
                    drop(returned);
                    Poll::Ready(())
                } else {
                    drop(state);
                    if progress {
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending
                }
            })
            .await
        };
        match timeout_at(deadline, close).await {
            Ok(()) => {
                self.closed = true;
                self.pending_report.unfinished.clear();
                self.pending_report
                    .closed_io
                    .push("VMess UDP logical stream".into());
                self.failures.finish(&mut self.pending_report)
            }
            Err(_) => {
                self.pending_report.unfinished.clear();
                self.pending_report.unfinished.push(Unfinished::Io(
                    "VMess UDP original stream or transferred views".into(),
                ));
                self.failures.finish(&mut self.pending_report)
            }
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

mod failure;

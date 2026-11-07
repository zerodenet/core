use super::shared::Shared;
use alloc::boxed::Box;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::ReadBuf,
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, DynChannelControl, Unfinished},
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
type Closing = Pin<
    Box<
        dyn Future<
                Output = (
                    zero_platform_tokio::contract::lifecycle::BoxChannelControl,
                    Result<CloseReport, ChannelError>,
                ),
            > + Send,
    >,
>;
pub struct Control {
    shared: Arc<Shared>,
    closing: Option<Closing>,
    raw_closed: bool,
    report: CloseReport,
    reported_tcp: bool,
    original: Option<Arc<super::association::Association>>,
}
impl Control {
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            closing: None,
            raw_closed: false,
            report: CloseReport::default(),
            reported_tcp: false,
            original: None,
        }
    }
    pub(super) fn with_original(mut self, original: Arc<super::association::Association>) -> Self {
        self.original = Some(original);
        self
    }
    pub(super) fn original(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOrigin> {
        self.original
            .as_ref()
            .and_then(|original| original.origin())
    }
    pub(super) fn related_control(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierAdoption> {
        self.original.as_ref().map(|original| original.parent())
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<Result<(), ChannelError>> {
        let mut failure = None;
        let mut state = self.shared.state.lock().expect("SOCKS5 session poisoned");
        // A native association returns its unique byte view, then closes the
        // original TCP controller explicitly. EOF is neither required nor used
        // as native-close evidence. Library bare sessions keep their old FIN/EOF
        // protocol path until their original provider is supplied.
        let returned_tcp = if self.original.is_some() {
            state.tcp.take()
        } else {
            None
        };
        if self.original.is_none() && !state.tcp_shutdown {
            match state
                .tcp
                .as_mut()
                .expect("pending control stream")
                .as_mut()
                .poll_shutdown(cx)
            {
                Poll::Ready(Ok(())) => state.tcp_shutdown = true,
                Poll::Ready(Err(source)) => {
                    failure = Some(
                        self.shared
                            .error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                            .with_source(source),
                    );
                }
                Poll::Pending => {}
            }
        }
        if self.original.is_none() && !state.tcp_eof {
            let mut bytes = [0; 64];
            let mut read = ReadBuf::new(&mut bytes);
            match state
                .tcp
                .as_mut()
                .expect("pending control stream")
                .as_mut()
                .poll_read(cx, &mut read)
            {
                Poll::Ready(Ok(())) if read.filled().is_empty() => state.tcp_eof = true,
                Poll::Ready(Ok(())) => cx.waker().wake_by_ref(),
                Poll::Ready(Err(source)) => {
                    failure = Some(
                        self.shared
                            .error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                            .with_source(source),
                    );
                }
                Poll::Pending => {}
            }
        }
        if state.tcp_shutdown && state.tcp_eof && !self.reported_tcp {
            drop(state.tcp.take());
            // Both directions of this logical control stream completed. This
            // says nothing about business delivery or its shared carrier owner.
            self.report
                .closed_io
                .push("SOCKS5 UDP ASSOCIATE logical control stream".into());
            self.reported_tcp = true;
        }
        if self.closing.is_none() && !self.raw_closed {
            let mut control = state
                .raw_control
                .take()
                .expect("owned raw datagram control");
            self.closing = Some(Box::pin(async move {
                let result = control.wait_closed(deadline).await;
                (control, result)
            }));
        }
        drop(state);
        drop(returned_tcp);
        if let Some(closing) = self.closing.as_mut() {
            if let Poll::Ready((control, result)) = closing.as_mut().poll(cx) {
                self.closing = None;
                self.shared
                    .state
                    .lock()
                    .expect("SOCKS5 session poisoned")
                    .raw_control = Some(control);
                let mut report = match result {
                    Ok(report) => report,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                self.raw_closed = report.is_complete();
                report.unfinished.clear();
                self.report.append(report);
                if !self.raw_closed && Instant::now() < deadline {
                    cx.waker().wake_by_ref();
                }
            }
        }
        if let Some(error) = failure {
            return Poll::Ready(Err(error));
        }
        if self.raw_closed && (self.reported_tcp || self.original.is_some()) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn take_report(&mut self) -> CloseReport {
        let mut report = std::mem::take(&mut self.report);
        if !self.reported_tcp {
            report
                .unfinished
                .push(Unfinished::Io("SOCKS5 UDP ASSOCIATE control stream".into()));
        }
        if !self.raw_closed {
            report
                .unfinished
                .push(Unfinished::Io("SOCKS5 UDP relay carrier".into()));
        }
        report
    }
}
impl ChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.shared.stop(reason);
        if let Some(original) = &self.original {
            // The controller is the actual published association receiver.
            // Request the same TCP+UDP pair; retained Tx/Rx views and native
            // receipts still independently gate both originals' refunds.
            original.parent().request_retirement();
        }
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        match timeout_at(deadline, poll_fn(|cx| self.poll_close(cx, deadline))).await {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => {
                if let Some(original) = &self.original {
                    let report = original.close(deadline).await?;
                    if report.is_complete() {
                        self.reported_tcp = true;
                    }
                    self.report.append(report);
                }
                Ok(self.take_report())
            }
            Err(_) => Ok(self.take_report()),
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}

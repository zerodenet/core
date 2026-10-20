use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::time::{sleep_until, Duration, Instant, Sleep};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynChannelControl, Unfinished},
    ChannelError, CloseReason,
};
type Closing = Pin<
    Box<
        dyn Future<
                Output = (
                    Box<dyn DynChannelControl>,
                    Result<CloseReport, ChannelError>,
                ),
            > + Send,
    >,
>;
pub(super) struct Entry {
    pub(super) reserved: bool,
    pub(super) active: bool,
    pub(super) closed: bool,
    pub(super) control: Option<Box<dyn DynChannelControl>>,
    closing: Option<Closing>,
    retry: Option<Pin<Box<Sleep>>>,
    failure: Option<Arc<ChannelError>>,
    unfinished: Vec<Unfinished>,
    receipts: CloseReport,
}
impl Entry {
    pub(super) fn reserved() -> Self {
        Self {
            reserved: true,
            active: false,
            closed: false,
            control: None,
            closing: None,
            retry: None,
            failure: None,
            unfinished: Vec::new(),
            receipts: CloseReport::default(),
        }
    }
    pub(super) fn retire(&mut self, reason: CloseReason) {
        self.active = false;
        if let Some(control) = &self.control {
            control.cancel(reason);
        }
    }
    pub(super) fn snapshot(&mut self, id: u64, take: bool) -> CloseReport {
        let mut report = if take {
            std::mem::take(&mut self.receipts)
        } else {
            CloseReport::default()
        };
        if !self.closed {
            if self.unfinished.is_empty() {
                let kind = if self.reserved {
                    "reserved Datagram open"
                } else {
                    "retired Datagram controller"
                };
                report
                    .unfinished
                    .push(Unfinished::Io(format!("{kind} {id}")));
            } else {
                report.unfinished.extend(self.unfinished.iter().cloned());
            }
        }
        report
    }
    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<Result<(), Arc<ChannelError>>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if self.reserved || self.active {
            return Poll::Pending;
        }
        if let Some(retry) = &mut self.retry {
            if retry.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.retry = None;
        }
        if self.closing.is_none() {
            let mut control = self
                .control
                .take()
                .expect("retired actual Datagram controller");
            self.closing = Some(Box::pin(async move {
                let result = control.wait_closed(deadline).await;
                (control, result)
            }));
        }
        let Poll::Ready((control, result)) = self.closing.as_mut().unwrap().as_mut().poll(cx)
        else {
            return Poll::Pending;
        };
        self.closing = None;
        self.control = Some(control);
        match result {
            Ok(mut report) => {
                self.failure = None;
                self.closed = report.is_complete();
                self.unfinished = std::mem::take(&mut report.unfinished);
                self.receipts.append(report);
                if self.closed {
                    Poll::Ready(Ok(()))
                } else {
                    let mut retry = Box::pin(sleep_until(
                        deadline.min(Instant::now() + Duration::from_millis(10)),
                    ));
                    let _ = retry.as_mut().poll(cx);
                    self.retry = Some(retry);
                    Poll::Pending
                }
            }
            Err(error) => {
                let error = Arc::new(error);
                self.failure = Some(error.clone());
                Poll::Ready(Err(error))
            }
        }
    }
}

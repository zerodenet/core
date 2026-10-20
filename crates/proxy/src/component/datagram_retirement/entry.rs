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
    failure_pending: bool,
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
            failure_pending: false,
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
    pub(super) fn acknowledge_failure(&mut self, source: &Arc<ChannelError>) {
        if self
            .failure
            .as_ref()
            .is_some_and(|failure| Arc::ptr_eq(failure, source))
        {
            self.failure_pending = false;
        }
    }
    pub(super) fn has_receipts(&self) -> bool {
        let report = &self.receipts;
        !report.closed_io.is_empty()
            || !report.exited_tasks.is_empty()
            || !report.failed_tasks.is_empty()
            || !report.released_resources.is_empty()
            || !report.recovered_resources.is_empty()
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
        if self.failure_pending {
            return Poll::Ready(Err(self
                .failure
                .as_ref()
                .expect("actual unreported failure")
                .clone()));
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
                self.failure_pending = false;
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
                self.failure_pending = true;
                let mut retry = Box::pin(sleep_until(Instant::now() + Duration::from_millis(10)));
                let _ = retry.as_mut().poll(cx);
                self.retry = Some(retry);
                Poll::Ready(Err(error))
            }
        }
    }
}

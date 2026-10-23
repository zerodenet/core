use std::{future::Future, pin::Pin, sync::Arc};
use tokio::time::Sleep;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynChannelControl, Unfinished},
    ChannelError, CloseReason,
};
mod cancel;
mod poll;
type Control = Arc<tokio::sync::Mutex<Option<Box<dyn DynChannelControl>>>>;
type Closing = Pin<Box<dyn Future<Output = Result<CloseReport, ChannelError>> + Send>>;
pub(super) struct Entry {
    pub(super) reserved: bool,
    pub(super) active: bool,
    pub(super) closed: bool,
    pub(super) control: Option<Control>,
    pending_cancel: Option<CloseReason>,
    polling: bool,
    protocol_closed: bool,
    poll_panicked: bool,
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
            pending_cancel: None,
            polling: false,
            protocol_closed: false,
            poll_panicked: false,
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
        self.pending_cancel = Some(reason);
    }
    pub(super) fn snapshot(&mut self, id: u64, take: bool) -> CloseReport {
        // Keep terminal receipts durable until the completed controller's
        // potentially arbitrary last destructor has also returned normally.
        let take = take && (!self.protocol_closed || self.closed);
        let mut report = if take {
            std::mem::take(&mut self.receipts)
        } else {
            CloseReport::default()
        };
        if !self.closed {
            if self.poll_panicked {
                report.unfinished.push(Unfinished::Io(format!("Datagram controller {id} cleanup poll panicked; original controller remains owned")));
            } else if self.unfinished.is_empty() {
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
        if self.protocol_closed && !self.closed {
            return false;
        }
        let report = &self.receipts;
        !report.closed_io.is_empty()
            || !report.exited_tasks.is_empty()
            || !report.failed_tasks.is_empty()
            || !report.released_resources.is_empty()
            || !report.recovered_resources.is_empty()
    }
}

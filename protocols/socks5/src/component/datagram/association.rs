//! The actual TCP control and related UDP originals survive preparation exit.
use alloc::vec::Vec;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{lifecycle::CloseReport, ChannelError, CloseReason},
    controlled_carrier::{loan::LoanOwner, CarrierAdoption, CarrierOrigin},
};

#[derive(Default)]
pub(in crate::component) struct Associations {
    records: Mutex<Vec<Arc<Association>>>,
    receipts: Mutex<CloseReport>,
}
pub(super) struct Association {
    parent: CarrierAdoption,
    stop: zero_platform_tokio::contract::CancellationToken,
    children: Mutex<Vec<CarrierAdoption>>,
    tcp: Mutex<Option<Arc<tokio::sync::Mutex<LoanOwner>>>>,
    cleanup: tokio::sync::Mutex<()>,
    origin: Mutex<Option<CarrierOrigin>>,
    preparing: AtomicBool,
    published: AtomicBool,
    closed: AtomicBool,
    receipts: Mutex<CloseReport>,
}
pub(super) struct Preparing(pub(super) Arc<Association>);
impl Drop for Preparing {
    fn drop(&mut self) {
        self.0.preparing.store(false, Ordering::Release);
    }
}
impl Associations {
    pub(super) fn install(
        &self,
        parent: CarrierAdoption,
        stop: zero_platform_tokio::contract::CancellationToken,
        limit: usize,
    ) -> Result<Arc<Association>, ChannelError> {
        let mut records = self.records.lock().unwrap();
        records.retain(|record| !record.closed.load(Ordering::Acquire));
        if records.len() >= limit {
            let mut error = failure(&parent, zero_platform_tokio::contract::Reason::Capacity);
            error.stage = zero_platform_tokio::contract::Stage::Prepare;
            error.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
            return Err(error);
        }
        let original = Arc::new(Association {
            parent,
            stop,
            children: Default::default(),
            tcp: Default::default(),
            cleanup: Default::default(),
            origin: Default::default(),
            preparing: true.into(),
            published: false.into(),
            closed: false.into(),
            receipts: Default::default(),
        });
        records.push(original.clone());
        Ok(original)
    }
    pub(super) async fn reap(&self, end: Instant) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            if !record.preparing.load(Ordering::Acquire)
                && !record.published.load(Ordering::Acquire)
            {
                let report = record.close(end).await?;
                self.receipts.lock().unwrap().append(report);
            }
        }
        self.records
            .lock()
            .unwrap()
            .retain(|record| !record.closed.load(Ordering::Acquire));
        Ok(())
    }
    pub(in crate::component) fn revoke(&self) {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.stop.cancel();
        }
    }
    pub(in crate::component) async fn close(
        &self,
        end: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let records = self.records.lock().unwrap().clone();
        for record in records {
            let report = record.close(end).await?;
            self.receipts.lock().unwrap().append(report);
        }
        self.records
            .lock()
            .unwrap()
            .retain(|record| !record.closed.load(Ordering::Acquire));
        Ok(std::mem::take(&mut *self.receipts.lock().unwrap()))
    }
}
impl Association {
    pub(super) fn install_tcp(&self, owner: LoanOwner) {
        let mut actual = self.tcp.lock().unwrap();
        assert!(actual.is_none(), "the actual control is installed once");
        *actual = Some(Arc::new(tokio::sync::Mutex::new(owner)));
    }
    pub(super) fn install_child(&self, child: CarrierAdoption) {
        let mut children = self.children.lock().unwrap();
        children.retain(|actual| !matching_closed(actual));
        children.push(child);
    }
    pub(super) fn publish(&self, origin: CarrierOrigin) {
        *self.origin.lock().unwrap() = Some(origin);
        self.published.store(true, Ordering::Release);
    }
    pub(super) fn origin(&self) -> Option<CarrierOrigin> {
        self.origin.lock().unwrap().clone()
    }
    pub(super) fn parent(&self) -> CarrierAdoption {
        self.parent.clone()
    }
    pub(super) async fn close(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        let _cleanup = tokio::time::timeout_at(end, self.cleanup.lock())
            .await
            .map_err(|source| {
                failure(&self.parent, zero_platform_tokio::contract::Reason::Timeout)
                    .with_source(source)
            })?;
        if self.closed.load(Ordering::Acquire) {
            return Ok(CloseReport::default());
        }
        // Positive results are retained in this original record before another
        // close can suspend or fail. Interrupted cleanup keeps the same cells.
        let children = self.children.lock().unwrap().clone();
        for child in &children {
            let mut report = child.retire(end).await?;
            let complete = report.is_complete();
            let unfinished = std::mem::take(&mut report.unfinished);
            self.receipts.lock().unwrap().append(report);
            if !complete {
                let mut report = std::mem::take(&mut *self.receipts.lock().unwrap());
                report.unfinished.extend(unfinished);
                return Ok(report);
            }
        }
        let tcp = self.tcp.lock().unwrap().clone();
        let mut report = if let Some(tcp) = tcp {
            let mut owner = tokio::time::timeout_at(end, tcp.lock())
                .await
                .map_err(|source| {
                    failure(&self.parent, zero_platform_tokio::contract::Reason::Timeout)
                        .with_source(source)
                })?;
            owner.cancel(CloseReason::Shutdown);
            let report = owner.close(end).await?;
            if report.is_complete() {
                // The same installed logical controller produced this receipt.
                // Cancellation while awaiting it leaves this strong cell intact.
                self.tcp.lock().unwrap().take();
            }
            report
        } else {
            self.parent.retire(end).await?
        };
        let unfinished = std::mem::take(&mut report.unfinished);
        self.receipts.lock().unwrap().append(report);
        let originals = children.iter().chain(std::iter::once(&self.parent));
        let complete = unfinished.is_empty()
            && !self.preparing.load(Ordering::Acquire)
            && originals.into_iter().all(matching_closed);
        let mut report = std::mem::take(&mut *self.receipts.lock().unwrap());
        report.unfinished.extend(unfinished);
        if complete {
            self.closed.store(true, Ordering::Release);
        } else if report.is_complete() {
            report.unfinished.push(
                zero_platform_tokio::contract::lifecycle::Unfinished::ReliableState(
                    "SOCKS5 UDP association original custody".into(),
                ),
            );
        }
        Ok(report)
    }
}
fn matching_closed(original: &CarrierAdoption) -> bool {
    original
        .closed()
        .is_some_and(|closed| closed.matches(original))
        && original
            .users_closed()
            .is_some_and(|users| users.matches(original))
}
fn failure(
    original: &CarrierAdoption,
    reason: zero_platform_tokio::contract::Reason,
) -> ChannelError {
    let mut error = ChannelError::new(
        zero_platform_tokio::contract::Stage::Close,
        reason,
        zero_platform_tokio::contract::Impact::Component,
    );
    error.component = Some(original.instance().component);
    error
}

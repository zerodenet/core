use crate::protocol_registry::{UdpAssociationCloseKind, UdpAssociationObserver};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, DynChannelControl},
    ChannelError, CloseReason,
};
pub(super) struct ObservedControl {
    actual: Box<dyn DynChannelControl>,
    observer: UdpAssociationObserver,
    reason: Arc<AtomicU8>,
    reported: bool,
}
impl ObservedControl {
    pub(super) fn new(
        actual: Box<dyn DynChannelControl>,
        observer: UdpAssociationObserver,
        reason: Arc<AtomicU8>,
    ) -> Self {
        Self {
            actual,
            observer,
            reason,
            reported: false,
        }
    }
}
impl ChannelControl for ObservedControl {
    fn cancel(&self, reason: CloseReason) {
        if matches!(
            reason,
            CloseReason::DeliveryUnknown | CloseReason::ProtocolFailure
        ) {
            let _ = self
                .reason
                .compare_exchange(0, 3, Ordering::AcqRel, Ordering::Acquire);
        }
        self.actual.cancel(reason);
    }
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        let report = self.actual.wait_closed(deadline).await?;
        if report.is_complete() && !self.reported {
            self.observer
                .record_close(match self.reason.load(Ordering::Acquire) {
                    2 => UdpAssociationCloseKind::IdleTimeout,
                    3 => UdpAssociationCloseKind::Dropped,
                    _ => UdpAssociationCloseKind::Closed,
                });
            self.reported = true;
        }
        Ok(report)
    }
}

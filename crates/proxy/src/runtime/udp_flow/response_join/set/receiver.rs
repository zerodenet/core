//! One prepaid RX operation in the original association driver, not another task.
mod polling;
use super::*;
use crate::protocol_registry::packet_memory::business::BusinessMemory;
use std::{pin::Pin, task::Context};
use zero_platform_tokio::contract::{scope::TaskJoinWitness, CleanupFailureReceipt, RetainedError};
use zero_traits::packet_metadata::{
    PacketMetadataArc, PacketMetadataCredit, PacketMetadataError, PacketMetadataLayout,
    PacketMetadataWeak,
};
struct Work {
    body: Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send>>,
    // Box deallocation always precedes its actual layout credit.
    _credit: PacketMetadataCredit,
}
pub(super) struct PacketReceiver {
    work: Mutex<Option<Work>>,
    original: TaskJoinWitness,
    stop: zero_platform_tokio::contract::CancellationToken,
    complete: AtomicBool,
    panicked: AtomicBool,
    sources: PacketMetadataError,
    receipt: CleanupFailureReceipt,
    failure_credit: Mutex<Option<PacketMetadataCredit>>,
    failure: std::sync::OnceLock<RetainedError>,
    metadata: zero_platform_tokio::contract::component::PacketDriverMetadataView,
}
/// Weak publication back to this exact installed original; expiry grants no ACK.
#[derive(Clone)]
pub(in crate::runtime::udp_flow) struct WeakPacketReceiver(PacketMetadataWeak<PacketReceiver>);
impl WeakPacketReceiver {
    pub(in crate::runtime::udp_flow) fn receive_failure(&self) {
        if let Some(actual) = self.0.upgrade() {
            actual.receive_failure();
        }
    }
}
impl PacketReceiver {
    fn prepare(
        memory: &BusinessMemory,
        original: TaskJoinWitness,
        stop: zero_platform_tokio::contract::CancellationToken,
        sources: PacketMetadataError,
        body: impl Future<Output = Result<(), ChannelError>> + Send + 'static,
    ) -> Result<PacketMetadataArc<Self>, EngineError> {
        let fail = |source| EngineError::from(std::io::Error::other(source));
        let body_bytes = std::mem::size_of_val(&body);
        let body_layout = PacketMetadataLayout::new(usize::from(body_bytes != 0), body_bytes);
        let error_layout = RetainedError::metadata_layout().map_err(fail)?;
        let receipt_layout = CleanupFailureReceipt::metadata_layout().map_err(fail)?;
        let owner_layout = PacketMetadataArc::<Self>::layout().map_err(fail)?;
        let layout = body_layout
            .checked_add(error_layout)
            .and_then(|layout| layout.checked_add(receipt_layout))
            .and_then(|layout| layout.checked_add(owner_layout))
            .map_err(fail)?;
        let mut credit = memory.metadata.reserve(layout).map_err(fail)?;
        let body_credit = credit.split(body_layout).map_err(fail)?;
        let failure_credit = credit.split(error_layout).map_err(fail)?;
        let receipt_credit = credit.split(receipt_layout).map_err(fail)?;
        let receipt = CleanupFailureReceipt::from_prepaid(receipt_credit)
            .unwrap_or_else(|_| panic!("exact prepaid packet RX fault receipt"));
        assert!(receipt.bind_metadata_payload(sources.clone()));
        PacketMetadataArc::from_prepaid(
            Self {
                work: Mutex::new(Some(Work {
                    body: Box::pin(body),
                    _credit: body_credit,
                })),
                original,
                stop,
                complete: AtomicBool::new(false),
                panicked: AtomicBool::new(false),
                sources,
                receipt,
                failure_credit: Mutex::new(Some(failure_credit)),
                failure: Default::default(),
                metadata: memory.metadata.clone(),
            },
            credit,
        )
        .map_err(|(source, _, _)| fail(source))
    }
    fn receive_failure(&self) {
        let credit = self
            .failure_credit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(credit) = credit {
            let source = ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                .with_metadata_source(self.sources.clone())
                .with_cleanup_failure_receipt(self.receipt.clone());
            let source = RetainedError::from_prepaid(source, credit)
                .unwrap_or_else(|_| panic!("same original RX error layout prepaid before IO"));
            let _ = self.failure.set(source);
        }
    }
    pub(super) fn failure(&self) -> Option<RetainedError> {
        self.failure.get().cloned()
    }
    pub(super) fn stop(&self) {
        self.stop.cancel();
    }
    pub(super) fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    pub(super) async fn close(&self, deadline: Instant) -> Result<(), ChannelError> {
        self.stop();
        if self.panicked.load(Ordering::Acquire) {
            // The real Critical driver's Scope owns the panic. Preserve its Box
            // until that exact task has actually returned, never invent a join.
            timeout_at(deadline, self.original.wait())
                .await
                .map_err(timeout)?;
            let work = self
                .work
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(work);
            self.complete.store(true, Ordering::Release);
            return Ok(());
        }
        timeout_at(
            deadline,
            std::future::poll_fn(|cx| {
                self.poll(cx);
                if self.is_complete() {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }),
        )
        .await
        .map_err(timeout)
    }
}
impl ResponseSetLease {
    pub(in crate::runtime::udp_flow) fn install_packet_receiver(
        &mut self,
        memory: &BusinessMemory,
        original: TaskJoinWitness,
        stop: zero_platform_tokio::contract::CancellationToken,
        sources: PacketMetadataError,
        body: impl Future<Output = Result<(), ChannelError>> + Send + 'static,
    ) -> Result<WeakPacketReceiver, EngineError> {
        if self.owner.revoked.load(Ordering::Acquire)
            || original.is_joined()
            || original.native_task_id() != tokio::task::try_id()
            || self.owner.receiver.get().is_some()
        {
            return Err(std::io::Error::other(
                "packet RX requires its same live original association driver",
            )
            .into());
        }
        let actual = PacketReceiver::prepare(memory, original, stop, sources, body)?;
        let weak = WeakPacketReceiver(PacketMetadataArc::downgrade(&actual));
        if let Err(unused) = self.owner.receiver.set(actual) {
            drop(unused);
            return Err(
                std::io::Error::other("one original packet RX operation per association").into(),
            );
        }
        Ok(weak)
    }
}

#[cfg(test)]
mod tests;

//! Same original Tx/Rx fences and custody, without rewriting borrowed frames.
use super::*;
// The original views are dropped before either shared custody reference. Only
// the final actual Tx/Rx return drops BorrowedDatagram and its original loan IO.
pub(super) struct Transferred {
    pub(super) original: BorrowedDatagram,
    pub(super) _view: crate::controlled_carrier::CarrierUserView,
}
pub(super) struct TransferredTx {
    pub(super) inner: Tx,
    pub(super) fence: crate::controlled_carrier::related::ViewCancellation,
    pub(super) send_pending: bool,
    pub(super) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(super) custody: Arc<Transferred>,
}
pub(super) struct TransferredRx {
    pub(super) inner: Rx,
    pub(super) fence: crate::controlled_carrier::related::ViewCancellation,
    pub(super) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(super) custody: Arc<Transferred>,
}
impl DatagramTx for TransferredTx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        frame: &zero_core::packet::Datagram,
    ) -> std::task::Poll<Result<crate::contract::SendReceipt, crate::contract::SendFailure>> {
        let this = self.get_mut();
        if this.fence.stopped(cx) {
            let delivery = if this.send_pending {
                DeliveryState::Unknown
            } else {
                DeliveryState::NotSubmitted
            };
            return std::task::Poll::Ready(Err(crate::contract::SendFailure::new(
                error(
                    this.custody.original.slot.instance,
                    Stage::Send,
                    Reason::Revoked,
                    delivery,
                ),
                delivery,
            )));
        }
        let result = this.inner.as_mut().poll_send(cx, frame);
        this.send_pending = result.is_pending();
        if let std::task::Poll::Ready(Ok(receipt)) = &result {
            if let Some(observer) = &this.observer {
                observer.sent(receipt.accepted_bytes);
            }
        }
        result
    }
}
impl DatagramRx for TransferredRx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<zero_core::packet::Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.fence.stopped(cx) {
            return std::task::Poll::Ready(Err(error(
                this.custody.original.slot.instance,
                Stage::Receive,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )));
        }
        let result = this.inner.as_mut().poll_recv(cx);
        if let std::task::Poll::Ready(Ok(Some(frame))) = &result {
            if let Some(observer) = &this.observer {
                observer.received(frame.payload.len());
            }
        }
        result
    }
}

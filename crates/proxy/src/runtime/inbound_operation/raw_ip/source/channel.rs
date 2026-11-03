//! Bounded, cancellation-safe Packet channel views over the same wire owner.
use super::*;
pub(crate) struct Tx {
    pub(super) sender: PollSender<zero_traits::PacketBuffer>,
    pub(super) cancel: zero_platform_tokio::contract::CancellationToken,
}
impl PacketTx for Tx {
    fn poll_send(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if self.cancel.is_cancelled() {
            return Poll::Ready(Err(SendFailure::new(
                error(Reason::Revoked),
                DeliveryState::NotSubmitted,
            )));
        }
        match self.sender.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(SendFailure::new(
                error(Reason::Revoked),
                DeliveryState::NotSubmitted,
            ))),
            Poll::Ready(Ok(())) => {
                let bytes = packet.payload().len();
                match self.sender.send_item(packet.payload().to_vec().into()) {
                    Ok(()) => Poll::Ready(Ok(SendReceipt {
                        accepted_bytes: bytes,
                        delivery: DeliveryState::LocallyAccepted,
                    })),
                    Err(_) => Poll::Ready(Err(SendFailure::new(
                        error(Reason::Revoked),
                        DeliveryState::NotSubmitted,
                    ))),
                }
            }
        }
    }
}
pub(crate) struct Rx {
    pub(super) receiver: mpsc::Receiver<IpPacket>,
}
impl PacketRx for Rx {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        self.receiver.poll_recv(cx).map(Ok)
    }
}
pub(crate) struct Control {
    pub(super) cancel: zero_platform_tokio::contract::CancellationToken,
    pub(super) stop: watch::Sender<bool>,
    pub(super) witness: TaskJoinWitness,
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.cancel.cancel();
        self.stop.send_replace(true);
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        tokio::time::timeout_at(deadline, self.witness.wait())
            .await
            .map_err(|source| error(Reason::Timeout).with_source(source))?;
        Ok(CloseReport {
            released_resources: vec![
                "original packet wire caller joined; native provider remains separate".into(),
            ],
            ..Default::default()
        })
    }
}

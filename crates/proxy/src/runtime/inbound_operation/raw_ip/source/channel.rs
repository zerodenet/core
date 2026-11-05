//! Bounded, cancellation-safe Packet channel views over the same wire owner.
use super::*;
pub(crate) struct Tx {
    pub(super) sender: PollSender<IpPacket>,
    pub(super) cancel: zero_platform_tokio::contract::CancellationToken,
    pub(super) pending: Arc<Mutex<Option<IpPacket>>>,
    pub(super) facts: PacketFacts,
}
impl PacketTx for Tx {
    fn poll_send(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if self.pending.lock().unwrap().is_some() {
            return Poll::Ready(Err(SendFailure::new(
                error(Reason::Protocol),
                DeliveryState::Unknown,
            )));
        }
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
                let mut copied = IpPacket::new(
                    packet.payload().to_vec().into(),
                    packet.return_association.clone(),
                )
                .map_err(|_| error(Reason::Protocol));
                if let Some(ingress) = packet.ingress() {
                    copied = copied.map(|packet| packet.with_ingress(ingress));
                }
                let copied = match copied {
                    Ok(packet) => packet,
                    Err(error) => {
                        return Poll::Ready(Err(SendFailure::new(
                            error,
                            DeliveryState::NotSubmitted,
                        )))
                    }
                };
                match self.sender.send_item(copied) {
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
    fn poll_send_owned(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &mut Option<IpPacket>,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        // The real persistent source owner holds the whole original before
        // polling queue capacity. No parked original belongs only to a future.
        if let Some(incoming) = packet.as_ref() {
            if incoming.return_association.endpoint != self.facts.endpoint
                || incoming.return_association.generation != self.facts.inbound.generation
            {
                return Poll::Ready(Err(SendFailure::new(
                    error(Reason::StalePlan),
                    DeliveryState::NotSubmitted,
                )));
            }
            let mut pending = self.pending.lock().unwrap();
            if pending.is_some() {
                return Poll::Ready(Err(SendFailure::new(
                    error(Reason::Protocol),
                    DeliveryState::Unknown,
                )));
            }
            *pending = packet.take();
        }
        if self.pending.lock().unwrap().is_none() {
            return Poll::Ready(Err(SendFailure::new(
                error(Reason::Protocol),
                DeliveryState::Unknown,
            )));
        }
        if self.cancel.is_cancelled() {
            *packet = self.pending.lock().unwrap().take();
            return Poll::Ready(Err(SendFailure::new(
                error(Reason::Revoked),
                if packet.is_some() {
                    DeliveryState::NotSubmitted
                } else {
                    DeliveryState::Unknown
                },
            )));
        }
        match self.sender.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => {
                *packet = self.pending.lock().unwrap().take();
                Poll::Ready(Err(SendFailure::new(
                    error(Reason::Revoked),
                    if packet.is_some() {
                        DeliveryState::NotSubmitted
                    } else {
                        DeliveryState::Unknown
                    },
                )))
            }
            Poll::Ready(Ok(())) => {
                let Some(original) = self.pending.lock().unwrap().take() else {
                    return Poll::Ready(Err(SendFailure::new(
                        error(Reason::Protocol),
                        DeliveryState::Unknown,
                    )));
                };
                let bytes = original.payload().len();
                match self.sender.send_item(original) {
                    Ok(()) => Poll::Ready(Ok(SendReceipt::accepted(bytes))),
                    Err(returned) => {
                        *packet = returned.into_inner();
                        Poll::Ready(Err(SendFailure::new(
                            error(Reason::Revoked),
                            if packet.is_some() {
                                DeliveryState::NotSubmitted
                            } else {
                                DeliveryState::Unknown
                            },
                        )))
                    }
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
    pub(super) pending: Arc<Mutex<Option<IpPacket>>>,
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
        self.pending.lock().unwrap().take();
        Ok(CloseReport {
            released_resources: vec![
                "original packet wire caller joined; native provider remains separate".into(),
            ],
            ..Default::default()
        })
    }
}

#[cfg(test)]
#[path = "channel/tests.rs"]
mod tests;

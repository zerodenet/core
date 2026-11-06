use super::*;
use zero_platform_tokio::contract::packet::PacketTx;
impl PacketTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        Poll::Ready(Err(failure(
            Reason::Unsupported,
            "native Packet requires original owned submission",
            DeliveryState::NotSubmitted,
        )))
    }
    fn poll_send_owned(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &mut Option<IpPacket>,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.pending.is_none() {
            let Some(original) = packet.as_ref() else {
                return Poll::Ready(Err(failure(
                    Reason::Protocol,
                    "owned Packet submission has no original",
                    DeliveryState::Unknown,
                )));
            };
            if this.session.cancel.is_cancelled() {
                return Poll::Ready(Err(failure(
                    Reason::Revoked,
                    "Packet logical session closed",
                    DeliveryState::NotSubmitted,
                )));
            }
            if !this.session.validates(original) {
                return Poll::Ready(Err(failure(
                    Reason::StalePlan,
                    "Packet return association differs from its original source",
                    DeliveryState::NotSubmitted,
                )));
            }
            let Some(parent) = this.session.parent.upgrade() else {
                return Poll::Ready(Err(failure(
                    Reason::Revoked,
                    "same Packet original is retired",
                    DeliveryState::NotSubmitted,
                )));
            };
            let source =
                zero_stack::packet::ip_source(original.payload()).expect("validated original");
            let router = parent
                .routers
                .iter()
                .find(|ip| ip.is_ipv4() == source.is_ipv4());
            let local = if original.payload().len() > parent.mtu
                && !zero_stack::packet::ipv4_fragmentation_allowed(original.payload())
            {
                Some(router.and_then(|router| {
                    zero_stack::packet::build_icmp_mtu_error_response(
                        original.payload(),
                        *router,
                        parent.mtu,
                    )
                }))
            } else if zero_stack::packet::ip_hop_limit(original.payload())
                .is_some_and(|hop| hop <= 1)
            {
                Some(router.and_then(|router| {
                    zero_stack::packet::build_icmp_time_exceeded_response(
                        original.payload(),
                        *router,
                        parent.mtu,
                    )
                }))
            } else {
                None
            };
            if let Some(local) = local {
                if let Some(local) = local {
                    let response = IpPacket::new(local.into(), original.return_association.clone())
                        .expect("stack generated ICMP envelope");
                    if !this.session.deliver(response, false) {
                        return Poll::Ready(Err(failure(
                            Reason::Capacity,
                            "same Packet reply budget full",
                            DeliveryState::NotSubmitted,
                        )));
                    }
                }
                let bytes = packet
                    .take()
                    .expect("same admitted original")
                    .payload()
                    .len();
                return Poll::Ready(Ok(SendReceipt::accepted(bytes)));
            }
            let permit = match this.writes.try_reserve() {
                Ok(permit) => permit,
                Err(_) => {
                    return Poll::Ready(Err(failure(
                        Reason::Capacity,
                        "native Packet write FIFO is full or stopped",
                        DeliveryState::NotSubmitted,
                    )))
                }
            };
            let Some(parent) = this.session.parent.upgrade() else {
                return Poll::Ready(Err(failure(
                    Reason::Revoked,
                    "Packet original owner retired",
                    DeliveryState::NotSubmitted,
                )));
            };
            if let Err(error) = parent.returns.register(original, &this.session) {
                return Poll::Ready(Err(SendFailure::new(error, DeliveryState::NotSubmitted)));
            }
            let (result, received) = oneshot::channel();
            this.session.pending.fetch_add(1, Ordering::AcqRel);
            let original = packet.take().expect("same admitted owned Packet");
            permit.send(Write::Owned(OwnedWrite {
                packet: original,
                session: this.session.clone(),
                result,
                _pending: PendingWrite(this.session.clone()),
            }));
            this.pending = Some(received);
        }
        let outcome = Pin::new(
            this.pending
                .as_mut()
                .expect("same original Packet write receipt"),
        )
        .poll(cx);
        match outcome {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.pending = None;
                match result {
                    Ok((original, Ok(receipt))) => {
                        drop(original);
                        Poll::Ready(Ok(receipt))
                    }
                    Ok((original, Err(failure))) => {
                        *packet = Some(original);
                        Poll::Ready(Err(failure))
                    }
                    Err(source) => Poll::Ready(Err(SendFailure::new(
                        error(
                            Reason::Io,
                            "original Packet write ended without its receipt",
                        )
                        .with_source(source),
                        DeliveryState::Unknown,
                    ))),
                }
            }
        }
    }
}

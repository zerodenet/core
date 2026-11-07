mod local;
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
                Some(local::Kind::Mtu)
            } else if zero_stack::packet::ip_hop_limit(original.payload())
                .is_some_and(|hop| hop <= 1)
            {
                Some(local::Kind::Hop)
            } else {
                None
            };
            if let Some(kind) = local {
                return Poll::Ready(local::respond(
                    &this.session,
                    packet,
                    kind,
                    router.copied(),
                    parent.mtu,
                ));
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
            let original = packet.as_mut().expect("same original before FIFO custody");
            let lengths =
                match zero_stack::packet::fragment_packet_lengths(original.payload(), parent.mtu) {
                    Ok(lengths) => lengths,
                    Err(source) => return Poll::Ready(Err(memory_failure(source))),
                };
            let Some(derived_bytes) = lengths
                .iter()
                .try_fold(0usize, |sum, length| sum.checked_add(*length))
            else {
                return Poll::Ready(Err(memory_failure(
                    zero_traits::PacketDeriveError::Overflow,
                )));
            };
            if let Err(source) = original.prepare_transmit_buffers(lengths.len(), derived_bytes) {
                return Poll::Ready(Err(memory_failure(source)));
            }
            let fragments = match original.try_derive_payload_buffers(&lengths) {
                Ok(fragments)
                    if fragments.len() == lengths.len()
                        && fragments
                            .iter()
                            .zip(&lengths)
                            .all(|(packet, length)| packet.len() == *length) =>
                {
                    fragments
                }
                Ok(_) => {
                    return Poll::Ready(Err(memory_failure(
                        zero_traits::PacketDeriveError::LayoutMismatch,
                    )))
                }
                Err(source) => return Poll::Ready(Err(memory_failure(source))),
            };
            let (result, received) = oneshot::channel();
            this.session.pending.fetch_add(1, Ordering::AcqRel);
            let original = packet.take().expect("same admitted owned Packet");
            permit.send(Write::Owned(OwnedWrite {
                packet: original,
                fragments,
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

fn memory_failure(source: zero_traits::PacketDeriveError) -> SendFailure {
    use zero_traits::PacketDeriveError;
    let reason = match source {
        PacketDeriveError::Busy | PacketDeriveError::Capacity | PacketDeriveError::Overflow => {
            Reason::Capacity
        }
        PacketDeriveError::Revoked => Reason::Revoked,
        PacketDeriveError::Unreserved => Reason::Unsupported,
        PacketDeriveError::AlreadyDerived | PacketDeriveError::LayoutMismatch => Reason::Protocol,
    };
    SendFailure::new(
        error(reason, "original Packet transmit storage was not admitted").with_source(source),
        DeliveryState::NotSubmitted,
    )
}

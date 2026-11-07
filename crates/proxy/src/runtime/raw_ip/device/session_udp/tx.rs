//! Same original pending sender and caller/view custody.
use super::*;
impl DatagramTx for Tx {
    fn poll_send(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if self.cancelled.as_mut().poll(cx).is_ready()
            || self.users_cancelled.as_mut().poll(cx).is_ready()
        {
            self.shared.stop();
        }
        let shared = &self.shared;
        if frame.association != shared.association
            || frame.address != shared.target
            || frame.payload.len() > 65_507
        {
            return Poll::Ready(Err(SendFailure::new(
                failure(Stage::Send, Reason::InvalidConfig),
                DeliveryState::NotSubmitted,
            )));
        }
        let notifications = shared
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .socket
            .sender();
        // Declared before the pending-state guard: every credit return and
        // synchronous memory wake observes the actual owner locks released.
        let _notifications = notifications.defer_memory_notifications();
        {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.frame.as_ref() != frame)
            {
                drop(state);
                shared.stop();
                return Poll::Ready(Err(SendFailure::new(
                    failure(Stage::Send, Reason::Protocol),
                    DeliveryState::Unknown,
                )));
            }
            if state.pending.is_none() {
                if shared.cancel.is_cancelled() {
                    drop(state);
                    shared.stop();
                    return Poll::Ready(Err(SendFailure::new(
                        failure(Stage::Send, Reason::Revoked),
                        DeliveryState::NotSubmitted,
                    )));
                }
                let sender = shared
                    .original
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .socket
                    .sender();
                // This retained future needs an independent allocation. Pay
                // before copying; the later IP/fragment/cipher envelope has its
                // own credit and cannot account for this pending payload.
                let payload = match sender.copy_pending_payload(&frame.payload) {
                    Ok(payload) => payload,
                    Err(source) => {
                        let reason = match source {
                            zero_traits::PacketDeriveError::Capacity
                            | zero_traits::PacketDeriveError::Busy => Reason::Capacity,
                            zero_traits::PacketDeriveError::Revoked => Reason::Revoked,
                            _ => Reason::InvalidConfig,
                        };
                        return Poll::Ready(Err(SendFailure::new(
                            failure(Stage::Send, reason).with_source(source),
                            DeliveryState::NotSubmitted,
                        )));
                    }
                };
                let frame = Arc::new(Datagram {
                    address: frame.address.clone(),
                    association: frame.association,
                    payload,
                });
                let bytes = frame.clone();
                let peer = shared.peer;
                state.pending = Some(Pending {
                    frame,
                    sending: Box::pin(async move {
                        sender
                            .send_to(
                                &bytes.payload,
                                zero_stack::packet::Endpoint {
                                    ip: peer.ip(),
                                    port: peer.port(),
                                },
                            )
                            .await
                    }),
                });
            }
        }
        // A canceled already-pending send must report its real accepted prefix.
        // Closing the actual socket wakes that original future; do not relabel
        // partially submitted packets as NotSubmitted merely from cancellation.
        if shared.cancel.is_cancelled() {
            shared.stop();
        }
        match std::task::ready!(shared.poll_pending(cx)) {
            Ok(()) => Poll::Ready(Ok(SendReceipt {
                accepted_bytes: frame.payload.len(),
                delivery: DeliveryState::LocallyAccepted,
            })),
            Err(source) => {
                let delivery = if source.accepted_fragments == 0 {
                    DeliveryState::NotSubmitted
                } else {
                    DeliveryState::Unknown
                };
                if delivery == DeliveryState::Unknown {
                    shared.stop();
                }
                Poll::Ready(Err(SendFailure::new(
                    failure(Stage::Send, Reason::Io).with_source(source),
                    delivery,
                )))
            }
        }
    }
}

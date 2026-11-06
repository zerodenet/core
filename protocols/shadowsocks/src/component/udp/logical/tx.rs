use super::*;
pub struct Tx {
    shared: Arc<Logical>,
    association: AssociationId,
    limits: DatagramLimits,
    cancel: BoxFuture<'static, ()>,
    pending: Option<(
        Datagram,
        BoxFuture<'static, Result<ShadowsocksUdpPendingResponse, zero_transport::RuntimeError>>,
    )>,
}
impl Tx {
    pub(super) fn new(
        shared: Arc<Logical>,
        association: AssociationId,
        limits: DatagramLimits,
    ) -> Self {
        Self {
            cancel: shared.cancellation(),
            shared,
            association,
            limits,
            pending: None,
        }
    }
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if let Err(mut error) = this.shared.check(&mut this.cancel, cx, Stage::Send) {
            let delivery = if this.pending.is_some() {
                DeliveryState::Unknown
            } else {
                DeliveryState::NotSubmitted
            };
            error.delivery = delivery;
            drop(this.pending.take());
            return Poll::Ready(Err(SendFailure::new(error, delivery)));
        }
        this.limits.validate(frame)?;
        if frame.association != this.association
            || this.pending.as_ref().is_some_and(|(original, _)| {
                original.association != frame.association
                    || original.address != frame.address
                    || original.payload != frame.payload
            })
        {
            this.shared.stop.cancel();
            drop(this.pending.take());
            return Poll::Ready(Err(SendFailure::new(
                this.shared
                    .error(Stage::Send, Reason::Protocol, DeliveryState::Unknown),
                DeliveryState::Unknown,
            )));
        }
        if this.pending.is_none() {
            let flow = this.shared.flow.clone();
            let copy = Datagram {
                association: frame.association,
                address: frame.address.clone(),
                payload: frame.payload.to_vec().into(),
            };
            let destination = copy.address.clone();
            let payload = copy.payload.to_vec();
            this.pending = Some((
                copy,
                Box::pin(async move {
                    flow.request_datagram(&destination.address, destination.port, &payload)
                        .await
                }),
            ));
        }
        let pending = &mut this.pending.as_mut().unwrap().1;
        let response = match pending.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        drop(this.pending.take());
        let response = match response {
            Ok(response) => response,
            Err(source) => {
                let rejection =
                    crate::transport::udp_socket::responses::admission::rejected(&source);
                let delivery = if rejection.is_some() {
                    DeliveryState::NotSubmitted
                } else {
                    DeliveryState::Unknown
                };
                let reason = if rejection == Some(std::io::ErrorKind::WouldBlock) {
                    Reason::Capacity
                } else {
                    Reason::Io
                };
                return Poll::Ready(Err(SendFailure::new(
                    this.shared
                        .error(Stage::Send, reason, delivery)
                        .with_source(source),
                    delivery,
                )));
            }
        };
        if let Some(observer) = &this.shared.observer {
            observer.datagram_sent(this.shared.session, response.encoded_bytes() as u64);
        }
        let wake = {
            let mut state = this.shared.state.lock().unwrap();
            state.pending.push(response);
            state.receiver.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Poll::Ready(Ok(SendReceipt {
            accepted_bytes: frame.payload.len(),
            delivery: DeliveryState::LocallyAccepted,
        }))
    }
}
impl Drop for Tx {
    fn drop(&mut self) {
        drop(self.pending.take());
        self.shared.return_view();
    }
}

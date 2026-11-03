//! Two packet views share one unique native custody lease, without a worker.
use super::{native::NativeControl, DatagramLoan};
use crate::{
    contract::{
        datagram::{BoxDatagramSession, DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
        ChannelError, DeliveryState, Impact, Reason, SendFailure, SendReceipt, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierDatagram, CarrierUserView},
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
};

struct Custody {
    io: CarrierDatagram,
    _view: CarrierUserView,
    association: AssociationId,
    endpoint: Destination,
}
struct Tx(Arc<Custody>);
struct Rx {
    custody: Arc<Custody>,
    bytes: Vec<u8>,
}

impl CarrierDatagram {
    pub fn into_datagram_loan(
        self,
        view: CarrierUserView,
        association: AssociationId,
    ) -> Result<DatagramLoan, ChannelError> {
        let adoption = CarrierAdoption {
            slot: self.slot.clone(),
        };
        if !view.matches(&adoption) {
            return Err(super::super::error(
                self.slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        let original = adoption.transferred_origin()?;
        let endpoint = self.slot.state().endpoint.clone().ok_or_else(|| {
            super::super::error(
                self.slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )
        })?;
        let custody = Arc::new(Custody {
            io: self,
            _view: view,
            association,
            endpoint: endpoint.clone(),
        });
        let session: BoxDatagramSession = DatagramSession {
            tx: Box::pin(Tx(custody.clone())),
            rx: Box::pin(Rx {
                custody,
                bytes: vec![0; 65536],
            }),
            control: Box::new(NativeControl(adoption)),
            limits: DatagramLimits {
                max_payload: 65507,
                queued_bytes: 0,
                targets: DatagramTargets::Fixed(endpoint.clone()),
            },
        };
        Ok(DatagramLoan::new(session, original, association, endpoint))
    }
}
fn failure(custody: &Custody, stage: Stage, source: std::io::Error) -> ChannelError {
    super::super::error(
        custody.io.slot.instance,
        stage,
        Reason::Io,
        DeliveryState::Unknown,
    )
    .with_source(source)
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let custody = &self.0;
        if frame.association != custody.association
            || frame.address != custody.endpoint
            || frame.payload.len() > 65507
        {
            return Poll::Ready(Err(SendFailure::new(
                ChannelError::new(Stage::Send, Reason::Protocol, Impact::Datagram),
                DeliveryState::NotSubmitted,
            )));
        }
        custody
            .io
            .io
            .poll_send_to_addr(cx, &frame.payload, custody.io.peer)
            .map(|result| match result {
                Ok(size) if size == frame.payload.len() => Ok(SendReceipt {
                    accepted_bytes: size,
                    delivery: DeliveryState::LocallyAccepted,
                }),
                result => Err(SendFailure::new(
                    failure(
                        custody,
                        Stage::Send,
                        result
                            .err()
                            .unwrap_or_else(|| std::io::ErrorKind::WriteZero.into()),
                    ),
                    DeliveryState::Unknown,
                )),
            })
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        for _ in 0..32 {
            match this.custody.io.io.poll_recv_from_addr(cx, &mut this.bytes) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(source)) => {
                    return Poll::Ready(Err(failure(&this.custody, Stage::Receive, source)))
                }
                Poll::Ready(Ok((size, peer))) if peer == this.custody.io.peer => {
                    return Poll::Ready(Ok(Some(Datagram {
                        association: this.custody.association,
                        address: this.custody.endpoint.clone(),
                        payload: this.bytes[..size].to_vec().into(),
                    })))
                }
                _ => {}
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

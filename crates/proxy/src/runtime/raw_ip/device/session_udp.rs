//! Standard logical UDP session over one original in-memory stack socket.
//! The endpoint keeps the shared wire/native owner and its original Scope.
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    CancellationToken, ChannelError, DeliveryState, Impact, Reason, SendFailure, SendReceipt,
    Stage,
};
use zero_stack::client_udp::{
    ClientUdpCloseReceipt, ClientUdpEvent, ClientUdpSendError, ClientUdpSocket,
};

mod control;
type Cancelled = Pin<Box<dyn Future<Output = ()> + Send>>;
pub(super) struct Tx {
    shared: Arc<Shared>,
    cancelled: Cancelled,
}
pub(super) struct Rx {
    shared: Arc<Shared>,
    cancelled: Cancelled,
}
pub(super) struct Control(Arc<Shared>);
pub(super) type Session = DatagramSession<Tx, Rx, Control>;
type Sending = Pin<Box<dyn Future<Output = Result<(), ClientUdpSendError>> + Send>>;
struct Pending {
    frame: Arc<Datagram>,
    sending: Sending,
}
struct State {
    pending: Option<Pending>,
    first: Option<Arc<ChannelError>>,
}
struct Original {
    socket: ClientUdpSocket,
    closed: Option<ClientUdpCloseReceipt>,
}
struct Shared {
    original: Mutex<Original>,
    state: Mutex<State>,
    peer: SocketAddr,
    target: Destination,
    association: AssociationId,
    cancel: CancellationToken,
}
pub(super) fn session(
    socket: ClientUdpSocket,
    peer: SocketAddr,
    target: Destination,
    association: AssociationId,
    cancel: CancellationToken,
) -> Session {
    let shared = Arc::new(Shared {
        original: Mutex::new(Original {
            socket,
            closed: None,
        }),
        state: Mutex::new(State {
            pending: None,
            first: None,
        }),
        peer,
        target: target.clone(),
        association,
        cancel: cancel.child_token(),
    });
    DatagramSession {
        tx: Tx {
            shared: shared.clone(),
            cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
        },
        rx: Rx {
            shared: shared.clone(),
            cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
        },
        control: Control(shared),
        limits: DatagramLimits {
            max_payload: 65_507,
            targets: DatagramTargets::Fixed(target),
            queued_bytes: 65_507,
        },
    }
}
fn failure(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
impl Shared {
    fn stop(&self) {
        self.cancel.cancel();
        let mut original = self
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if original.closed.is_none() {
            original.closed = Some(original.socket.close());
        }
    }
    fn poll_pending(&self, cx: &mut Context<'_>) -> Poll<Result<(), ClientUdpSendError>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(pending) = state.pending.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let result = std::task::ready!(pending.sending.as_mut().poll(cx));
        state.pending.take();
        if let Err(source) = &result {
            if source.accepted_fragments != 0 && state.first.is_none() {
                let mut error = failure(Stage::Send, Reason::Io).with_source(*source);
                error.delivery = DeliveryState::Unknown;
                state.first = Some(Arc::new(error));
            }
        }
        Poll::Ready(result)
    }
}
impl DatagramTx for Tx {
    fn poll_send(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
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
                let frame = Arc::new(Datagram {
                    address: frame.address.clone(),
                    association: frame.association,
                    payload: frame.payload.to_vec().into(),
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
impl DatagramRx for Rx {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.shared.stop();
        }
        let shared = &self.shared;
        if shared.cancel.is_cancelled() {
            shared.stop();
            return Poll::Ready(Ok(None));
        }
        let mut original = shared
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            match std::task::ready!(original.socket.poll_recv_event(cx)) {
                Some(ClientUdpEvent::Datagram(packet)) => {
                    if packet.source.ip != shared.peer.ip()
                        || packet.source.port != shared.peer.port()
                    {
                        continue;
                    }
                    return Poll::Ready(Ok(Some(Datagram {
                        address: shared.target.clone(),
                        association: shared.association,
                        payload: packet.payload.into(),
                    })));
                }
                Some(ClientUdpEvent::IcmpError(source)) => {
                    return Poll::Ready(Err(failure(Stage::Receive, Reason::Io).with_source(
                        std::io::Error::new(
                            std::io::ErrorKind::NetworkUnreachable,
                            format!("packet endpoint ICMP error: {source:?}"),
                        ),
                    )))
                }
                None => return Poll::Ready(Ok(None)),
            }
        }
    }
}

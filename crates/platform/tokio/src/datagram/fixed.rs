//! Raw datagram I/O and explicit native close share the original descriptor.
use crate::{
    contract::{
        datagram::{
            erase_session, BoxDatagramSession, DatagramLimits, DatagramRx, DatagramSession,
            DatagramTx,
        },
        lifecycle::{ChannelControl, CloseReport, Unfinished},
        ChannelError, CloseReason, DeliveryState, Impact, Reason, SendFailure, SendReceipt, Stage,
    },
    TokioDatagramSocket,
};
use std::{
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::time::{sleep_until, Duration, Instant};
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address,
};
struct Shared {
    state: Mutex<State>,
    cancel: CancellationToken,
    association: AssociationId,
    peer: SocketAddr,
    limits: DatagramLimits,
}
struct State {
    socket: Option<TokioDatagramSocket>,
    closed: bool,
    error: Option<Arc<io::Error>>,
}
struct Tx {
    shared: Arc<Shared>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
struct Rx {
    shared: Arc<Shared>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    buffer: Vec<u8>,
}
struct Control {
    shared: Arc<Shared>,
    reported: bool,
}
fn destination(peer: SocketAddr) -> Destination {
    Destination::new(
        match peer.ip() {
            IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
pub(super) fn session(
    socket: TokioDatagramSocket,
    association: AssociationId,
    peer: SocketAddr,
    cancel: CancellationToken,
) -> BoxDatagramSession {
    let max_payload = if peer.is_ipv4() { 65507 } else { 65527 };
    let limits = DatagramLimits {
        max_payload,
        targets: DatagramTargets::Fixed(destination(peer)),
        queued_bytes: 0,
    };
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            socket: Some(socket),
            closed: false,
            error: None,
        }),
        cancel: cancel.child_token(),
        association,
        peer,
        limits: limits.clone(),
    });
    erase_session(DatagramSession {
        tx: Tx {
            shared: shared.clone(),
            cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
        },
        rx: Rx {
            shared: shared.clone(),
            cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
            buffer: Vec::new(),
        },
        control: Control {
            shared,
            reported: false,
        },
        limits,
    })
}
fn error(stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Session);
    error.delivery = delivery;
    error
}
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        if this.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(SendFailure::new(
                error(Stage::Send, Reason::Cancelled, DeliveryState::NotSubmitted),
                DeliveryState::NotSubmitted,
            )));
        }
        if frame.association != this.shared.association {
            return Poll::Ready(Err(SendFailure::new(
                error(Stage::Send, Reason::StalePlan, DeliveryState::NotSubmitted),
                DeliveryState::NotSubmitted,
            )));
        }
        if let Err(failure) = this.shared.limits.validate(frame) {
            return Poll::Ready(Err(failure));
        }
        let state = this.shared.state.lock().expect("raw UDP owner poisoned");
        let Some(socket) = &state.socket else {
            return Poll::Ready(Err(SendFailure::new(
                error(Stage::Send, Reason::Revoked, DeliveryState::NotSubmitted),
                DeliveryState::NotSubmitted,
            )));
        };
        match socket.poll_send_to_addr(cx, &frame.payload, this.shared.peer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(size)) if size == frame.payload.len() => {
                Poll::Ready(Ok(SendReceipt::accepted(size)))
            }
            Poll::Ready(Ok(_)) => Poll::Ready(Err(SendFailure::new(
                error(Stage::Send, Reason::Io, DeliveryState::Unknown),
                DeliveryState::Unknown,
            ))),
            Poll::Ready(Err(source)) => Poll::Ready(Err(SendFailure::new(
                error(Stage::Send, Reason::Io, DeliveryState::NotSubmitted).with_source(source),
                DeliveryState::NotSubmitted,
            ))),
        }
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        if this.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(None));
        }
        if this.buffer.is_empty() {
            this.buffer.resize(this.shared.limits.max_payload + 1, 0);
        }
        let state = this.shared.state.lock().expect("raw UDP owner poisoned");
        let Some(socket) = &state.socket else {
            return Poll::Ready(Ok(None));
        };
        match socket.poll_recv_from_addr(cx, &mut this.buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(source)) => Poll::Ready(Err(error(
                Stage::Receive,
                Reason::Io,
                DeliveryState::Unknown,
            )
            .with_source(source))),
            Poll::Ready(Ok((size, peer))) => {
                if peer != this.shared.peer {
                    return Poll::Ready(Err(error(
                        Stage::Receive,
                        Reason::Protocol,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(io::Error::other("unexpected raw UDP sender"))));
                }
                if size > this.shared.limits.max_payload {
                    return Poll::Ready(Err(error(
                        Stage::Receive,
                        Reason::Capacity,
                        DeliveryState::NotSubmitted,
                    )));
                }
                Poll::Ready(Ok(Some(Datagram {
                    address: destination(peer),
                    association: this.shared.association,
                    payload: this.buffer[..size].to_vec().into(),
                })))
            }
        }
    }
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        loop {
            {
                let mut state = self.shared.state.lock().expect("raw UDP owner poisoned");
                if let Some(socket) = state.socket.take() {
                    match socket.close() {
                        Ok(()) => state.closed = true,
                        Err(source) => state.error = Some(Arc::new(source)),
                    }
                }
                if !state.closed {
                    if let Some(source) = state.error.clone() {
                        if TokioDatagramSocket::close_is_retryable(&source) {
                            match TokioDatagramSocket::retry_close(&source) {
                                Ok(true) => {
                                    state.closed = true;
                                    state.error = None;
                                }
                                Ok(false) => {}
                                Err(source) => state.error = Some(Arc::new(source)),
                            }
                        } else {
                            return Err(error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                                .with_source(source));
                        }
                    }
                }
                if state.closed {
                    let mut report = CloseReport::default();
                    if !self.reported {
                        report.closed_io.push(format!(
                            "native UDP association {}",
                            self.shared.association.0
                        ));
                        self.reported = true;
                    }
                    return Ok(report);
                }
                if Instant::now() >= deadline {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io(format!(
                            "native UDP association {}",
                            self.shared.association.0
                        ))],
                        ..Default::default()
                    });
                }
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(10))).await;
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

//! Logical packet views of one registered physical pool, without child tasks.
//! Protocols own each packet's framing and response boundary.
use super::PoolUse;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};
use zero_core::{
    component::{AssociationId, SessionId},
    packet::Datagram,
    request::Destination,
};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Reason, SendFailure,
    SendReceipt, Stage,
};

/// Actual protocol packet handle, physical custody and original lineage.
pub struct PooledDatagram<I> {
    pub io: I,
    pub usage: PoolUse,
    pub origin: zero_platform_tokio::controlled_carrier::CarrierOrigin,
}
pub type PacketPoll = Poll<io::Result<Option<(Destination, Vec<u8>)>>>;
pub type PooledSession<I, U = PoolUse> = DatagramSession<Tx<I, U>, Rx<I, U>, Control<I, U>>;
/// Internal packet-I/O helper; protocol integration still exposes standard roles.
/// A send queues exactly one whole wire frame, or rejects it before submission.
pub trait PacketIo: Send + 'static {
    fn send(&mut self, frame: &Datagram) -> io::Result<()>;
    fn poll_send(&mut self, _cx: &mut Context<'_>, frame: &Datagram) -> Poll<io::Result<()>> {
        Poll::Ready(self.send(frame))
    }
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> PacketPoll;
}
struct State<I, U> {
    io: Option<I>,
    usage: Option<U>,
    views: usize,
}
struct Shared<I, U> {
    state: Mutex<State<I, U>>,
    changed: Notify,
    stop: CancellationToken,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
}
impl<I, U> Shared<I, U> {
    fn cancellation(&self) -> BoxFuture<'static, ()> {
        let stop = self.stop.clone();
        let admission = self.admission.clone();
        Box::pin(async move {
            tokio::select! { _=stop.cancelled()=>{}, _=admission.cancelled()=>{} }
        })
    }
    fn error(&self, stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }
    fn check(
        &self,
        cancel: &mut BoxFuture<'static, ()>,
        cx: &mut Context<'_>,
        stage: Stage,
    ) -> Result<(), ChannelError> {
        if self.stop.is_cancelled() || cancel.as_mut().poll(cx).is_ready() {
            self.stop.cancel();
            Err(self.error(stage, Reason::Revoked, DeliveryState::NotSubmitted))
        } else {
            Ok(())
        }
    }
    fn return_view(&self) {
        let mut state = self.state.lock().unwrap();
        state.views -= 1;
        // Even after the control is dropped, every outstanding view retains
        // real logical custody. The last Arc releases it only after I/O drops.
        drop(state);
        self.changed.notify_waiters();
    }
}
pub struct Tx<I, U = PoolUse> {
    shared: Arc<Shared<I, U>>,
    association: AssociationId,
    limits: DatagramLimits,
    cancel: BoxFuture<'static, ()>,
}
pub struct Rx<I, U = PoolUse> {
    shared: Arc<Shared<I, U>>,
    association: AssociationId,
    limits: DatagramLimits,
    cancel: BoxFuture<'static, ()>,
}
pub struct Control<I, U = PoolUse> {
    shared: Arc<Shared<I, U>>,
    closed: bool,
}

pub fn session<I: PacketIo>(
    io: I,
    usage: PoolUse,
    admission: Arc<SessionAdmission>,
    context: &zero_platform_tokio::contract::context::OpenContext,
    request: zero_platform_tokio::contract::datagram::DatagramRequest,
    limits: DatagramLimits,
) -> Result<PooledSession<I>, ChannelError> {
    session_with_lease(io, usage, admission, context, request, limits)
}
/// Reuse the exact packet/control implementation for another actual logical
/// custody lease. This creates no physical pool, driver, Scope or native proof.
pub fn session_with_lease<I: PacketIo, U: Send + 'static>(
    io: I,
    usage: U,
    admission: Arc<SessionAdmission>,
    context: &zero_platform_tokio::contract::context::OpenContext,
    request: zero_platform_tokio::contract::datagram::DatagramRequest,
    limits: DatagramLimits,
) -> Result<PooledSession<I, U>, ChannelError> {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            io: Some(io),
            usage: Some(usage),
            views: 2,
        }),
        changed: Notify::new(),
        stop: context.cancel.child_token(),
        session: context.session_identity()?,
        admission,
    });
    Ok(DatagramSession {
        tx: Tx {
            cancel: shared.cancellation(),
            shared: shared.clone(),
            association: request.association,
            limits: limits.clone(),
        },
        rx: Rx {
            cancel: shared.cancellation(),
            shared: shared.clone(),
            association: request.association,
            limits: limits.clone(),
        },
        control: Control {
            shared,
            closed: false,
        },
        limits,
    })
}
impl<I: PacketIo, U: Send + 'static> DatagramTx for Tx<I, U> {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        let this = self.get_mut();
        this.shared
            .check(&mut this.cancel, cx, Stage::Send)
            .map_err(|error| SendFailure::new(error, DeliveryState::NotSubmitted))?;
        this.limits.validate(frame)?;
        if frame.association != this.association {
            return Poll::Ready(Err(SendFailure::new(
                this.shared
                    .error(Stage::Send, Reason::Protocol, DeliveryState::NotSubmitted),
                DeliveryState::NotSubmitted,
            )));
        }
        let mut state = this.shared.state.lock().unwrap();
        let result = std::task::ready!(state
            .io
            .as_mut()
            .expect("active packet views")
            .poll_send(cx, frame));
        Poll::Ready(
            result
                .map(|()| SendReceipt {
                    accepted_bytes: frame.payload.len(),
                    delivery: DeliveryState::LocallyAccepted,
                })
                .map_err(|source| {
                    // Only the protocol's explicit local validation fact can
                    // reject one unsubmitted packet. An io::ErrorKind alone
                    // must never downgrade a real codec/driver failure.
                    let packet = source
                        .get_ref()
                        .and_then(|source| source.downcast_ref::<ChannelError>())
                        .filter(|error| {
                            error.stage == Stage::Send
                                && error.delivery == DeliveryState::NotSubmitted
                                && error.impact == zero_platform_tokio::contract::Impact::Datagram
                                && matches!(error.reason, Reason::Protocol | Reason::Unsupported)
                                && error.cleanup_failure_receipt().is_none()
                        })
                        .map(|error| (error.reason, error.retry));
                    let reason = match source.kind() {
                        io::ErrorKind::WouldBlock => Reason::Capacity,
                        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
                            Reason::Protocol
                        }
                        _ => Reason::Io,
                    };
                    let mut error = this.shared.error(
                        Stage::Send,
                        packet.map_or(reason, |(reason, _)| reason),
                        DeliveryState::NotSubmitted,
                    );
                    if let Some((_, retry)) = packet {
                        error.impact = zero_platform_tokio::contract::Impact::Datagram;
                        error.retry = retry;
                    }
                    SendFailure::new(error.with_source(source), DeliveryState::NotSubmitted)
                }),
        )
    }
}
impl<I: PacketIo, U: Send + 'static> DatagramRx for Rx<I, U> {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        this.shared.check(&mut this.cancel, cx, Stage::Receive)?;
        let mut state = this.shared.state.lock().unwrap();
        let packet = std::task::ready!(state
            .io
            .as_mut()
            .expect("active packet views")
            .poll_recv(cx));
        drop(state);
        match packet {
            Ok(Some((address, payload))) if payload.len() <= this.limits.max_payload => {
                Poll::Ready(Ok(Some(Datagram {
                    association: this.association,
                    address,
                    payload: payload.into(),
                })))
            }
            Ok(None) => Poll::Ready(Ok(None)),
            packet => {
                this.shared.stop.cancel();
                Poll::Ready(Err(match packet {
                    Err(source) => this
                        .shared
                        .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                        .with_source(source),
                    _ => {
                        this.shared
                            .error(Stage::Receive, Reason::Capacity, DeliveryState::Unknown)
                    }
                }))
            }
        }
    }
}
impl<I, U> Drop for Tx<I, U> {
    fn drop(&mut self) {
        self.shared.return_view();
    }
}
impl<I, U> Drop for Rx<I, U> {
    fn drop(&mut self) {
        self.shared.return_view();
    }
}
impl<I: Send, U: Send> ChannelControl for Control<I, U> {
    fn cancel(&self, _: CloseReason) {
        self.shared.stop.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        let close = async {
            loop {
                let changed = self.shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let mut state = self.shared.state.lock().unwrap();
                    if state.views == 0 {
                        // The protocol's actual logical registration is removed
                        // before ending physical custody. Neither certifies native closure.
                        drop(state.io.take());
                        drop(state.usage.take());
                        break;
                    }
                }
                changed.await;
            }
        };
        if timeout_at(deadline, close).await.is_err() {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "MUX UDP transferred packet views".into(),
                )],
                ..Default::default()
            });
        }
        let mut report = CloseReport::default();
        if !self.closed {
            report
                .released_resources
                .push("MUX UDP logical registration and packet views".into());
            self.closed = true;
        }
        Ok(report)
    }
}
impl<I, U> Drop for Control<I, U> {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}

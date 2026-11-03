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
pub type PooledSession<I> = DatagramSession<Tx<I>, Rx<I>, Control<I>>;
/// Internal packet-I/O helper; protocol integration still exposes standard roles.
/// A send queues exactly one whole wire frame, or rejects it before submission.
pub trait PacketIo: Send + 'static {
    fn send(&mut self, frame: &Datagram) -> io::Result<()>;
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> PacketPoll;
}
struct State<I> {
    io: Option<I>,
    usage: Option<PoolUse>,
    views: usize,
}
struct Shared<I> {
    state: Mutex<State<I>>,
    changed: Notify,
    stop: CancellationToken,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
}
impl<I> Shared<I> {
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
pub struct Tx<I> {
    shared: Arc<Shared<I>>,
    association: AssociationId,
    limits: DatagramLimits,
    cancel: BoxFuture<'static, ()>,
}
pub struct Rx<I> {
    shared: Arc<Shared<I>>,
    association: AssociationId,
    limits: DatagramLimits,
    cancel: BoxFuture<'static, ()>,
}
pub struct Control<I> {
    shared: Arc<Shared<I>>,
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
impl<I: PacketIo> DatagramTx for Tx<I> {
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
        let result = state.io.as_mut().expect("active packet views").send(frame);
        Poll::Ready(
            result
                .map(|()| SendReceipt {
                    accepted_bytes: frame.payload.len(),
                    delivery: DeliveryState::LocallyAccepted,
                })
                .map_err(|source| {
                    let reason = match source.kind() {
                        io::ErrorKind::WouldBlock => Reason::Capacity,
                        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
                            Reason::Protocol
                        }
                        _ => Reason::Io,
                    };
                    SendFailure::new(
                        this.shared
                            .error(Stage::Send, reason, DeliveryState::NotSubmitted)
                            .with_source(source),
                        DeliveryState::NotSubmitted,
                    )
                }),
        )
    }
}
impl<I: PacketIo> DatagramRx for Rx<I> {
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
impl<I> Drop for Tx<I> {
    fn drop(&mut self) {
        self.shared.return_view();
    }
}
impl<I> Drop for Rx<I> {
    fn drop(&mut self) {
        self.shared.return_view();
    }
}
impl<I: Send> ChannelControl for Control<I> {
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
impl<I> Drop for Control<I> {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}

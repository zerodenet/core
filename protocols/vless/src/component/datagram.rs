//! One original VLESS u16 packet stream, shared by ordinary and gRPC callers.
mod control;
pub(super) mod response;
mod rx;
mod tx;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use zero_core::{
    component::{AssociationId, SessionId},
    request::Destination,
};
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        context::{with_deadline, ConnectFact, OpenContext},
        datagram::{DatagramLimits, DatagramRequest, DatagramSession},
        stream::BoxStream,
        CancellationToken, ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{loan::LoanOwner, CarrierOrigin},
};
pub(super) use {control::Control, rx::Rx, tx::Tx};
struct State {
    stream: Option<BoxStream>,
    views: usize,
}
struct Shared {
    state: Mutex<State>,
    owner: tokio::sync::Mutex<Option<LoanOwner>>,
    original: Option<CarrierOrigin>,
    changed: Notify,
    cancel: CancellationToken,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
    association: AssociationId,
    target: Destination,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
}
impl Shared {
    fn error(&self, stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }
    fn cancellation(&self) -> zero_platform_tokio::contract::BoxFuture<'static, ()> {
        let cancel = self.cancel.clone();
        let admission = self.admission.clone();
        Box::pin(async move {
            tokio::select! {_=cancel.cancelled()=>{},_=admission.cancelled()=>{}}
        })
    }
    fn returned(&self) {
        self.state.lock().unwrap().views -= 1;
        self.changed.notify_waiters();
    }
    fn stop(&self, reason: zero_platform_tokio::contract::CloseReason) {
        self.cancel.cancel();
        self.changed.notify_waiters();
        if let Ok(owner) = self.owner.try_lock() {
            if let Some(owner) = owner.as_ref() {
                owner.cancel(reason);
            }
        }
    }
    fn return_stream(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.views != 0 {
            return false;
        }
        let returned = state.stream.take();
        drop(state);
        drop(returned);
        true
    }
}
/// `owner`/`original` remain genuine gRPC controls when supplied. Ordinary
/// framing returns its raw view to the outer same-slot native transfer owner.
pub(super) fn session(
    io: BoxStream,
    owner: Option<LoanOwner>,
    original: Option<CarrierOrigin>,
    request: DatagramRequest,
    context: OpenContext,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
    target: Destination,
) -> DatagramSession<Tx, Rx, Control> {
    let limits = DatagramLimits {
        max_payload: u16::MAX as usize,
        targets: request.targets,
        queued_bytes: u16::MAX as usize + 2,
    };
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            stream: Some(io),
            views: 2,
        }),
        owner: tokio::sync::Mutex::new(owner),
        original,
        changed: Notify::new(),
        cancel: context.cancel.child_token(),
        admission,
        session,
        association: request.association,
        target,
        observer: context.observer,
    });
    DatagramSession {
        tx: Tx::new(shared.clone(), limits.clone()),
        rx: Rx::new(shared.clone()),
        control: Control(shared),
        limits,
    }
}

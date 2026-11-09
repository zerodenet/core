//! One original VLESS u16 packet stream, shared by ordinary and gRPC callers.
mod control;
pub(in crate::component) mod memory;
pub(in crate::component) use memory::{Budget, Prepared};
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
#[derive(Clone)]
enum SharedRef {
    Library(Arc<Shared>),
    Hosted(zero_traits::packet_metadata::PacketMetadataArc<Shared>),
}
impl std::ops::Deref for SharedRef {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        match self {
            Self::Library(actual) => actual,
            Self::Hosted(actual) => actual,
        }
    }
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
    _targets: Option<zero_platform_tokio::controlled_carrier::CarrierBufferCredit>,
}
impl Shared {
    fn error(&self, stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }
    fn cancellation(&self) -> zero_platform_tokio::contract::BoxFuture<'static, ()> {
        Box::pin(cancelled(self.cancel.clone(), self.admission.clone()))
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
    prepared: Prepared,
) -> DatagramSession<Tx, Rx, Control> {
    let limits = DatagramLimits {
        max_payload: u16::MAX as usize,
        targets: request.targets,
        queued_bytes: u16::MAX as usize + 2,
    };
    let (header, tx, rx, output, tx_cancel, rx_cancel, targets) = match prepared {
        Prepared::Library => (
            None,
            memory::Buffer::Library(vec![]),
            memory::Buffer::Library(vec![0; 2]),
            None,
            None,
            None,
            None,
        ),
        Prepared::Hosted(prepared) => (
            Some(prepared.shared),
            prepared.tx,
            prepared.rx,
            Some(prepared.output),
            Some(prepared.tx_cancel),
            Some(prepared.rx_cancel),
            prepared.targets,
        ),
    };
    let shared = Shared {
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
        _targets: targets,
    };
    let shared = match header {
        Some(header) => SharedRef::Hosted(
            zero_traits::packet_metadata::PacketMetadataArc::from_prepaid(shared, header)
                .unwrap_or_else(|_| panic!("same actual Shared header promised")),
        ),
        None => SharedRef::Library(Arc::new(shared)),
    };
    DatagramSession {
        tx: Tx::new(shared.clone(), limits.clone(), tx, tx_cancel),
        rx: Rx::new(shared.clone(), rx, output, rx_cancel),
        control: Control(shared),
        limits,
    }
}

fn cancelled(
    cancel: CancellationToken,
    admission: Arc<SessionAdmission>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        tokio::select! {_=cancel.cancelled()=>{},_=admission.cancelled()=>{}}
    }
}

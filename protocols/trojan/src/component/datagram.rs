//! One original Trojan packet stream; no protocol worker or socket is created.
mod control;
mod rx;
mod tx;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use zero_core::component::{AssociationId, SessionId};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    context::{ConnectFact, OpenContext},
    datagram::{DatagramLimits, DatagramRequest, DatagramSession},
    stream::BoxStream,
    CancellationToken, ChannelError, DeliveryState, Reason, Stage,
};
use zero_transport::{
    runner::{DatagramProtocolHandshake, GuardedCall, HandshakeError},
    TcpRelayStream,
};
pub(super) use {control::Control, rx::Rx, tx::Tx};
struct State {
    stream: Option<BoxStream>,
    views: usize,
}
struct Shared {
    state: Mutex<State>,
    changed: Notify,
    cancel: CancellationToken,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
    association: AssociationId,
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
    fn stop(&self, _reason: zero_platform_tokio::contract::CloseReason) {
        self.cancel.cancel();
        self.changed.notify_waiters();
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
pub(super) fn session(
    io: BoxStream,
    request: DatagramRequest,
    context: OpenContext,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
) -> DatagramSession<Tx, Rx, Control> {
    let limits = DatagramLimits {
        max_payload: u16::MAX as usize,
        targets: request.targets,
        queued_bytes: u16::MAX as usize + 262,
    };
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            stream: Some(io),
            views: 2,
        }),
        changed: Notify::new(),
        cancel: context.cancel.child_token(),
        admission,
        session,
        association: request.association,
        observer: context.observer,
    });
    DatagramSession {
        tx: Tx::new(shared.clone(), limits.clone()),
        rx: Rx::new(shared.clone()),
        control: Control(shared),
        limits,
    }
}

impl DatagramProtocolHandshake<super::Options> for super::TrojanProtocol {
    type Prepared = Vec<u8>;
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;

    fn enforce_dial_policy(&self) -> bool {
        false
    }

    fn prepare_datagram(
        &self,
        options: &super::Options,
        request: &DatagramRequest,
    ) -> Result<Vec<u8>, HandshakeError> {
        let zero_core::request::DatagramTargets::Fixed(target) = &request.targets else {
            return Err(HandshakeError::unsupported());
        };
        crate::shared::build_request_with_hash(
            &options.password,
            &target.address,
            target.port,
            crate::shared::CMD_UDP,
        )
        .map_err(HandshakeError::invalid_config)
    }

    async fn handshake_datagram(
        &self,
        _options: &super::Options,
        mut io: TcpRelayStream,
        bytes: Vec<u8>,
        request: DatagramRequest,
        context: OpenContext,
        admission: Arc<SessionAdmission>,
        user: Option<SessionId>,
    ) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
        {
            let call = GuardedCall::from_admitted(&admission, &context, user);
            call.run_io(Stage::Handshake, DeliveryState::Unknown, async {
                io.write_all(&bytes).await?;
                io.flush().await
            })
            .await?;
            let facts = call.facts();
            facts.record(ConnectFact::CarrierReady);
            facts.record(ConnectFact::RequestSubmitted);
            facts.protocol_traffic(0, bytes.len() as u64);
        }
        Ok(session(Box::pin(io), request, context, admission, user))
    }
}

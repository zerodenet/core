//! One original Trojan packet stream; no protocol worker or socket is created.
mod control;
mod rx;
mod tx;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use zero_core::component::{AssociationId, SessionId};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::{DatagramLimits, DatagramRequest, DatagramSession},
    stream::BoxStream,
    CancellationToken, ChannelError, DeliveryState, Reason, Stage,
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

impl zero_platform_tokio::contract::datagram::DatagramOutbound for super::Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
        let user = self.admission.check(&context)?;
        let zero_core::request::DatagramTargets::Fixed(target) = &request.targets else {
            return Err(self.admission.error(
                user,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let bytes = crate::shared::build_request_with_hash(
            &self.options.password,
            &target.address,
            target.port,
            crate::shared::CMD_UDP,
        )
        .map_err(|source| {
            self.admission
                .error(
                    user,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let mut io = self.open_transport(&context, user).await?;
        let submitting = async {
            use tokio::io::AsyncWriteExt;
            io.write_all(&bytes).await?;
            io.flush().await?;
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(user,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(user,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,submitting)=>result.map_err(|source|self.admission.error(user,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(user,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if let Some(observer) = &context.observer {
            observer.record(user, ConnectFact::CarrierReady);
            observer.record(user, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(user, 0, bytes.len() as u64);
        }
        Ok(session(
            Box::pin(io),
            request,
            context,
            self.admission.clone(),
            user,
        ))
    }
}

//! Fixed-target VLESS UDP framing over the same actual pooled gRPC controller.
mod control;
mod response;
mod rx;
mod tx;
use super::factory::Outbound;
use std::sync::{Arc, Mutex};
use tokio::{io::AsyncWriteExt, sync::Notify};
use zero_core::{
    component::{AssociationId, SessionId},
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        context::{with_deadline, ConnectFact, OpenContext},
        datagram::{DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession},
        CancellationToken, ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{
        loan::{LoanIo, LoanOwner, StreamLoan},
        CarrierOrigin,
    },
};
struct State {
    stream: Option<LoanIo>,
    views: usize,
}
struct Shared {
    state: Mutex<State>,
    owner: tokio::sync::Mutex<LoanOwner>,
    original: CarrierOrigin,
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
            owner.cancel(reason);
        }
    }
}
impl DatagramOutbound for Outbound {
    type Tx = tx::Tx;
    type Rx = rx::Rx;
    type Control = control::Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.admission.check(&context)?;
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let bytes = crate::shared::build_request_to(
            &target.address,
            target.port,
            &self.options.prepared.protocol.uuid,
            crate::shared::CMD_UDP,
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let raw = self.acquire(&context).await?;
        let (io, control, original) = raw.into_controlled_parts()?;
        let mut io =
            zero_platform_tokio::TcpRelayStream::new(crate::component::carrier::Carrier::new(io));
        let handshake = async {
            io.write_all(&bytes).await?;
            io.flush().await
        };
        tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,handshake)=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(session, 0, bytes.len() as u64);
        }
        // The existing UDP tunnel waits for the actual VLESS response header;
        // gRPC establishment is not destination acceptance.
        let response = response::read(&mut io, &context, &self.admission, session).await?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::PeerConfirmed);
            observer.protocol_traffic(session, response as u64, 0);
        }
        let StreamLoan { io, owner } = StreamLoan::new(Box::pin(io), control, original.clone());
        let limits = DatagramLimits {
            max_payload: u16::MAX as usize,
            targets: request.targets.clone(),
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
            admission: self.admission.clone(),
            session,
            association: request.association,
            target: target.clone(),
            observer: context.observer,
        });
        Ok(DatagramSession {
            tx: tx::Tx::new(shared.clone(), limits.clone()),
            rx: rx::Rx::new(shared.clone()),
            control: control::Control(shared),
            limits,
        })
    }
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let association = request.association;
        let channel = self.open(request, context).await?;
        let original = channel.control.0.original.clone();
        let target = channel.control.0.target.clone();
        Ok(
            zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new(
                zero_platform_tokio::contract::datagram::erase_session(channel),
                original,
                association,
                target,
            ),
        )
    }
}

//! Protocol-owned packet session over one ordinary VMess carrier. No worker.
mod control;
mod io;
use super::*;
pub(super) use control::Control;
pub(super) use io::{Rx, Tx};
use std::sync::Arc;
use tokio::sync::Notify;
use zero_core::{component::AssociationId, request::DatagramTargets};
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession},
    CancellationToken,
};
struct State {
    stream: Option<crate::stream::VmessAeadStream<zero_transport::TcpRelayStream>>,
    views: usize,
}
struct Shared {
    state: Mutex<State>,
    changed: Notify,
    cancel: CancellationToken,
    admission: Arc<SessionAdmission>,
    session: Option<zero_core::component::SessionId>,
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
            tokio::select! { _=cancel.cancelled()=>{}, _=admission.cancelled()=>{} }
        })
    }
    fn returned(&self) {
        self.state.lock().unwrap().views -= 1;
        self.changed.notify_waiters();
    }
}
impl DatagramOutbound for Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
        let (target, header_len) = prepare(&request, &self.admission, &context)?;
        let carrier = self.open_transport(&target, &context).await?;
        open_carrier(
            &self.options,
            self.admission.clone(),
            request,
            context,
            carrier,
            header_len,
        )
        .await
    }
}
pub(super) fn prepare(
    request: &DatagramRequest,
    admission: &SessionAdmission,
    context: &OpenContext,
) -> Result<(zero_core::request::Destination, usize), ChannelError> {
    let session = admission.check(context)?;
    let DatagramTargets::Fixed(target) = &request.targets else {
        return Err(admission.error(
            session,
            Stage::Prepare,
            Reason::Unsupported,
            DeliveryState::NotSubmitted,
        ));
    };
    let header_len = crate::udp::encode_udp_flow_packet(&target.address, target.port, &[])
        .map_err(|source| {
            admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?
        .len();
    Ok((target.clone(), header_len))
}
pub(super) async fn open_carrier(
    options: &Options,
    admission: Arc<SessionAdmission>,
    request: DatagramRequest,
    context: OpenContext,
    mut carrier: zero_transport::TcpRelayStream,
    header_len: usize,
) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
    let session = admission.check(&context)?;
    let DatagramTargets::Fixed(target) = &request.targets else {
        unreachable!("prepared fixed target")
    };
    let (bytes, state) = crate::shared::prepare_request(
        &target.address,
        target.port,
        &options.uuid,
        options.cipher,
        crate::shared::CMD_UDP,
    )
    .map_err(|source| {
        admission
            .error(
                session,
                Stage::Handshake,
                Reason::Protocol,
                DeliveryState::NotSubmitted,
            )
            .with_source(source)
    })?;
    let submit = async {
        carrier.write_all(&bytes).await?;
        carrier.flush().await
    };
    tokio::select! { biased;
            _=admission.cancelled()=>return Err(admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,submit)=>result.map_err(|source|admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
    let stream = crate::stream::VmessAeadStream::outbound(carrier, state).map_err(|source| {
        admission
            .error(
                session,
                Stage::Handshake,
                Reason::Protocol,
                DeliveryState::Unknown,
            )
            .with_source(source)
    })?;
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            stream: Some(stream),
            views: 2,
        }),
        changed: Notify::new(),
        cancel: context.cancel.child_token(),
        admission,
        session,
        association: request.association,
        observer: context.observer,
    });
    let limits = DatagramLimits {
        max_payload: 65537 - header_len,
        targets: DatagramTargets::Fixed(target.clone()),
        queued_bytes: 65537,
    };
    Ok(DatagramSession {
        tx: Tx::new(shared.clone(), limits.clone()),
        rx: Rx::new(shared.clone()),
        control: Control::new(shared),
        limits,
    })
}

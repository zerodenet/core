//! SOCKS5 owns UDP ASSOCIATE, datagram framing and its logical control lifetime.
use super::Options;
use std::sync::Arc;
use zero_core::{
    component::SessionId,
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    context::{with_deadline, OpenContext},
    datagram::{DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession},
    network::CarrierContext,
    ChannelError, DeliveryState, Reason, Stage,
};
pub(super) mod association;
mod control;
mod control_handshake;
mod controlled;
mod rx;
mod shared;
mod tx;

pub(super) struct Outbound {
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    carriers: Option<zero_platform_tokio::controlled_carrier::CarrierGrants>,
    associations: Arc<association::Associations>,
}
impl Outbound {
    pub(super) fn new(
        options: Arc<Options>,
        admission: Arc<SessionAdmission>,
        carriers: Option<zero_platform_tokio::controlled_carrier::CarrierGrants>,
        associations: Arc<association::Associations>,
    ) -> Self {
        Self {
            options,
            admission,
            carriers,
            associations,
        }
    }
    fn failure(&self, session: Option<SessionId>, stage: Stage, reason: Reason) -> ChannelError {
        self.admission
            .error(session, stage, reason, DeliveryState::Unknown)
    }
}
impl DatagramOutbound for Outbound {
    type Tx = tx::Tx;
    type Rx = rx::Rx;
    type Control = control::Control;
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let session = self.admission.check(&context)?;
        let DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        if self.carriers.is_none() {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        }
        let association = request.association;
        let channel = self.open(request, context).await?;
        let original = channel
            .control
            .original()
            .expect("controlled association has its actual UDP original");
        let parent = channel
            .control
            .related_control()
            .expect("controlled UDP retains its same actual TCP control");
        let mut loan = zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new(
            zero_platform_tokio::contract::datagram::erase_session(channel),
            original,
            association,
            endpoint,
        );
        // Strong custody of the exact associated parent, not an observational
        // related_control edge, lets the receiving actual LoanOwner prove the
        // whole TCP+UDP cleanup handoff to its caller's finite pending ledger.
        loan.retain_related_control(parent)?;
        Ok(loan)
    }
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.admission.check(&context)?;
        if context.upstream.binding().dial != self.options.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if self.carriers.is_some() {
            return self.open_controlled(request, context, session).await;
        }
        let carrier = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.failure(session,Stage::Dial,Reason::Revoked)),
            _ = context.cancel.cancelled() => return Err(self.failure(session,Stage::Dial,Reason::Cancelled)),
            opened = with_deadline(context.deadline, context.upstream.open_stream(self.options.endpoint.clone(),carrier.clone())) => opened.map_err(|source| self.failure(session,Stage::Dial,Reason::Timeout).with_source(source))?,
        }.map_err(|error| self.admission.attach(error,session))?;
        let (tcp, relay) = self.establish_control(io, &context, session).await?;
        let raw = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.failure(session,Stage::Dial,Reason::Revoked)),
            _ = context.cancel.cancelled() => return Err(self.failure(session,Stage::Dial,Reason::Cancelled)),
            raw = with_deadline(context.deadline,context.upstream.open_datagram(DatagramRequest { association: request.association, targets: DatagramTargets::Fixed(relay.clone()) },carrier)) => raw.map_err(|source|self.failure(session,Stage::Dial,Reason::Timeout).with_source(source))?,
        }.map_err(|error|self.admission.attach(error,session))?;
        let endpoint = match &raw.limits.targets {
            DatagramTargets::Fixed(endpoint) => endpoint.clone(),
            DatagramTargets::Multiple => relay,
        };
        let limits = DatagramLimits {
            max_payload: raw.limits.max_payload.saturating_sub(10),
            targets: request.targets,
            queued_bytes: 0,
        };
        let shared = shared::Shared::new(tcp, raw.control, self.admission.clone(), session);
        Ok(DatagramSession {
            tx: tx::Tx::new(
                raw.tx,
                shared.clone(),
                request.association,
                endpoint.clone(),
                limits.clone(),
                raw.limits,
                context.cancel.clone(),
            ),
            rx: rx::Rx::new(
                raw.rx,
                shared.clone(),
                request.association,
                endpoint,
                context.cancel,
            ),
            control: control::Control::new(shared),
            limits,
        })
    }
}

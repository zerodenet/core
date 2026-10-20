//! SOCKS5 owns UDP ASSOCIATE, datagram framing and its logical control lifetime.
use super::{socket::Socket, Options};
use std::sync::Arc;
use zero_core::{
    component::SessionId,
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::{DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession},
    network::CarrierContext,
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_traits::UdpRelayProtocol;
mod control;
mod rx;
mod shared;
mod tx;

pub(super) struct Outbound {
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
}
impl Outbound {
    pub(super) fn new(options: Arc<Options>, admission: Arc<SessionAdmission>) -> Self {
        Self { options, admission }
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
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let mut socket = Socket::new(io);
        let relay_target = crate::udp::Socks5UdpRelayTarget {
            auth: self.options.auth.as_ref().map(|auth| auth.as_ref()),
        };
        let handshake = crate::Socks5Outbound.establish_udp_relay(&mut socket, &relay_target);
        let relay = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.failure(session,Stage::Handshake,Reason::Revoked)),
            _ = context.cancel.cancelled() => return Err(self.failure(session,Stage::Handshake,Reason::Cancelled)),
            relay = with_deadline(context.deadline,handshake) => relay.map_err(|source|self.failure(session,Stage::Handshake,Reason::Timeout).with_source(source))?,
        }.map_err(|wire| {
            let source = socket.take_source();
            let reason = if source.is_some() || matches!(wire,zero_core::Error::Io(_)) { Reason::Io } else { Reason::Protocol };
            self.failure(session,Stage::Handshake,reason).with_source(super::error::HandshakeError { wire, source })
        })?;
        if relay.1 == 0 {
            return Err(self.failure(session, Stage::Handshake, Reason::Protocol));
        }
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.record(session, ConnectFact::PeerConfirmed);
        }
        let relay = Destination::new(relay.0, relay.1);
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
        let shared = shared::Shared::new(
            socket.into_inner(),
            raw.control,
            self.admission.clone(),
            session,
        );
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

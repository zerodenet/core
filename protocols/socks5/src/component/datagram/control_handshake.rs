//! One actual SOCKS UDP control handshake, shared by ordinary and loan paths.
use super::*;
use crate::component::socket::Socket;
use zero_platform_tokio::contract::{context::ConnectFact, stream::BoxStream};
use zero_traits::UdpRelayProtocol;

impl Outbound {
    pub(super) async fn establish_control(
        &self,
        io: BoxStream,
        context: &OpenContext,
        session: Option<SessionId>,
    ) -> Result<(BoxStream, Destination), ChannelError> {
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
            self.failure(session,Stage::Handshake,reason).with_source(crate::component::error::HandshakeError { wire, source })
        })?;
        if relay.1 == 0 {
            return Err(self.failure(session, Stage::Handshake, Reason::Protocol));
        }
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.record(session, ConnectFact::PeerConfirmed);
        }
        Ok((socket.into_inner(), Destination::new(relay.0, relay.1)))
    }
}

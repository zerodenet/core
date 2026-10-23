//! A single SOCKS CONNECT reply owns the original stream and unread payload.
use alloc::boxed::Box;
use zero_core::request::RequestFacts;
use zero_platform_tokio::contract::{
    inbound::{ReplyOutcome, StreamReply},
    stream::{BoxStream, StreamIo},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};

/// The host supplies its real request identity after authentication. Policy
/// remains with the host; an absent subject does not imply unauthenticated I/O.
pub struct ConnectReply {
    facts: RequestFacts,
    socket: super::socket::Socket,
}
impl ConnectReply {
    pub fn new(io: impl StreamIo + 'static, facts: RequestFacts) -> Result<Self, ChannelError> {
        if facts.target.is_none() || facts.protocol.as_str() != "socks5" {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        }
        Ok(Self {
            facts,
            socket: super::socket::Socket::new(Box::pin(io)),
        })
    }
}
impl StreamReply for ConnectReply {
    type Stream = BoxStream;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(mut self, outcome: ReplyOutcome) -> Result<Option<BoxStream>, ChannelError> {
        let reply = match outcome {
            ReplyOutcome::LocalChannelReady => crate::Socks5Reply::Succeeded,
            ReplyOutcome::Blocked => crate::Socks5Reply::ConnectionNotAllowed,
            ReplyOutcome::UpstreamFailed => crate::Socks5Reply::HostUnreachable,
        };
        if let Err(wire) = crate::Socks5Inbound
            .send_response(&mut self.socket, reply)
            .await
        {
            let mut failure = ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session)
                .with_source(super::error::HandshakeError {
                    wire,
                    source: self.socket.take_source(),
                });
            failure.session = Some(self.facts.session);
            failure.delivery = DeliveryState::Unknown;
            return Err(failure);
        }
        Ok(matches!(outcome, ReplyOutcome::LocalChannelReady).then(|| self.socket.into_inner()))
    }
}

#[cfg(test)]
#[path = "../../tests/component/inbound.rs"]
mod tests;

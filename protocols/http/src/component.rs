//! A single HTTP CONNECT reply owns the original stream and unread payload.
mod error;
mod socket;
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
    socket: socket::Socket,
}
impl ConnectReply {
    pub fn new(io: impl StreamIo + 'static, facts: RequestFacts) -> Result<Self, ChannelError> {
        if facts.target.is_none() || facts.protocol.as_str() != "http" {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        }
        Ok(Self {
            facts,
            socket: socket::Socket::new(Box::pin(io)),
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
            ReplyOutcome::LocalChannelReady => crate::HttpConnectResponse::ConnectionEstablished,
            ReplyOutcome::Blocked => crate::HttpConnectResponse::Forbidden,
            ReplyOutcome::UpstreamFailed => crate::HttpConnectResponse::BadGateway,
        };
        if let Err(wire) = crate::HttpConnectInbound
            .send_response(&mut self.socket, reply)
            .await
        {
            let mut failure = ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session)
                .with_source(error::HandshakeError {
                    wire,
                    source: self.socket.take_source(),
                });
            failure.session = Some(self.facts.session);
            failure.delivery = DeliveryState::Unknown;
            return Err(failure);
        }
        if matches!(outcome, ReplyOutcome::LocalChannelReady) {
            Ok(Some(self.socket.into_inner()))
        } else {
            zero_traits::AsyncSocket::shutdown(&mut self.socket)
                .await
                .map_err(|source| {
                    let mut failure = ChannelError::new(Stage::Close, Reason::Io, Impact::Session)
                        .with_source(self.socket.take_source().unwrap_or(source));
                    failure.session = Some(self.facts.session);
                    failure.delivery = DeliveryState::Unknown;
                    failure
                })?;
            Ok(None)
        }
    }
}

#[cfg(test)]
#[path = "../tests/component/inbound.rs"]
mod tests;

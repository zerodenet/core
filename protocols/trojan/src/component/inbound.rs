//! Trojan has no CONNECT acknowledgement or protocol rejection response.
use zero_core::request::RequestFacts;
use zero_platform_tokio::contract::{
    inbound::{ReplyOutcome, StreamReply},
    stream::{BoxStream, StreamIo},
    ChannelError, Impact, Reason, Stage,
};

/// Owns the original accepted carrier, including any unread business bytes.
pub struct TcpReply {
    facts: RequestFacts,
    io: BoxStream,
}
impl TcpReply {
    pub fn new(io: impl StreamIo + 'static, facts: RequestFacts) -> Result<Self, ChannelError> {
        if facts.target.is_none() || facts.protocol.as_str() != "trojan" {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        }
        Ok(Self {
            facts,
            io: Box::pin(io),
        })
    }
}
impl StreamReply for TcpReply {
    type Stream = BoxStream;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> Result<Option<BoxStream>, ChannelError> {
        // No bytes or write shutdown: local readiness must preserve reverse
        // traffic after the client half-closes its upload direction.
        Ok(matches!(outcome, ReplyOutcome::LocalChannelReady).then_some(self.io))
    }
}

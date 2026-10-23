//! VMess authenticates and writes its response header before host admission.
use zero_core::request::RequestFacts;
use zero_platform_tokio::contract::{
    inbound::{ReplyOutcome, StreamReply},
    stream::{BoxStream, StreamIo},
    ChannelError, Impact, Reason, Stage,
};

/// Retains the original AEAD codec, carrier and unread request body.
pub struct TcpReply {
    facts: RequestFacts,
    io: BoxStream,
}
impl TcpReply {
    pub fn new(io: impl StreamIo + 'static, facts: RequestFacts) -> Result<Self, ChannelError> {
        if facts.target.is_none() || facts.protocol.as_str() != "vmess" {
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
        // Existing VMess response/header framing has already run exactly once.
        // Admission adds no second reply or upload shutdown to that codec.
        Ok(matches!(outcome, ReplyOutcome::LocalChannelReady).then_some(self.io))
    }
}

//! The encrypted stream owns its response salt/header; TCP admission adds none.
use zero_core::request::RequestFacts;
use zero_platform_tokio::contract::{
    inbound::{ReplyOutcome, StreamReply},
    stream::{BoxStream, StreamIo},
    ChannelError, Impact, Reason, Stage,
};
pub struct TcpReply {
    facts: RequestFacts,
    io: BoxStream,
}
impl TcpReply {
    pub fn new(io: impl StreamIo + 'static, facts: RequestFacts) -> Result<Self, ChannelError> {
        if facts.protocol.as_str() != "shadowsocks" || facts.target.is_none() {
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
        // Do not flush or half-close this cipher: first reverse business bytes
        // must trigger the existing response-salt/header rules exactly once.
        Ok(matches!(outcome, ReplyOutcome::LocalChannelReady).then_some(self.io))
    }
}

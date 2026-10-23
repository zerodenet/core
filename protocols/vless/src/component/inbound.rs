//! VLESS ordinary TCP response over the same original carrier and unread bytes.
use tokio::io::AsyncWriteExt;
use zero_core::request::RequestFacts;
use zero_platform_tokio::contract::{
    inbound::{ReplyOutcome, StreamReply},
    stream::{BoxStream, StreamIo},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};
pub struct TcpReply {
    facts: RequestFacts,
    io: BoxStream,
}
impl TcpReply {
    pub fn new(io: impl StreamIo + 'static, facts: RequestFacts) -> Result<Self, ChannelError> {
        if facts.protocol.as_str() != "vless" || facts.target.is_none() {
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
    async fn respond(mut self, outcome: ReplyOutcome) -> Result<Option<BoxStream>, ChannelError> {
        if matches!(outcome, ReplyOutcome::LocalChannelReady) {
            let result = async {
                self.io
                    .write_all(&[crate::shared::VLESS_VERSION, 0])
                    .await?;
                self.io.flush().await
            }
            .await;
            result.map_err(|source| {
                let mut error = ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session)
                    .with_source(source);
                error.session = Some(self.facts.session);
                error.delivery = DeliveryState::Unknown;
                error
            })?;
            Ok(Some(self.io))
        } else {
            // Preserve the original VLESS rejection: write-half close without
            // a success header or any invented protocol error bytes.
            let _ = self.io.shutdown().await;
            Ok(None)
        }
    }
}

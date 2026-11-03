//! Send the unpadded response before handing the original Vision codec to relay.
use super::*;
use crate::inbound::vision::VlessInboundTcpStream;
pub(crate) struct Reply<S> {
    pub(crate) io: VlessInboundTcpStream<S>,
    pub(crate) facts: RequestFacts,
}
impl<S> StreamReply for Reply<S>
where
    S: zero_platform_tokio::ClientStream + 'static,
{
    type Stream = BoxStream;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(mut self, outcome: ReplyOutcome) -> Result<Option<BoxStream>, ChannelError> {
        if outcome == ReplyOutcome::LocalChannelReady {
            zero_traits::AsyncSocket::write_all(&mut self.io, &[crate::shared::VLESS_VERSION, 0])
                .await
                .map_err(super::datagram::failure)?;
            Ok(Some(Box::pin(self.io)))
        } else {
            let _ = zero_traits::AsyncSocket::shutdown(&mut self.io).await;
            Ok(None)
        }
    }
}

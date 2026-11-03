//! Each logical handshake remains independent of other streams and the listener.
use super::*;
use crate::inbound::multiplex::{MieruInboundMultiplexer, MieruLogicalStream};
use std::num::NonZeroUsize;
use zero_core::InboundClientResponse;
use zero_core::{request::RequestFacts, ProtocolType, SessionAuth};
use zero_platform_tokio::contract::{context::AcceptContext, BoxFuture};
pub(super) struct Source {
    connection: MieruInboundMultiplexer,
    facts: InboundConnectionFacts,
    cancel: CancellationToken,
}
impl Source {
    pub(super) fn new(
        connection: MieruInboundMultiplexer,
        facts: InboundConnectionFacts,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            connection,
            facts,
            cancel,
        }
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl RequestMultiplexer for Source {
    fn auth(&self) -> Option<&SessionAuth> {
        Some(self.connection.authentication())
    }
    fn close(&mut self, _: &str) {
        self.cancel.cancel();
    }
    fn cancellation(&self) -> Option<CancellationToken> {
        Some(self.cancel.clone())
    }
    fn max_in_flight(&self) -> NonZeroUsize {
        NonZeroUsize::new(256).unwrap()
    }
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            let incoming = tokio::select! { biased; _=context.cancel.cancelled()=>return Ok(None), stream=self.connection.next_stream()=>stream.map_err(error)? };
            Ok(incoming.map(|stream| {
                PendingInbound::new(Logical {
                    stream,
                    facts: self.facts.clone(),
                    auth: self.connection.authentication().clone(),
                })
            }))
        })
    }
}
struct Logical {
    stream: MieruLogicalStream,
    facts: InboundConnectionFacts,
    auth: SessionAuth,
}
impl InboundHandshake for Logical {
    async fn complete(
        mut self,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let request = crate::tunnel::accept_tunneled_request(&mut self.stream)
            .await
            .map_err(error)?;
        match request {
            crate::tunnel::MieruTunnelRequest::Tcp { target, port } => {
                let facts = self.facts.request(
                    &context,
                    ProtocolType::new("mieru"),
                    Some(Destination::new(target, port)),
                    Some(self.auth),
                )?;
                Ok(AcceptedRequest::Stream(accept_stream(Reply {
                    io: self.stream,
                    facts,
                })))
            }
            crate::tunnel::MieruTunnelRequest::UdpAssociate { .. } => {
                let sender = self.facts.effective_source.ok_or_else(|| {
                    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Session)
                })?;
                let facts = self.facts.request(
                    &context,
                    ProtocolType::new("mieru_udp"),
                    None,
                    Some(self.auth),
                )?;
                Ok(AcceptedRequest::Datagram(accept_datagram(
                    datagram::Reply {
                        stream: TcpRelayStream::new(self.stream),
                        sender,
                        facts,
                    },
                )))
            }
        }
    }
}
struct Reply {
    io: MieruLogicalStream,
    facts: RequestFacts,
}
impl StreamReply for Reply {
    type Stream = MieruLogicalStream;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(
        mut self,
        outcome: ReplyOutcome,
    ) -> Result<Option<Self::Stream>, ChannelError> {
        if matches!(outcome, ReplyOutcome::LocalChannelReady) {
            return Ok(Some(self.io));
        }
        // Preserve the protocol's existing flushed logical CLOSE response for
        // policy/upstream rejection; dropping a stream only queues retirement.
        crate::inbound::MieruInbound
            .send_blocked(&mut self.io)
            .await
            .map_err(error)?;
        Ok(None)
    }
}

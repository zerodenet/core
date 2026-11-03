//! Every real carrier stream passes through the same standard authenticated request.
use super::*;
use zero_core::{InboundRouteAccept, InboundTransportMultiplexer};
use zero_platform_tokio::contract::{
    context::AcceptContext, BoxFuture, CancellationToken, Impact, Reason, Stage,
};
impl VlessInboundListenerRequest {
    pub(super) async fn accept_standard_transport(
        self,
        socket: TokioSocket,
        facts: InboundConnectionFacts,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let caller = context
            .accepted
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        match self
            .transport
            .clone()
            .accept_transport_streams(socket, caller)
            .await
            .map_err(datagram::failure)?
        {
            InboundRouteAccept::Route(incoming) => {
                Ok(AcceptedRequest::Multiplex(Box::new(Source {
                    request: self,
                    incoming,
                    facts,
                    cancel: context.cancel.child_token(),
                })))
            }
            InboundRouteAccept::Control(control) => Ok(AcceptedRequest::Control(control)),
            InboundRouteAccept::Fallback(mut replay) => {
                replay
                    .select_standard_route(&self.transport.fallback_policy, &facts)
                    .await
                    .map_err(datagram::failure)?;
                Ok(AcceptedRequest::Fallback(fallback::accepted(replay)?))
            }
        }
    }
}
struct Source {
    request: VlessInboundListenerRequest,
    incoming: VlessInboundTransportStreams,
    facts: InboundConnectionFacts,
    cancel: CancellationToken,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.incoming.close();
        self.cancel.cancel();
    }
}
impl RequestMultiplexer for Source {
    fn max_in_flight(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(256).unwrap()
    }
    fn cancellation(&self) -> Option<CancellationToken> {
        Some(self.cancel.clone())
    }
    fn close(&mut self, _: &str) {
        self.incoming.close();
        self.cancel.cancel();
    }
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            let incoming = tokio::select! {biased;
                _=self.cancel.cancelled()=>return Ok(None),
                _=context.cancel.cancelled()=>return Err(ChannelError::new(Stage::Receive,Reason::Cancelled,Impact::Session)),
                incoming=zero_platform_tokio::contract::context::with_deadline(context.deadline,self.incoming.accept_stream())=>incoming.map_err(datagram::failure)?,
            };
            Ok(incoming.map(|incoming| {
                PendingInbound::new(Stream {
                    request: self.request.clone(),
                    incoming,
                    facts: self.facts.clone(),
                })
            }))
        })
    }
}
struct Stream {
    request: VlessInboundListenerRequest,
    incoming: (TcpRelayStream, VlessInboundStreamMetadata),
    facts: InboundConnectionFacts,
}
impl InboundHandshake for Stream {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let (stream, metadata) = self.incoming;
        let mut facts = self.facts;
        facts.sni = metadata.sni;
        let stream = session::decrypt_stream(self.request.transport.decryption.as_ref(), stream)
            .await
            .map_err(datagram::failure)?;
        self.request
            .accept_standard_stream(record_client_stream(stream), facts, metadata.alpn, context)
            .await
    }
}

pub(super) fn from_native(
    request: VlessInboundListenerRequest,
    incoming: VlessInboundTransportStreams,
    facts: InboundConnectionFacts,
    cancel: CancellationToken,
) -> AcceptedRequest {
    AcceptedRequest::Multiplex(Box::new(Source {
        request,
        incoming,
        facts,
        cancel,
    }))
}

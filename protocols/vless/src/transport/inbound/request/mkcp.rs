//! mKCP logical streams use the real native peer admission and standard requests.
use super::*;
use zero_core::InboundTransportMultiplexer;
use zero_platform_tokio::contract::{
    context::AcceptContext, BoxFuture, CancellationToken, Impact, Reason, Stage,
};
impl VlessInboundListenerRequest {
    pub(crate) fn accept_standard_mkcp_peer(
        self,
        peer: udp::DatagramPeer,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let facts = peer.connection_facts().map_err(datagram::failure)?;
        let caller = context
            .accepted
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let profile = self
            .mkcp
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Session))?;
        let cancel = context.cancel.child_token();
        let incoming = profile.accept_peer_scoped(
            peer,
            self.transport.final_mask.udp(),
            caller,
            cancel.clone(),
        )?;
        let cancel = incoming.cancellation();
        Ok(AcceptedRequest::Multiplex(Box::new(Source {
            request: self,
            incoming,
            facts,
            cancel,
        })))
    }
    pub(crate) fn accept_standard_mkcp_carrier_peer(
        self,
        peer: zero_transport::finalmask::owned::source::Peer,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let facts = peer.facts.clone();
        let caller = context
            .accepted
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let profile = self
            .mkcp
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Session))?;
        let incoming =
            profile.accept_carrier_peer_scoped(peer, caller, context.cancel.child_token())?;
        let cancel = incoming.cancellation();
        Ok(AcceptedRequest::Multiplex(Box::new(Source {
            request: self,
            incoming,
            facts,
            cancel,
        })))
    }
}
struct Source {
    request: VlessInboundListenerRequest,
    incoming: zero_transport::mkcp::ScopedPeerStreams,
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
        std::num::NonZeroUsize::new(128).unwrap()
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
            let incoming = tokio::select! { biased;
                _=self.cancel.cancelled()=>return Ok(None),
                _=context.cancel.cancelled()=>return Ok(None),
                stream=zero_platform_tokio::contract::context::with_deadline(context.deadline,self.incoming.accept_stream())=>stream.map_err(datagram::failure)?,
            };
            Ok(incoming.map(|stream| {
                PendingInbound::new(Stream {
                    request: self.request.clone(),
                    stream,
                    facts: self.facts.clone(),
                })
            }))
        })
    }
}
struct Stream {
    request: VlessInboundListenerRequest,
    stream: zero_transport::mkcp::MkcpStream,
    facts: InboundConnectionFacts,
}
impl InboundHandshake for Stream {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let mut facts = self.facts;
        let (stream, alpn) = if let Some(acceptor) = self.request.transport.tls_acceptor.as_ref() {
            let stream = zero_transport::tls::accept_tls_handshake(acceptor, self.stream)
                .await
                .map_err(datagram::failure)?;
            facts.sni = stream.negotiated_server_name().map(str::to_owned);
            let alpn = stream
                .negotiated_alpn()
                .map(|b| String::from_utf8_lossy(b).into_owned());
            (TcpRelayStream::new(stream), alpn)
        } else {
            (TcpRelayStream::new(self.stream), None)
        };
        let stream = session::decrypt_stream(self.request.transport.decryption.as_ref(), stream)
            .await
            .map_err(datagram::failure)?;
        self.request
            .accept_standard_stream(record_client_stream(stream), facts, alpn, context)
            .await
    }
}

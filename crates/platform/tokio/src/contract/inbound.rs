use std::future::Future;
use zero_core::inbound::InboundControlSession;

use super::{
    context::{AcceptContext, HandshakeContext},
    BoxFuture, ChannelError,
};
mod accepted;
mod fallback;
pub use accepted::*;
pub use fallback::*;

pub enum AcceptedRequest {
    Stream(Box<dyn AcceptedStream>),
    Datagram(Box<dyn AcceptedDatagram>),
    Multiplex(Box<dyn RequestMultiplexer>),
    Packet(Box<dyn AcceptedPacket>),
    Control(Box<dyn InboundControlSession>),
    Fallback(Box<dyn AcceptedFallback>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyOutcome {
    /// A local channel exists; this does not claim deferred peer confirmation.
    LocalChannelReady,
    Blocked,
    UpstreamFailed,
}

pub trait InboundHandshake: Send + 'static {
    fn complete(
        self,
        context: HandshakeContext,
    ) -> impl Future<Output = Result<AcceptedRequest, ChannelError>> + Send;
}

trait DynHandshake: Send {
    fn complete(
        self: Box<Self>,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>>;
}
struct Handshake<T>(T);
impl<T: InboundHandshake> DynHandshake for Handshake<T> {
    fn complete(
        self: Box<Self>,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>> {
        Box::pin(self.0.complete(context))
    }
}

/// Owns the unhanded connection. Dropping it closes that connection.
pub struct PendingInbound(Box<dyn DynHandshake>);
impl PendingInbound {
    pub fn new(handshake: impl InboundHandshake) -> Self {
        Self(Box::new(Handshake(handshake)))
    }
    pub fn complete(
        self,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>> {
        self.0.complete(context)
    }
}

pub trait InboundSource: Send + 'static {
    type Handshake: InboundHandshake;
    /// Cancellation preserves undelivered requests. Accept never performs a slow handshake.
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> impl Future<Output = Result<Option<Self::Handshake>, ChannelError>> + Send;
}

pub trait DynInboundSource: Send {
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>>;
}
struct Source<T>(T);
impl<T: InboundSource> DynInboundSource for Source<T> {
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            self.0
                .next(context)
                .await
                .map(|pending| pending.map(PendingInbound::new))
        })
    }
}
pub fn erase_inbound(source: impl InboundSource) -> Box<dyn DynInboundSource> {
    Box::new(Source(source))
}

pub trait RequestMultiplexer: Send {
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>>;
}

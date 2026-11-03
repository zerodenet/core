use std::future::Future;
use zero_core::inbound::InboundControlSession;

use super::{
    context::{AcceptContext, HandshakeContext},
    BoxFuture, ChannelError,
};
mod accepted;
mod datagram;
pub use datagram::*;
mod activity;
mod admission;
pub use activity::StreamActivity;
pub(crate) use admission::TcpAdmission;
mod message;
pub use message::*;
mod fallback;
pub use accepted::*;
pub use fallback::*;
pub mod tcp;
pub mod udp;

pub enum AcceptedRequest {
    Stream(Box<dyn AcceptedStream>),
    Message(Box<dyn AcceptedMessage>),
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
    /// One erasure at the source boundary. Native adapters preserve their
    /// original accepted entry here; this performs no handshake or task start.
    fn into_pending(self) -> PendingInbound
    where
        Self: Sized,
    {
        PendingInbound::new(self)
    }
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
        Box::pin(complete_handshake(self.0, context))
    }
}

/// Owns the unhanded connection. Dropping it closes that connection.
pub struct PendingInbound {
    handshake: Box<dyn DynHandshake>,
    admission: Option<TcpAdmission>,
    peer: Option<udp::DatagramPeerAdmission>,
    carrier: Option<super::context::InboundConnectionFacts>,
    packet: Option<crate::controlled_datagram::listener::DatagramListenerIdentity>,
}
impl PendingInbound {
    pub fn new(handshake: impl InboundHandshake) -> Self {
        Self {
            handshake: Box::new(Handshake(handshake)),
            admission: None,
            peer: None,
            carrier: None,
            packet: None,
        }
    }
    /// Retains only the weak original listener identity, not native authority.
    pub fn from_packet_listener(
        handshake: impl InboundHandshake,
        original: crate::controlled_datagram::listener::DatagramListenerIdentity,
    ) -> Self {
        let mut pending = Self::new(handshake);
        pending.packet = Some(original);
        pending
    }
    pub fn packet_listener(
        &self,
    ) -> Option<&crate::controlled_datagram::listener::DatagramListenerIdentity> {
        self.packet.as_ref()
    }
    pub fn complete(
        self,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>> {
        self.handshake.complete(context)
    }
}
impl InboundHandshake for PendingInbound {
    fn into_pending(self) -> PendingInbound {
        self
    }
    fn complete(
        self,
        context: HandshakeContext,
    ) -> impl Future<Output = Result<AcceptedRequest, ChannelError>> + Send {
        PendingInbound::complete(self, context)
    }
}

// Keep cancellation/deadline wrapping inside the same one-time generic erasure;
// PendingInbound adds no second boxed future around an already erased one.
async fn complete_handshake<T: InboundHandshake>(
    handshake: T,
    context: HandshakeContext,
) -> Result<AcceptedRequest, ChannelError> {
    let cancel = context.cancel.clone();
    let deadline = context.deadline;
    if cancel.is_cancelled() {
        return Err(handshake_stopped(super::Reason::Cancelled));
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(handshake_stopped(super::Reason::Timeout));
    }
    // Central enforcement applies to every protocol, including one
    // whose concrete handshake never polls the context token itself.
    tokio::select! { biased;
        _ = cancel.cancelled() => Err(handshake_stopped(super::Reason::Cancelled)),
        result = super::context::with_deadline(Some(deadline), handshake.complete(context)) => {
            result.map_err(|source| handshake_stopped(super::Reason::Timeout).with_source(source))?
        }
    }
}

fn handshake_stopped(reason: super::Reason) -> ChannelError {
    ChannelError::new(super::Stage::Handshake, reason, super::Impact::Session)
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
                .map(|pending| pending.map(InboundHandshake::into_pending))
        })
    }
}
pub fn erase_inbound(source: impl InboundSource) -> Box<dyn DynInboundSource> {
    Box::new(Source(source))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestSourceEnd {
    /// The shared carrier ended; active logical channels must be cancelled.
    CarrierClosed,
    /// Ownership was handed to active requests (for example CONNECT/upgrade).
    /// Keep the source alive and drain those actual requests before closing.
    RequestsExhausted,
}

pub trait RequestMultiplexer: Send {
    /// Authentication of the actual shared connection. Per-request identity is
    /// still allocated only by each delivered handshake.
    fn auth(&self) -> Option<&zero_core::SessionAuth> {
        None
    }
    /// Requests cancellation of this original carrier; never a close receipt.
    fn close(&mut self, _reason: &str) {}
    /// Actual carrier retirement must remain observable while the host has
    /// reached the logical request limit and is not polling `next`.
    fn cancellation(&self) -> Option<super::CancellationToken> {
        None
    }
    /// Actual simultaneous logical requests supported by this carrier. A
    /// sequential message connection executes in its original accepted task.
    fn max_in_flight(&self) -> std::num::NonZeroUsize;

    fn end_disposition(&self) -> RequestSourceEnd {
        RequestSourceEnd::CarrierClosed
    }

    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>>;
}

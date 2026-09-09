//! Runtime-neutral contracts for protocols that multiplex native streams.
//!
//! Protocols own authentication, framing, stream classification and connection
//! state. Runtime owns routing, accounting and the tasks consuming these streams.
//! Frame-oriented MUX protocols continue to use `InboundMuxServer`; native
//! stream multiplexers need neither its frame reader nor its u16 stream IDs.

use core::future::Future;

use crate::{InboundClientResponse, InboundDatagramUdpRelay, Session, SessionAuth};
use zero_traits::AsyncSocket;

pub trait InboundStreamMultiplexer: Send + Sync + 'static {
    type Stream: AsyncSocket + 'static;
    type ResponseProtocol: InboundClientResponse<Self::Stream> + Send + Sync + 'static;
    type Error: Send;

    fn auth(&self) -> Option<&SessionAuth>;
    fn close(&self, reason: &str);
    fn response_protocol(&self) -> Self::ResponseProtocol;

    /// Return the next authenticated logical stream. Cancellation must not
    /// replay a request or discard a stream already returned to the runtime.
    fn accept_next_tcp_stream(
        &self,
    ) -> impl Future<Output = Result<Option<(Session, Self::Stream)>, Self::Error>> + Send;
}

/// Optional datagram role on the same authenticated connection. The source is
/// opaque to runtime: only the protocol's responder interprets its packets.
pub trait InboundDatagramMultiplexer: InboundStreamMultiplexer {
    type DatagramSource: Send + Sync + 'static;
    type UdpRelay: InboundDatagramUdpRelay<Self::DatagramSource> + Send + 'static;

    fn datagram_source(&self) -> Self::DatagramSource;
    fn udp_relay(&self) -> Self::UdpRelay;
}

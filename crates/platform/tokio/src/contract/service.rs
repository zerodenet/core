//! Root TCP network boundary. Ordinary protocol roles receive UpstreamTransport;
//! only the host and explicit root services receive this capability.
use super::{
    network::CarrierContext, stream::StreamIo, BoxFuture, ChannelError, DeliveryState, Impact,
    Reason, Stage,
};
use std::{io, net::SocketAddr};
use zero_core::component::NetworkGeneration;
use zero_traits::DialPolicy;

#[derive(Debug, Clone)]
pub struct TcpNetworkRequest {
    /// Already resolved endpoint. This boundary never performs hidden DNS.
    pub peer: SocketAddr,
    pub dial: DialPolicy,
    pub generation: NetworkGeneration,
}

pub struct NetworkConnection<S> {
    pub io: S,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub generation: NetworkGeneration,
    pub interface_bound: bool,
}

/// Keeps the native socket error owned until the caller chooses its error
/// boundary. Legacy dial diagnostics retain raw_os_error and the exact stage.
#[derive(Debug)]
pub struct NetworkFailure {
    pub source: io::Error,
    pub operation: &'static str,
    pub stage: Stage,
    pub reason: Reason,
    pub delivery: DeliveryState,
    pub local: Option<SocketAddr>,
    pub interface_bound: bool,
}
impl NetworkFailure {
    pub fn into_channel_error(self) -> ChannelError {
        let mut error =
            ChannelError::new(self.stage, self.reason, Impact::Session).with_source(self.source);
        error.delivery = self.delivery;
        error
    }
}
impl std::fmt::Display for NetworkFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.operation, self.source)
    }
}
impl std::error::Error for NetworkFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub trait NetworkService: Send + Sync {
    type Stream: StreamIo;
    fn generation(&self) -> NetworkGeneration;
    /// The returned socket is owned by the caller. Dropping an unfinished open
    /// cancels its inline I/O; there is no detached dial worker or retry owner.
    fn open_tcp(
        &self,
        request: TcpNetworkRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<NetworkConnection<Self::Stream>, NetworkFailure>>;
}

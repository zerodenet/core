#[cfg(any(feature = "tls", feature = "quic"))]
pub mod fingerprint;
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "h2")]
pub mod h2;
#[cfg(feature = "http_upgrade")]
pub mod http_upgrade;
#[cfg(feature = "tls")]
pub mod inbound_stack;
pub mod metered;
pub mod outbound_datagram;
#[cfg(any(
    feature = "tls",
    feature = "ws",
    feature = "grpc",
    feature = "h2",
    feature = "http_upgrade"
))]
pub mod outbound_stack;
pub mod profile;
#[cfg(feature = "quic")]
pub mod quic;
pub mod quic_initial;
#[cfg(feature = "split_http")]
pub mod split_http;
pub mod stream;
#[cfg(feature = "tls")]
pub mod tls;
pub mod udp_packet_path;
#[cfg(feature = "ws")]
pub mod ws;

pub use metered::MeteredStream;
pub use outbound_datagram::{
    OutboundDatagramSocketFactory, OutboundHostResolveFuture, OutboundHostResolver,
};
pub use stream::{
    ClientStream, PrefixedSocket, RecordingStream, RelayCarrier, ReplayStream, TcpRelayStream,
};
pub use zero_error::RuntimeError;
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StreamTraffic {
    pub read_bytes: u64,
    pub written_bytes: u64,
}

impl StreamTraffic {
    pub fn is_empty(self) -> bool {
        self.read_bytes == 0 && self.written_bytes == 0
    }
}

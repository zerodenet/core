#[cfg(feature = "managed-stream-runtime")]
mod mux;
#[cfg(feature = "managed-stream-runtime")]
mod recorded;
#[cfg(feature = "managed-stream-runtime")]
#[allow(dead_code)]
mod stream;

#[cfg(feature = "managed-stream-runtime")]
pub(crate) use mux::{
    dispatch_no_client_mux_route_request_with_defaults, NoClientMuxRouteDefaults,
};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use recorded::{
    dispatch_recorded_protocol_mux_stream_request_with_defaults,
    dispatch_recorded_protocol_mux_tcp_request_with_defaults, RecordedProtocolMuxRouteDefaults,
};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use stream::dispatch_no_client_stream_route;

//! XHTTP transport: normalized path/session/sequence metadata, distinct packet
//! and stream upload modes, and multiplexed HTTP/1.1/HTTP/2 inbound requests.
//! HTTP execution belongs here; protocol authentication and route tasks do not.
mod xmux;
pub use xmux::{XhttpCarrier, XhttpCarrierFactory, XhttpClientPool, XhttpDownload};
mod body;
mod chunked;
mod client;
mod http3;
pub use http3::{accept_xhttp_h3_connection, connect_xhttp_h3};
mod io;
mod mode;
pub use mode::XhttpMode;
mod registry;
mod request;
mod server;
mod sessions;
mod stream_one;
mod wire;

pub use client::{connect_split_http, connect_split_http_with_browser};
pub use io::XhttpStream;
pub use registry::SplitHttpRegistry;
pub use server::{accept_xhttp_connection, XhttpIncoming};
pub use stream_one::{
    accept_xhttp_stream_one, accept_xhttp_stream_one_http1, connect_xhttp_stream_one,
    connect_xhttp_stream_one_http1, AcceptedXhttpStreamOne, XhttpStreamOne,
};

/// Single-stream convenience API. Listener integrations use the stream source
/// above so multiple logical streams on one HTTP/2 connection are all routed.
pub async fn accept_xhttp_inbound<S, P>(
    stream: S,
    config: &P,
    registry: &SplitHttpRegistry,
) -> Result<Option<XhttpStream>, crate::RuntimeError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    P: zero_traits::SplitHttpTransportProfile + ?Sized,
{
    Ok(accept_xhttp_connection(stream, config, registry)
        .first()
        .await)
}

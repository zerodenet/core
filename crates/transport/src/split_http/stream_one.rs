use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::RuntimeError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::{AsyncSocket, SplitHttpTransportProfile};

use super::chunked::{ChunkedDecoder, DecodeStep};
use super::wire::{find_header_end, parse_status};

/// Single-connection bidirectional XHTTP stream (`stream-one` mode).
pub struct XhttpStreamOne<S> {
    inner: S,
    decoder: ChunkedDecoder,
    response_headers: Option<Vec<u8>>,
    write_finished: bool,
}

/// Server-side XHTTP stream-one transport selected from the client's wire
/// protocol. Xray uses HTTP/1.1 for cleartext clients while H2/H2C clients use
/// one HTTP/2 request stream; both expose the same bidirectional byte stream.
pub struct AcceptedXhttpStreamOne<S> {
    inner: super::io::XhttpStream,
    marker: core::marker::PhantomData<fn() -> S>,
}

mod handshake;
mod stream;
pub use handshake::{
    accept_xhttp_stream_one, accept_xhttp_stream_one_http1, connect_xhttp_stream_one,
    connect_xhttp_stream_one_carrier, connect_xhttp_stream_one_http1,
    connect_xhttp_stream_one_with_settings,
};

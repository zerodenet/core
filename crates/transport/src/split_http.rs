//! XHTTP transport (formerly SplitHTTP) 鈥?`split_http.rs`
//!
//! Splits a bidirectional stream across HTTP request(s) paired by an
//! `X-Session-Id` header. XTLS renamed SplitHTTP 鈫?XHTTP; the standalone
//! `quic` transport was removed in favour of XHTTP `stream-one` over H3.
//!
//! ## Modes (`SplitHttpTransportProfile::mode`)
//! - **stream-one** (default, also selected by `auto`): a single bidirectional
//!   request stream. Outbound uses H2/H2C; inbound sniffs and accepts either
//!   H2/H2C or HTTP/1.1 chunked, which is used by cleartext Xray clients. This
//!   is the only mode that works as a **relay-chain final hop**, where the
//!   relay prefix provides one carrier.
//! - **packet-up** / **stream-up**: the legacy two-connection model 鈥?a POST
//!   connection uploads, a separate GET connection downloads, paired by the
//!   server-side `SplitHttpRegistry`. Single-hop direct only; cannot be a
//!   relay final hop.
//!
//! ## Architecture
//! - Client `stream-one`: `connect_xhttp_stream_one` — one H2/H2C stream.
//! - Server `stream-one`: `accept_xhttp_stream_one` — H2/H2C or HTTP/1.1.
//! - Client two-connection: `connect_split_http` 鈥?POST + GET on two sockets.
//! - Server two-connection: `accept_split_http` pairs POST/GET by session ID.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::RuntimeError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::{AsyncSocket, SplitHttpTransportProfile};

mod chunked;
mod legacy;
mod paired;
mod registry;
mod stream_one;
mod wire;

pub use legacy::{accept_split_http, connect_split_http};
pub use paired::{SplitHttpPairedStream, SplitHttpStream};
pub use registry::SplitHttpRegistry;
pub use stream_one::{
    accept_xhttp_stream_one, accept_xhttp_stream_one_http1, connect_xhttp_stream_one,
    connect_xhttp_stream_one_http1, AcceptedXhttpStreamOne, XhttpMode, XhttpStreamOne,
};
/// Accepted inbound XHTTP/SplitHTTP stream.
pub enum AcceptedSplitHttpInboundStream<S> {
    StreamOne(AcceptedXhttpStreamOne<S>),
    Paired(SplitHttpStream<S>),
}

impl<S> AsyncRead for AcceptedSplitHttpInboundStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::StreamOne(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Paired(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S> AsyncWrite for AcceptedSplitHttpInboundStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::StreamOne(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Paired(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::StreamOne(stream) => Pin::new(stream).poll_flush(cx),
            Self::Paired(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::StreamOne(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Paired(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl<S> AsyncSocket for AcceptedSplitHttpInboundStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl<S> ClientStream for AcceptedSplitHttpInboundStream<S> where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync
{
}

/// Accept either XHTTP stream-one or paired SplitHTTP inbound transport.
pub async fn accept_xhttp_inbound<S, TProfile>(
    stream: S,
    config: &TProfile,
    registry: &SplitHttpRegistry,
) -> Result<Option<AcceptedSplitHttpInboundStream<S>>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    if XhttpMode::parse(config.mode()).is_single_connection() {
        return accept_xhttp_stream_one(stream, config)
            .await
            .map(AcceptedSplitHttpInboundStream::StreamOne)
            .map(Some);
    }

    accept_split_http(stream, config, registry)
        .await
        .map(|stream| stream.map(AcceptedSplitHttpInboundStream::Paired))
}
